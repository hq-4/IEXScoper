from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.iextools_backfill_core import existing_tops_days, tops_output_paths

DEFAULT_PARQUET_ROOT = Path("/media/tn/pq")
DEFAULT_OUTPUT_ROOT = Path("/media/tn/pq/derived/daily-trade-bars")
DEFAULT_SYMBOL_ERAS_PATH = Path("reports/symbol-stability/symbol_eras.parquet")
DEFAULT_START_DAY = "20161212"
DEFAULT_END_DAY = "20260622"
TRADE_TYPE = "TradeReport"
PRICE_SCALE = 10_000

OUTPUT_SCHEMA = {
    "day": pl.String,
    "symbol": pl.String,
    "symbol_era_id": pl.String,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "trade_count": pl.Int64,
    "notional": pl.Float64,
    "vwap": pl.Float64,
    "first_timestamp": pl.Int64,
    "last_timestamp": pl.Int64,
}


@dataclass(frozen=True)
class TradeBarConfig:
    parquet_root: Path
    output_root: Path
    symbol_eras_path: Path
    start_day: str
    end_day: str
    compression: str
    limit_days: int | None
    replace: bool


@dataclass(frozen=True)
class DayResult:
    day: str
    status: str
    output_path: str | None = None
    input_path: str | None = None
    rows: int = 0
    trade_rows: int = 0
    unmatched_trade_rows: int = 0
    size_bytes: int = 0
    error: str | None = None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet-root", default=str(DEFAULT_PARQUET_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--symbol-eras-path", default=str(DEFAULT_SYMBOL_ERAS_PATH))
    parser.add_argument("--start-day", default=DEFAULT_START_DAY)
    parser.add_argument("--end-day", default=DEFAULT_END_DAY)
    parser.add_argument("--compression", default="zstd", choices=["zstd", "snappy"])
    parser.add_argument("--limit-days", type=int)
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()

    config = TradeBarConfig(
        parquet_root=Path(args.parquet_root),
        output_root=Path(args.output_root),
        symbol_eras_path=Path(args.symbol_eras_path),
        start_day=args.start_day,
        end_day=args.end_day,
        compression=args.compression,
        limit_days=args.limit_days,
        replace=args.replace,
    )
    setup_logging(str(config.output_root / "daily_trade_bars_audit.jsonl"))
    result = build_daily_trade_bars(config)
    get_logger(__name__).info(
        "daily trade bars complete",
        extra={"event": "daily_trade_bars_complete", "detail": result["summary"]},
    )
    return 0


def build_daily_trade_bars(config: TradeBarConfig) -> dict[str, Any]:
    config.output_root.mkdir(parents=True, exist_ok=True)
    era_frame = load_symbol_eras(config.symbol_eras_path)
    days = discover_days(config)
    results = [process_day(config, day, era_frame) for day in days]
    summary = build_summary(config, days, results)
    write_summary(config.output_root, summary, results)
    return {"summary": summary, "results": [result.__dict__ for result in results]}


def discover_days(config: TradeBarConfig) -> list[str]:
    days = [
        day
        for day in sorted(existing_tops_days(config.parquet_root))
        if config.start_day <= day <= config.end_day
    ]
    if config.limit_days is not None:
        return days[: config.limit_days]
    return days


def load_symbol_eras(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"symbol eras file does not exist: {path}")
    required = ["symbol", "symbol_era_id", "first_day", "last_day"]
    frame = pl.read_parquet(path)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    return frame.select(required)


def process_day(config: TradeBarConfig, day: str, era_frame: pl.DataFrame) -> DayResult:
    output_path = trade_bar_output_path(config.output_root, day)
    if output_path.exists() and not config.replace:
        return DayResult(day=day, status="skipped_existing", output_path=str(output_path))
    main_path, _ = tops_output_paths(config.parquet_root, day)
    logger = get_logger(__name__)
    try:
        bars, trade_rows, unmatched = build_day_bars(main_path, day, era_frame)
        write_day_bars(output_path, bars, config.compression)
    except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
        result = DayResult(
            day=day,
            status="failed",
            input_path=str(main_path),
            output_path=str(output_path),
            error=f"{type(exc).__name__}: {exc}",
        )
        logger.exception("daily trade bars day failed", extra=day_extra(day, result))
        return result
    result = DayResult(
        day=day,
        status="processed",
        input_path=str(main_path),
        output_path=str(output_path),
        rows=bars.height,
        trade_rows=trade_rows,
        unmatched_trade_rows=unmatched,
        size_bytes=output_path.stat().st_size,
    )
    logger.info("daily trade bars day processed", extra=day_extra(day, result))
    return result


def build_day_bars(path: Path, day: str, era_frame: pl.DataFrame) -> tuple[pl.DataFrame, int, int]:
    validate_trade_columns(path)
    day_eras = era_frame.filter((pl.col("first_day") <= day) & (pl.col("last_day") >= day))
    if day_eras.is_empty():
        return empty_bars(), 0, 0
    trades = build_trade_frame(path, day)
    trade_rows = trades.select(pl.len()).collect().item()
    joined = trades.join(day_eras.lazy(), on="symbol", how="left")
    unmatched = joined.filter(pl.col("symbol_era_id").is_null()).select(pl.len()).collect().item()
    bars = aggregate_trades(joined.filter(pl.col("symbol_era_id").is_not_null()), day)
    return bars, int(trade_rows), int(unmatched)


def validate_trade_columns(path: Path) -> None:
    columns = pl.scan_parquet(str(path)).collect_schema().names()
    required = ["type", "timestamp", "symbol", "size"]
    missing = [column for column in required if column not in columns]
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    if "price" not in columns and "price_int" not in columns:
        raise ValueError(f"{path} missing price and price_int columns")


def build_trade_frame(path: Path, day: str) -> pl.LazyFrame:
    price = trade_price_expr(pl.scan_parquet(str(path)).collect_schema().names())
    return (
        pl.scan_parquet(str(path))
        .select("type", "timestamp", "symbol", "size", price.alias("trade_price"))
        .filter(pl.col("type") == TRADE_TYPE)
        .filter(pl.col("symbol").is_not_null())
        .filter(pl.col("timestamp").is_not_null())
        .filter(pl.col("size").is_not_null() & (pl.col("size") > 0))
        .filter(pl.col("trade_price").is_not_null() & (pl.col("trade_price") > 0))
        .with_columns(pl.lit(day).alias("day"))
    )


def trade_price_expr(columns: list[str]) -> pl.Expr:
    price_from_int = (pl.col("price_int") / PRICE_SCALE).cast(pl.Float64)
    if "price" in columns and "price_int" in columns:
        return pl.coalesce(pl.col("price").cast(pl.Float64), price_from_int)
    if "price" in columns:
        return pl.col("price").cast(pl.Float64)
    return price_from_int


def aggregate_trades(trades: pl.LazyFrame, day: str) -> pl.DataFrame:
    bars = (
        trades.group_by("symbol", "symbol_era_id")
        .agg(
            pl.col("trade_price").sort_by("timestamp").first().alias("open"),
            pl.col("trade_price").max().alias("high"),
            pl.col("trade_price").min().alias("low"),
            pl.col("trade_price").sort_by("timestamp").last().alias("close"),
            pl.col("size").sum().alias("volume"),
            pl.len().alias("trade_count"),
            (pl.col("trade_price") * pl.col("size")).sum().alias("notional"),
            pl.col("timestamp").min().alias("first_timestamp"),
            pl.col("timestamp").max().alias("last_timestamp"),
        )
        .with_columns((pl.col("notional") / pl.col("volume")).alias("vwap"))
        .with_columns(pl.lit(day).alias("day"))
        .select(list(OUTPUT_SCHEMA))
        .sort("symbol_era_id")
        .collect()
    )
    return bars.cast(OUTPUT_SCHEMA)


def empty_bars() -> pl.DataFrame:
    return pl.DataFrame(schema=OUTPUT_SCHEMA)


def write_day_bars(path: Path, frame: pl.DataFrame, compression: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    frame.write_parquet(tmp_path, compression=compression)
    tmp_path.replace(path)


def trade_bar_output_path(output_root: Path, day: str) -> Path:
    return output_root / day[:4] / day[4:6] / f"{day}_confirmed_trade_bars.parquet"


def build_summary(
    config: TradeBarConfig, days: list[str], results: list[DayResult]
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "daily confirmed TradeReport OHLCV by symbol_era_id",
        "parquet_root": str(config.parquet_root),
        "output_root": str(config.output_root),
        "symbol_eras_path": str(config.symbol_eras_path),
        "start_day": config.start_day,
        "end_day": config.end_day,
        "candidate_day_count": len(days),
        "processed_day_count": count_status(results, "processed"),
        "skipped_existing_day_count": count_status(results, "skipped_existing"),
        "failed_day_count": count_status(results, "failed"),
        "bar_row_count": sum(result.rows for result in results),
        "trade_row_count": sum(result.trade_rows for result in results),
        "unmatched_trade_row_count": sum(result.unmatched_trade_rows for result in results),
    }


def write_summary(output_root: Path, summary: dict[str, Any], results: list[DayResult]) -> None:
    (output_root / "daily_trade_bars_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_root / "daily_trade_bars_results.jsonl").write_text(
        "\n".join(json.dumps(result.__dict__, sort_keys=True) for result in results)
        + ("\n" if results else ""),
        encoding="utf-8",
    )


def count_status(results: list[DayResult], status: str) -> int:
    return sum(1 for result in results if result.status == status)


def day_extra(day: str, result: DayResult) -> dict[str, Any]:
    return {"event": f"daily_trade_bars_day_{result.status}", "day": day, "detail": result.__dict__}


if __name__ == "__main__":
    raise SystemExit(main())
