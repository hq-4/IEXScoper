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

DEFAULT_UNIVERSE_PATH = Path("reports/stable-long-window-universe/stable_long_window_universe.parquet")
DEFAULT_DAILY_BARS_ROOT = Path("/media/tn/pq/derived/daily-trade-bars")
DEFAULT_OUTPUT_ROOT = Path("reports/stable-long-window-quality")
DEFAULT_EXTREME_RETURN = 0.50
DEFAULT_NEAR_ZERO_PRICE = 0.01
DEFAULT_OUTLIER_MULTIPLE = 20.0
DEFAULT_MIN_OUTLIER_NOTIONAL = 1_000_000.0

@dataclass(frozen=True)
class QualityConfig:
    universe_path: Path
    daily_bars_root: Path
    output_root: Path
    extreme_return_threshold: float
    near_zero_price_threshold: float
    outlier_multiple: float
    min_outlier_notional: float

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe-path", default=str(DEFAULT_UNIVERSE_PATH))
    parser.add_argument("--daily-bars-root", default=str(DEFAULT_DAILY_BARS_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--extreme-return-threshold", type=float, default=DEFAULT_EXTREME_RETURN)
    parser.add_argument("--near-zero-price-threshold", type=float, default=DEFAULT_NEAR_ZERO_PRICE)
    parser.add_argument("--outlier-multiple", type=float, default=DEFAULT_OUTLIER_MULTIPLE)
    parser.add_argument("--min-outlier-notional", type=float, default=DEFAULT_MIN_OUTLIER_NOTIONAL)
    args = parser.parse_args()
    config = QualityConfig(
        universe_path=Path(args.universe_path),
        daily_bars_root=Path(args.daily_bars_root),
        output_root=Path(args.output_root),
        extreme_return_threshold=args.extreme_return_threshold,
        near_zero_price_threshold=args.near_zero_price_threshold,
        outlier_multiple=args.outlier_multiple,
        min_outlier_notional=args.min_outlier_notional,
    )
    setup_logging(str(config.output_root / "stable_universe_quality.jsonl"))
    result = build_quality_report(config)
    get_logger(__name__).info(
        "stable universe quality complete",
        extra={"event": "stable_universe_quality_complete", "detail": result["summary"]},
    )
    return 0

def build_quality_report(config: QualityConfig) -> dict[str, Any]:
    config.output_root.mkdir(parents=True, exist_ok=True)
    universe = load_universe(config.universe_path)
    daily = build_daily_quality_frame(config, universe)
    by_symbol = aggregate_by_symbol(daily, universe)
    events = build_events(daily, config)
    summary = build_summary(config, universe, by_symbol, events)
    write_outputs(config.output_root, by_symbol, events, summary)
    return {"summary": summary}

def load_universe(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"stable universe file does not exist: {path}")
    required = ["symbol", "symbol_era_id", "liquidity_tier", "expected_days_in_era"]
    frame = pl.read_parquet(path)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    return frame.select(required)

def daily_bar_paths(root: Path) -> list[str]:
    paths = sorted(str(path) for path in root.glob("*/*/*_confirmed_trade_bars.parquet"))
    if not paths:
        raise FileNotFoundError(f"no daily trade bar files found under {root}")
    return paths

def build_daily_quality_frame(config: QualityConfig, universe: pl.DataFrame) -> pl.DataFrame:
    bars = pl.scan_parquet(daily_bar_paths(config.daily_bars_root))
    joined = bars.join(universe.lazy(), on=["symbol", "symbol_era_id"], how="inner")
    sorted_rows = joined.sort(["symbol_era_id", "day"]).with_columns(
        pl.col("close").shift(1).over("symbol_era_id").alias("prev_close"),
        pl.col("notional").median().over("symbol_era_id").alias("median_notional_by_symbol"),
        pl.col("volume").median().over("symbol_era_id").alias("median_volume_by_symbol"),
    )
    return (
        sorted_rows.with_columns(
            ((pl.col("close") / pl.col("prev_close")) - 1.0).alias("raw_close_return"),
            invalid_ohlc_expr().alias("invalid_ohlc"),
            price_at_or_below_expr(0.0).alias("nonpositive_price"),
            price_at_or_below_expr(config.near_zero_price_threshold).alias("near_zero_price"),
            notional_outlier_expr(config).alias("notional_outlier"),
            volume_outlier_expr(config).alias("volume_outlier"),
        )
        .with_columns(
            (pl.col("raw_close_return").abs() >= config.extreme_return_threshold).fill_null(False)
            .alias("extreme_return")
        )
        .collect()
    )

def invalid_ohlc_expr() -> pl.Expr:
    return (
        (pl.col("high") < pl.col("low"))
        | (pl.col("open") < pl.col("low"))
        | (pl.col("open") > pl.col("high"))
        | (pl.col("close") < pl.col("low"))
        | (pl.col("close") > pl.col("high"))
    ).fill_null(True)

def price_at_or_below_expr(threshold: float) -> pl.Expr:
    return (
        (pl.col("open") <= threshold)
        | (pl.col("high") <= threshold)
        | (pl.col("low") <= threshold)
        | (pl.col("close") <= threshold)
    ).fill_null(True)

def notional_outlier_expr(config: QualityConfig) -> pl.Expr:
    threshold = pl.max_horizontal(
        pl.col("median_notional_by_symbol") * config.outlier_multiple,
        pl.lit(config.min_outlier_notional),
    )
    return (pl.col("notional") > threshold).fill_null(False)

def volume_outlier_expr(config: QualityConfig) -> pl.Expr:
    threshold = pl.col("median_volume_by_symbol") * config.outlier_multiple
    return (pl.col("volume") > threshold).fill_null(False)

def aggregate_by_symbol(daily: pl.DataFrame, universe: pl.DataFrame) -> pl.DataFrame:
    aggregates = daily.group_by("symbol", "symbol_era_id", "liquidity_tier").agg(
        pl.col("day").n_unique().alias("observed_trade_days"),
        pl.col("invalid_ohlc").sum().alias("invalid_ohlc_days"),
        pl.col("nonpositive_price").sum().alias("nonpositive_price_days"),
        pl.col("near_zero_price").sum().alias("near_zero_price_days"),
        pl.col("extreme_return").sum().alias("extreme_return_days"),
        pl.col("notional_outlier").sum().alias("notional_outlier_days"),
        pl.col("volume_outlier").sum().alias("volume_outlier_days"),
        pl.col("raw_close_return").min().alias("min_raw_close_return"),
        pl.col("raw_close_return").max().alias("max_raw_close_return"),
        pl.col("raw_close_return").abs().max().alias("max_abs_raw_close_return"),
        pl.col("notional").median().alias("median_daily_notional"),
        pl.col("volume").median().alias("median_daily_volume"),
    )
    return (
        universe.join(aggregates, on=["symbol", "symbol_era_id", "liquidity_tier"], how="left")
        .with_columns(pl.col("observed_trade_days").fill_null(0))
        .with_columns(
            (pl.col("expected_days_in_era") - pl.col("observed_trade_days")).alias(
                "missing_trade_days"
            )
        )
        .with_columns(quality_score_expr().alias("quality_issue_days"))
        .sort(["quality_issue_days", "max_abs_raw_close_return", "symbol"], descending=[True, True, False])
    )

def quality_score_expr() -> pl.Expr:
    issue_columns = [
        "invalid_ohlc_days",
        "nonpositive_price_days",
        "near_zero_price_days",
        "extreme_return_days",
        "notional_outlier_days",
        "volume_outlier_days",
        "missing_trade_days",
    ]
    return sum(pl.col(column).fill_null(0) for column in issue_columns)

def build_events(daily: pl.DataFrame, config: QualityConfig) -> pl.DataFrame:
    event_specs = [
        ("invalid_ohlc", pl.col("invalid_ohlc")),
        ("nonpositive_price", pl.col("nonpositive_price")),
        ("near_zero_price", pl.col("near_zero_price")),
        ("extreme_return", pl.col("extreme_return")),
        ("notional_outlier", pl.col("notional_outlier")),
        ("volume_outlier", pl.col("volume_outlier")),
    ]
    frames = [event_frame(daily, name, predicate, config) for name, predicate in event_specs]
    return pl.concat(frames, how="vertical").sort(["day", "symbol_era_id", "event_type"])

def event_frame(
    daily: pl.DataFrame, event_type: str, predicate: pl.Expr, config: QualityConfig
) -> pl.DataFrame:
    return (
        daily.filter(predicate)
        .with_columns(
            pl.lit(event_type).alias("event_type"),
            event_detail_expr(event_type, config).alias("event_detail"),
        )
        .select(EVENT_COLUMNS)
    )

def event_detail_expr(event_type: str, config: QualityConfig) -> pl.Expr:
    if event_type == "extreme_return":
        return pl.format("raw_close_return={}", pl.col("raw_close_return"))
    if event_type == "notional_outlier":
        return pl.format("notional={} median={}", pl.col("notional"), pl.col("median_notional_by_symbol"))
    if event_type == "volume_outlier":
        return pl.format("volume={} median={}", pl.col("volume"), pl.col("median_volume_by_symbol"))
    if event_type == "near_zero_price":
        return pl.lit(f"price <= {config.near_zero_price_threshold}")
    return pl.lit(event_type)

def build_summary(
    config: QualityConfig, universe: pl.DataFrame, by_symbol: pl.DataFrame, events: pl.DataFrame
) -> dict[str, Any]:
    event_counts = {
        row["event_type"]: row["len"] for row in events.group_by("event_type").len().to_dicts()
    }
    tier_issue_counts = {
        row["liquidity_tier"]: row["len"]
        for row in by_symbol.filter(pl.col("quality_issue_days") > 0)
        .group_by("liquidity_tier")
        .len()
        .to_dicts()
    }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "stable long-window universe daily-bar quality report",
        "universe_path": str(config.universe_path),
        "daily_bars_root": str(config.daily_bars_root),
        "stable_era_count": universe.height,
        "symbols_with_quality_issues": by_symbol.filter(pl.col("quality_issue_days") > 0).height,
        "symbols_with_daily_events": events.select("symbol_era_id").n_unique(),
        "event_count": events.height,
        "event_counts": event_counts,
        "tier_issue_counts": tier_issue_counts,
        "extreme_return_threshold": config.extreme_return_threshold,
        "near_zero_price_threshold": config.near_zero_price_threshold,
        "outlier_multiple": config.outlier_multiple,
        "min_outlier_notional": config.min_outlier_notional,
    }

def write_outputs(
    output_root: Path, by_symbol: pl.DataFrame, events: pl.DataFrame, summary: dict[str, Any]
) -> None:
    by_symbol.write_parquet(output_root / "quality_by_symbol.parquet", compression="zstd")
    events.write_parquet(output_root / "quality_events.parquet", compression="zstd")
    by_symbol.write_csv(output_root / "quality_by_symbol.csv")
    events.write_csv(output_root / "quality_events.csv")
    (output_root / "quality_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    write_markdown(output_root / "quality_report.md", by_symbol, events, summary)

def write_markdown(
    path: Path, by_symbol: pl.DataFrame, events: pl.DataFrame, summary: dict[str, Any]
) -> None:
    lines = [
        "# Stable Long-Window Quality Report",
        "",
        f"- Stable eras checked: `{summary['stable_era_count']}`",
        f"- Symbols with quality issues: `{summary['symbols_with_quality_issues']}`",
        f"- Symbols with daily event rows: `{summary['symbols_with_daily_events']}`",
        f"- Total event rows: `{summary['event_count']}`",
        "",
        "## Event Counts",
        "",
    ]
    lines += [f"- `{event_type}`: `{count}`" for event_type, count in sorted(summary["event_counts"].items())]
    lines += ["", "## Top Symbols By Issue Days", "", SYMBOL_TABLE_HEADER, SYMBOL_TABLE_RULE]
    lines += [symbol_markdown_row(row) for row in by_symbol.head(25).to_dicts()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def symbol_markdown_row(row: dict[str, Any]) -> str:
    return (
        f"| {row['symbol']} | {row['symbol_era_id']} | {row['liquidity_tier']} | "
        f"{row['quality_issue_days']} | {row['max_abs_raw_close_return'] or 0:.4f} | "
        f"{row['missing_trade_days']} |"
    )

SYMBOL_TABLE_HEADER = "| Symbol | Era | Tier | Issue days | Max abs return | Missing trade days |"
SYMBOL_TABLE_RULE = "|---|---:|---|---:|---:|---:|"
EVENT_COLUMNS = [
    "day", "symbol", "symbol_era_id", "liquidity_tier", "event_type", "event_detail",
    "open", "high", "low", "close", "prev_close", "raw_close_return", "volume",
    "trade_count", "notional",
]


if __name__ == "__main__":
    raise SystemExit(main())
