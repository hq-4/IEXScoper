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

DEFAULT_SYMBOL_ERAS_PATH = Path("reports/symbol-stability/symbol_eras.parquet")
DEFAULT_DAILY_BARS_ROOT = Path("/media/tn/pq/derived/daily-trade-bars")
DEFAULT_OUTPUT_ROOT = Path("reports/stable-long-window-universe")
DEFAULT_MIN_TRADE_DAY_COVERAGE = 0.95
DEFAULT_CORE_MEDIAN_DAILY_NOTIONAL = 1_000_000.0
DEFAULT_ACTIVE_MEDIAN_DAILY_NOTIONAL = 100_000.0


@dataclass(frozen=True)
class StableUniverseConfig:
    symbol_eras_path: Path
    daily_bars_root: Path
    output_root: Path
    min_trade_day_coverage: float
    core_median_daily_notional: float
    active_median_daily_notional: float


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol-eras-path", default=str(DEFAULT_SYMBOL_ERAS_PATH))
    parser.add_argument("--daily-bars-root", default=str(DEFAULT_DAILY_BARS_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--min-trade-day-coverage", type=float, default=DEFAULT_MIN_TRADE_DAY_COVERAGE)
    parser.add_argument(
        "--core-median-daily-notional",
        type=float,
        default=DEFAULT_CORE_MEDIAN_DAILY_NOTIONAL,
    )
    parser.add_argument(
        "--active-median-daily-notional",
        type=float,
        default=DEFAULT_ACTIVE_MEDIAN_DAILY_NOTIONAL,
    )
    args = parser.parse_args()
    config = StableUniverseConfig(
        symbol_eras_path=Path(args.symbol_eras_path),
        daily_bars_root=Path(args.daily_bars_root),
        output_root=Path(args.output_root),
        min_trade_day_coverage=args.min_trade_day_coverage,
        core_median_daily_notional=args.core_median_daily_notional,
        active_median_daily_notional=args.active_median_daily_notional,
    )
    setup_logging(str(config.output_root / "stable_long_window_universe.jsonl"))
    result = build_stable_long_window_universe(config)
    get_logger(__name__).info(
        "stable long-window universe complete",
        extra={"event": "stable_long_window_universe_complete", "detail": result["summary"]},
    )
    return 0


def build_stable_long_window_universe(config: StableUniverseConfig) -> dict[str, Any]:
    config.output_root.mkdir(parents=True, exist_ok=True)
    era_frame = load_long_window_eras(config.symbol_eras_path)
    bar_paths = daily_bar_paths(config.daily_bars_root)
    stats = aggregate_daily_bars(bar_paths, era_frame)
    rows = finalize_rows(era_frame, stats, config)
    summary = build_summary(config, rows, bar_paths)
    write_outputs(config.output_root, rows, summary)
    return {"summary": summary, "rows": rows.to_dicts()}


def load_long_window_eras(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"symbol eras file does not exist: {path}")
    required = [
        "symbol",
        "symbol_era_id",
        "first_day",
        "last_day",
        "expected_days_in_era",
        "coverage_ratio",
        "recommended_use",
        "identity_status",
    ]
    frame = pl.read_parquet(path)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    return frame.filter(pl.col("recommended_use") == "long_window_candidate").select(required)


def daily_bar_paths(root: Path) -> list[str]:
    paths = sorted(str(path) for path in root.glob("*/*/*_confirmed_trade_bars.parquet"))
    if not paths:
        raise FileNotFoundError(f"no daily trade bar files found under {root}")
    return paths


def aggregate_daily_bars(paths: list[str], eras: pl.DataFrame) -> pl.DataFrame:
    bars = pl.scan_parquet(paths)
    joined = bars.join(eras.lazy().select("symbol_era_id"), on="symbol_era_id", how="inner")
    return (
        joined.group_by("symbol_era_id")
        .agg(
            pl.col("day").n_unique().alias("traded_days"),
            pl.col("day").min().alias("first_trade_day"),
            pl.col("day").max().alias("last_trade_day"),
            pl.col("volume").sum().alias("total_volume"),
            pl.col("trade_count").sum().alias("total_trade_count"),
            pl.col("notional").sum().alias("total_notional"),
            pl.col("notional").mean().alias("avg_daily_notional"),
            pl.col("notional").median().alias("median_daily_notional"),
            pl.col("notional").quantile(0.10).alias("p10_daily_notional"),
            pl.col("volume").mean().alias("avg_daily_volume"),
            pl.col("volume").median().alias("median_daily_volume"),
            pl.col("trade_count").mean().alias("avg_daily_trade_count"),
            pl.col("trade_count").median().alias("median_daily_trade_count"),
            pl.col("close").sort_by("day").first().alias("first_close"),
            pl.col("close").sort_by("day").last().alias("last_close"),
            pl.col("close").min().alias("min_close"),
            pl.col("close").max().alias("max_close"),
        )
        .collect()
    )


def finalize_rows(
    eras: pl.DataFrame, stats: pl.DataFrame, config: StableUniverseConfig
) -> pl.DataFrame:
    numeric_defaults = {
        "traded_days": 0,
        "total_volume": 0,
        "total_trade_count": 0,
        "total_notional": 0.0,
    }
    rows = eras.join(stats, on="symbol_era_id", how="left").with_columns(
        [pl.col(column).fill_null(value) for column, value in numeric_defaults.items()]
    )
    return (
        rows.with_columns(
            (pl.col("traded_days") / pl.col("expected_days_in_era")).alias(
                "trade_day_coverage_ratio"
            )
        )
        .with_columns(liquidity_tier_expr(config).alias("liquidity_tier"))
        .select(STABLE_UNIVERSE_COLUMNS)
        .sort(["median_daily_notional", "symbol"], descending=[True, False])
    )


def liquidity_tier_expr(config: StableUniverseConfig) -> pl.Expr:
    coverage_ok = pl.col("trade_day_coverage_ratio") >= config.min_trade_day_coverage
    median_notional = pl.col("median_daily_notional").fill_null(0.0)
    return (
        pl.when(pl.col("traded_days") == 0)
        .then(pl.lit("no_confirmed_trades"))
        .when(coverage_ok & (median_notional >= config.core_median_daily_notional))
        .then(pl.lit("core_liquid"))
        .when(coverage_ok & (median_notional >= config.active_median_daily_notional))
        .then(pl.lit("active"))
        .otherwise(pl.lit("thin"))
    )


def build_summary(
    config: StableUniverseConfig, rows: pl.DataFrame, bar_paths: list[str]
) -> dict[str, Any]:
    tier_counts = {
        row["liquidity_tier"]: row["len"]
        for row in rows.group_by("liquidity_tier").len().to_dicts()
    }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "stable long-window ticker-era universe from confirmed trades",
        "symbol_eras_path": str(config.symbol_eras_path),
        "daily_bars_root": str(config.daily_bars_root),
        "daily_bar_file_count": len(bar_paths),
        "stable_era_count": rows.height,
        "tier_counts": tier_counts,
        "min_trade_day_coverage": config.min_trade_day_coverage,
        "core_median_daily_notional": config.core_median_daily_notional,
        "active_median_daily_notional": config.active_median_daily_notional,
    }


def write_outputs(output_root: Path, rows: pl.DataFrame, summary: dict[str, Any]) -> None:
    rows.write_parquet(output_root / "stable_long_window_universe.parquet", compression="zstd")
    rows.write_csv(output_root / "stable_long_window_universe.csv")
    (output_root / "stable_long_window_universe.jsonl").write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows.to_dicts()) + "\n",
        encoding="utf-8",
    )
    (output_root / "stable_long_window_universe_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(output_root / "stable_long_window_universe_report.md", rows, summary)


def write_markdown(path: Path, rows: pl.DataFrame, summary: dict[str, Any]) -> None:
    top = rows.sort("median_daily_notional", descending=True).head(25).to_dicts()
    lines = [
        "# Stable Long-Window Ticker-Era Universe",
        "",
        f"- Stable era count: `{summary['stable_era_count']}`",
        f"- Daily bar files scanned: `{summary['daily_bar_file_count']}`",
        f"- Minimum trade-day coverage threshold: `{summary['min_trade_day_coverage']}`",
        f"- Core median daily notional threshold: `${summary['core_median_daily_notional']:,.0f}`",
        f"- Active median daily notional threshold: `${summary['active_median_daily_notional']:,.0f}`",
        "",
        "## Liquidity Tiers",
        "",
    ]
    for tier, count in sorted(summary["tier_counts"].items()):
        lines.append(f"- `{tier}`: `{count}`")
    lines.extend(
        [
            "",
            "## Top 25 By Median Daily Notional",
            "",
            "| Symbol | Era | Tier | Trade coverage | Median daily notional | Median trades/day |",
            "|---|---:|---|---:|---:|---:|",
        ]
    )
    for row in top:
        lines.append(
            "| {symbol} | {symbol_era_id} | {liquidity_tier} | {coverage:.4f} | "
            "${notional:,.0f} | {trades:,.0f} |".format(
                symbol=row["symbol"],
                symbol_era_id=row["symbol_era_id"],
                liquidity_tier=row["liquidity_tier"],
                coverage=row["trade_day_coverage_ratio"],
                notional=row["median_daily_notional"] or 0,
                trades=row["median_daily_trade_count"] or 0,
            )
        )
    lines.extend(
        [
            "",
            "## Caveat",
            "",
            "This universe is ticker-era based, not issuer-ID based. Use `symbol_era_id` for "
            "point-in-time analysis and join an external security master before making "
            "issuer-level corporate-action claims.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


STABLE_UNIVERSE_COLUMNS = [
    "symbol",
    "symbol_era_id",
    "liquidity_tier",
    "first_day",
    "last_day",
    "expected_days_in_era",
    "coverage_ratio",
    "traded_days",
    "trade_day_coverage_ratio",
    "first_trade_day",
    "last_trade_day",
    "total_volume",
    "total_trade_count",
    "total_notional",
    "avg_daily_notional",
    "median_daily_notional",
    "p10_daily_notional",
    "avg_daily_volume",
    "median_daily_volume",
    "avg_daily_trade_count",
    "median_daily_trade_count",
    "first_close",
    "last_close",
    "min_close",
    "max_close",
    "identity_status",
]


if __name__ == "__main__":
    raise SystemExit(main())
