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

DEFAULT_UNIVERSE_PATH = Path(
    "reports/iex-entity-enrichment/stable_long_window_universe_iex_enriched.parquet"
)
DEFAULT_DAILY_BARS_ROOT = Path("/media/tn/pq/derived/daily-trade-bars")
DEFAULT_QUALITY_EVENTS_PATH = Path("reports/stable-long-window-quality/quality_events.parquet")
DEFAULT_OUTPUT_PATH = Path("/media/tn/pq/derived/stable-daily-panel/stable_daily_panel.parquet")
DEFAULT_REPORT_ROOT = Path("reports/stable-daily-panel")
DEFAULT_ACCEPTED_CONFIDENCE = (
    "iex_snapshot_overlap",
    "iex_snapshot_changed_during_window",
    "iex_snapshot_removed_before_latest",
)
QUALITY_EVENT_TYPES = (
    "invalid_ohlc",
    "nonpositive_price",
    "near_zero_price",
    "extreme_return",
    "notional_outlier",
    "volume_outlier",
)


@dataclass(frozen=True)
class StableDailyPanelConfig:
    universe_path: Path
    daily_bars_root: Path
    quality_events_path: Path
    output_path: Path
    report_root: Path
    accepted_confidence: tuple[str, ...]
    start_day: str | None
    end_day: str | None
    compression: str
    replace: bool


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe-path", default=str(DEFAULT_UNIVERSE_PATH))
    parser.add_argument("--daily-bars-root", default=str(DEFAULT_DAILY_BARS_ROOT))
    parser.add_argument("--quality-events-path", default=str(DEFAULT_QUALITY_EVENTS_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--report-root", default=str(DEFAULT_REPORT_ROOT))
    parser.add_argument("--accepted-confidence", default=",".join(DEFAULT_ACCEPTED_CONFIDENCE))
    parser.add_argument("--start-day")
    parser.add_argument("--end-day")
    parser.add_argument("--compression", default="zstd", choices=["zstd", "snappy"])
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()
    config = StableDailyPanelConfig(
        universe_path=Path(args.universe_path),
        daily_bars_root=Path(args.daily_bars_root),
        quality_events_path=Path(args.quality_events_path),
        output_path=Path(args.output_path),
        report_root=Path(args.report_root),
        accepted_confidence=csv_tuple(args.accepted_confidence),
        start_day=args.start_day,
        end_day=args.end_day,
        compression=args.compression,
        replace=args.replace,
    )
    setup_logging(str(config.report_root / "stable_daily_panel.jsonl"))
    result = build_stable_daily_panel(config)
    get_logger(__name__).info(
        "stable daily panel complete",
        extra={"event": "stable_daily_panel_complete", "detail": result["summary"]},
    )
    return 0


def build_stable_daily_panel(config: StableDailyPanelConfig) -> dict[str, Any]:
    validate_inputs(config)
    config.report_root.mkdir(parents=True, exist_ok=True)
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    if config.output_path.exists() and not config.replace:
        raise FileExistsError(f"output exists; pass --replace to overwrite: {config.output_path}")
    universe = load_universe(config)
    panel = build_panel_frame(config, universe)
    write_panel(config.output_path, panel, config.compression)
    summary = build_summary(config, universe)
    write_report_outputs(config.report_root, summary)
    return {"summary": summary}


def validate_inputs(config: StableDailyPanelConfig) -> None:
    for path, label in [
        (config.universe_path, "stable enriched universe"),
        (config.daily_bars_root, "daily bars root"),
        (config.quality_events_path, "quality events parquet"),
    ]:
        if not path.exists():
            raise FileNotFoundError(f"{label} does not exist: {path}")
    if not daily_bar_paths(config.daily_bars_root):
        raise FileNotFoundError(f"no confirmed-trade daily bars found under {config.daily_bars_root}")


def load_universe(config: StableDailyPanelConfig) -> pl.DataFrame:
    required = ["symbol", "symbol_era_id", "liquidity_tier", "iex_entity_confidence"]
    frame = pl.read_parquet(config.universe_path)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{config.universe_path} missing required columns: {missing}")
    return (
        frame.filter(pl.col("iex_entity_confidence").is_in(config.accepted_confidence))
        .select([column for column in STABLE_METADATA_COLUMNS if column in frame.columns])
        .sort("symbol_era_id")
    )


def build_panel_frame(config: StableDailyPanelConfig, universe: pl.DataFrame) -> pl.DataFrame:
    bars = pl.scan_parquet(daily_bar_paths(config.daily_bars_root))
    bars = apply_day_filters(bars, config)
    joined = bars.join(universe.lazy(), on=["symbol", "symbol_era_id"], how="inner")
    quality = quality_flags(config.quality_events_path)
    flag_columns = [f"quality_{event}" for event in QUALITY_EVENT_TYPES]
    return (
        joined.join(quality, on=["day", "symbol_era_id"], how="left")
        .with_columns(pl.col(column).fill_null(False) for column in flag_columns)
        .with_columns(pl.any_horizontal(flag_columns).alias("quality_has_event"))
        .sort(["symbol_era_id", "day"])
        .collect()
    )


def apply_day_filters(frame: pl.LazyFrame, config: StableDailyPanelConfig) -> pl.LazyFrame:
    if config.start_day:
        frame = frame.filter(pl.col("day") >= config.start_day)
    if config.end_day:
        frame = frame.filter(pl.col("day") <= config.end_day)
    return frame


def quality_flags(path: Path) -> pl.LazyFrame:
    events = pl.scan_parquet(path).select("day", "symbol_era_id", "event_type")
    return events.group_by("day", "symbol_era_id").agg(
        [(pl.col("event_type") == event).any().alias(f"quality_{event}") for event in QUALITY_EVENT_TYPES]
    )


def write_panel(path: Path, panel: pl.DataFrame, compression: str) -> None:
    tmp_path = path.with_name(path.name + ".tmp")
    panel.write_parquet(tmp_path, compression=compression)
    tmp_path.replace(path)


def build_summary(config: StableDailyPanelConfig, universe: pl.DataFrame) -> dict[str, Any]:
    panel = pl.scan_parquet(config.output_path)
    rows = panel.select(
        pl.len().alias("panel_row_count"),
        pl.col("day").min().alias("first_day"),
        pl.col("day").max().alias("last_day"),
        pl.col("symbol_era_id").n_unique().alias("symbol_era_count"),
        pl.col("quality_has_event").sum().alias("quality_event_row_count"),
    ).collect().to_dicts()[0]
    tier_counts = count_by(universe, "liquidity_tier")
    confidence_counts = count_by(universe, "iex_entity_confidence")
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "stable daily confirmed-trade OHLCV panel enriched with IEX entity evidence",
        "universe_path": str(config.universe_path),
        "daily_bars_root": str(config.daily_bars_root),
        "quality_events_path": str(config.quality_events_path),
        "output_path": str(config.output_path),
        "accepted_confidence": list(config.accepted_confidence),
        "selected_stable_era_count": universe.height,
        "panel_row_count": rows["panel_row_count"],
        "first_day": rows["first_day"],
        "last_day": rows["last_day"],
        "panel_symbol_era_count": rows["symbol_era_count"],
        "quality_event_row_count": rows["quality_event_row_count"],
        "output_size_bytes": config.output_path.stat().st_size,
        "liquidity_tier_counts": tier_counts,
        "entity_confidence_counts": confidence_counts,
    }


def write_report_outputs(root: Path, summary: dict[str, Any]) -> None:
    (root / "stable_daily_panel_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(root / "stable_daily_panel_report.md", summary)


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Stable Daily Panel",
        "",
        "This table is the research-ready confirmed-trade daily OHLCV panel for stable ticker eras.",
        "",
        f"- Output: `{summary['output_path']}`",
        f"- Date range: `{summary['first_day']}` to `{summary['last_day']}`",
        f"- Stable eras selected: `{summary['selected_stable_era_count']}`",
        f"- Stable eras present in panel: `{summary['panel_symbol_era_count']}`",
        f"- Daily panel rows: `{summary['panel_row_count']}`",
        f"- Rows with quality events: `{summary['quality_event_row_count']}`",
        f"- Output size: `{summary['output_size_bytes']}` bytes",
        "",
        "## Liquidity Tiers",
        "",
    ]
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["liquidity_tier_counts"].items())
    lines.extend(["", "## Entity Confidence", ""])
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["entity_confidence_counts"].items())
    lines.extend(["", "## Caveat", ""])
    lines.append("Prices are raw TOPS confirmed-trade prices and are not split/dividend adjusted.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def daily_bar_paths(root: Path) -> list[str]:
    return sorted(str(path) for path in root.glob("*/*/*_confirmed_trade_bars.parquet"))


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {row[column]: row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()}


def csv_tuple(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


STABLE_METADATA_COLUMNS = [
    "symbol",
    "symbol_era_id",
    "liquidity_tier",
    "first_day",
    "last_day",
    "expected_days_in_era",
    "coverage_ratio",
    "trade_day_coverage_ratio",
    "median_daily_notional",
    "median_daily_volume",
    "median_daily_trade_count",
    "identity_status",
    "iex_latest_issuer",
    "iex_product_hint",
    "iex_issuer_variant_count",
    "iex_seen_in_latest",
    "iex_removed_after_seen",
    "iex_entity_confidence",
]


if __name__ == "__main__":
    raise SystemExit(main())
