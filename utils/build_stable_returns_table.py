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

DEFAULT_PANEL_PATH = Path("/media/tn/pq/derived/stable-daily-panel/stable_daily_panel.parquet")
DEFAULT_OUTPUT_PATH = Path("/media/tn/pq/derived/stable-returns/stable_returns.parquet")
DEFAULT_REPORT_ROOT = Path("reports/stable-returns")
POTENTIAL_CORP_ACTION_THRESHOLD = 0.45
RETURN_COLUMNS = [
    "day",
    "symbol",
    "symbol_era_id",
    "liquidity_tier",
    "close",
    "prev_day",
    "prev_close",
    "raw_close_return",
    "log_close_return",
    "volume",
    "trade_count",
    "notional",
    "vwap",
    "quality_has_event",
    "prev_quality_has_event",
    "is_clean_return_day",
    "potential_corporate_action",
    "dirty_return_reason",
    "iex_latest_issuer",
    "iex_entity_confidence",
]


@dataclass(frozen=True)
class StableReturnsConfig:
    panel_path: Path
    output_path: Path
    report_root: Path
    compression: str
    replace: bool


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--panel-path", default=str(DEFAULT_PANEL_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--report-root", default=str(DEFAULT_REPORT_ROOT))
    parser.add_argument("--compression", default="zstd", choices=["zstd", "snappy"])
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()
    config = StableReturnsConfig(
        panel_path=Path(args.panel_path),
        output_path=Path(args.output_path),
        report_root=Path(args.report_root),
        compression=args.compression,
        replace=args.replace,
    )
    setup_logging(str(config.report_root / "stable_returns.jsonl"))
    result = build_stable_returns_table(config)
    get_logger(__name__).info(
        "stable returns table complete",
        extra={"event": "stable_returns_table_complete", "detail": result["summary"]},
    )
    return 0


def build_stable_returns_table(config: StableReturnsConfig) -> dict[str, Any]:
    validate_inputs(config)
    config.report_root.mkdir(parents=True, exist_ok=True)
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    if config.output_path.exists() and not config.replace:
        raise FileExistsError(f"output exists; pass --replace to overwrite: {config.output_path}")
    returns = build_returns_frame(config.panel_path)
    write_returns(config.output_path, returns, config.compression)
    summary = build_summary(config)
    write_report_outputs(config.report_root, summary)
    return {"summary": summary}


def validate_inputs(config: StableReturnsConfig) -> None:
    if not config.panel_path.exists():
        raise FileNotFoundError(f"stable daily panel does not exist: {config.panel_path}")
    missing = missing_columns(config.panel_path)
    if missing:
        raise ValueError(f"{config.panel_path} missing required columns: {missing}")


def missing_columns(path: Path) -> list[str]:
    columns = pl.scan_parquet(path).collect_schema().names()
    required = [
        "day",
        "symbol",
        "symbol_era_id",
        "liquidity_tier",
        "close",
        "volume",
        "trade_count",
        "notional",
        "vwap",
        "quality_has_event",
        "iex_latest_issuer",
        "iex_entity_confidence",
    ]
    return [column for column in required if column not in columns]


def build_returns_frame(panel_path: Path) -> pl.DataFrame:
    panel = pl.scan_parquet(panel_path).select(
        "day",
        "symbol",
        "symbol_era_id",
        "liquidity_tier",
        "close",
        "volume",
        "trade_count",
        "notional",
        "vwap",
        "quality_has_event",
        "iex_latest_issuer",
        "iex_entity_confidence",
    )
    return (
        panel.sort(["symbol_era_id", "day"])
        .with_columns(
            pl.col("day").shift(1).over("symbol_era_id").alias("prev_day"),
            pl.col("close").shift(1).over("symbol_era_id").alias("prev_close"),
            pl.col("quality_has_event")
            .shift(1)
            .over("symbol_era_id")
            .alias("prev_quality_has_event"),
        )
        .with_columns(return_expr().alias("raw_close_return"))
        .with_columns(pl.col("raw_close_return").log1p().alias("log_close_return"))
        .with_columns(
            pl.col("raw_close_return").abs().ge(POTENTIAL_CORP_ACTION_THRESHOLD).fill_null(False).alias(
                "potential_corporate_action"
            )
        )
        .with_columns(dirty_reason_expr().alias("dirty_return_reason"))
        .with_columns(pl.col("dirty_return_reason").is_null().alias("is_clean_return_day"))
        .select(RETURN_COLUMNS)
        .collect()
    )


def return_expr() -> pl.Expr:
    return (
        pl.when(pl.col("prev_close").is_not_null() & (pl.col("prev_close") > 0))
        .then((pl.col("close") / pl.col("prev_close")) - 1.0)
        .otherwise(None)
    )


def dirty_reason_expr() -> pl.Expr:
    return (
        pl.when(pl.col("prev_close").is_null())
        .then(pl.lit("first_observation"))
        .when(pl.col("prev_close") <= 0)
        .then(pl.lit("nonpositive_prev_close"))
        .when(pl.col("close") <= 0)
        .then(pl.lit("nonpositive_close"))
        .when(pl.col("prev_quality_has_event").fill_null(False))
        .then(pl.lit("previous_day_quality_event"))
        .when(pl.col("quality_has_event"))
        .then(pl.lit("current_day_quality_event"))
        .otherwise(None)
    )


def write_returns(path: Path, frame: pl.DataFrame, compression: str) -> None:
    tmp_path = path.with_name(path.name + ".tmp")
    frame.write_parquet(tmp_path, compression=compression)
    tmp_path.replace(path)


def build_summary(config: StableReturnsConfig) -> dict[str, Any]:
    returns = pl.scan_parquet(config.output_path)
    core = returns.select(
        pl.len().alias("row_count"),
        pl.col("symbol_era_id").n_unique().alias("symbol_era_count"),
        pl.col("day").min().alias("first_day"),
        pl.col("day").max().alias("last_day"),
        pl.col("raw_close_return").is_not_null().sum().alias("return_observation_count"),
        pl.col("is_clean_return_day").sum().alias("clean_return_count"),
        (~pl.col("is_clean_return_day")).sum().alias("dirty_return_count"),
        pl.col("potential_corporate_action").sum().alias("potential_corporate_action_count"),
    ).collect().to_dicts()[0]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "stable daily close-to-close raw returns from confirmed-trade panel",
        "panel_path": str(config.panel_path),
        "output_path": str(config.output_path),
        "output_size_bytes": config.output_path.stat().st_size,
        "dirty_reason_counts": count_by(returns, "dirty_return_reason"),
        "liquidity_tier_counts": count_by(returns, "liquidity_tier"),
        **core,
    }


def count_by(frame: pl.LazyFrame, column: str) -> dict[str, int]:
    return {
        str(row[column]): row["len"]
        for row in frame.group_by(column).len().sort(column).collect().to_dicts()
        if row[column] is not None
    }


def write_report_outputs(root: Path, summary: dict[str, Any]) -> None:
    (root / "stable_returns_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(root / "stable_returns_report.md", summary)


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Stable Returns Table",
        "",
        "This table derives close-to-close returns from the validated stable daily panel.",
        "",
        f"- Output: `{summary['output_path']}`",
        f"- Date range: `{summary['first_day']}` to `{summary['last_day']}`",
        f"- Rows: `{summary['row_count']}`",
        f"- Return observations: `{summary['return_observation_count']}`",
        f"- Clean return observations: `{summary['clean_return_count']}`",
        f"- Dirty return observations: `{summary['dirty_return_count']}`",
        f"- Potential corporate-action-like jumps: `{summary['potential_corporate_action_count']}`",
        f"- Output size: `{summary['output_size_bytes']}` bytes",
        "",
        "## Dirty Return Reasons",
        "",
    ]
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["dirty_reason_counts"].items())
    lines.extend(["", "## Caveat", ""])
    lines.append("Returns are raw close-to-close returns and are not split/dividend adjusted.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
