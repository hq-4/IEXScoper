from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl


def write_schema_failure(root: Path, summary: dict[str, Any]) -> None:
    (root / "stable_daily_panel_validation_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(root / "stable_daily_panel_validation_report.md", summary)


def write_outputs(
    root: Path,
    summary: dict[str, Any],
    nulls: pl.DataFrame,
    flag_mismatches: pl.DataFrame,
    year_tier: pl.DataFrame,
    era_coverage: pl.DataFrame,
    confidence: pl.DataFrame,
    tier: pl.DataFrame,
) -> None:
    (root / "stable_daily_panel_validation_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    nulls.write_csv(root / "critical_null_counts.csv")
    flag_mismatches.write_csv(root / "quality_flag_mismatches.csv")
    year_tier.write_csv(root / "year_tier_coverage.csv")
    era_coverage.write_csv(root / "era_coverage.csv")
    confidence.write_csv(root / "entity_confidence_counts.csv")
    tier.write_csv(root / "liquidity_tier_counts.csv")
    write_markdown(root / "stable_daily_panel_validation_report.md", summary)


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Stable Daily Panel Validation",
        "",
        f"- Status: `{summary['validation_status']}`",
        f"- Hard failures: `{summary['hard_failure_count']}`",
        f"- Panel: `{summary['panel_path']}`",
        f"- Quality events: `{summary['quality_events_path']}`",
    ]
    if missing := summary.get("missing_columns"):
        lines.extend(["", "## Missing Columns", ""])
        lines.extend(f"- `{column}`" for column in missing)
    else:
        lines.extend(core_metric_lines(summary))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def core_metric_lines(summary: dict[str, Any]) -> list[str]:
    return [
        "",
        "## Core Metrics",
        "",
        f"- Rows: `{summary['row_count']}`",
        f"- Symbol eras: `{summary['symbol_era_count']}`",
        f"- Symbols: `{summary['symbol_count']}`",
        f"- Date range: `{summary['first_day']}` to `{summary['last_day']}`",
        f"- Duplicate key rows: `{summary['duplicate_key_count']}`",
        f"- Critical nulls: `{summary['critical_null_count']}`",
        f"- Invalid OHLC rows: `{summary['invalid_ohlc_count']}`",
        f"- Quality flag/source mismatches: `{summary['quality_flag_source_mismatch_count']}`",
        f"- Min era panel-day coverage: `{summary['min_panel_day_coverage_ratio']:.6f}`",
        f"- Median era panel-day coverage: `{summary['median_panel_day_coverage_ratio']:.6f}`",
    ]
