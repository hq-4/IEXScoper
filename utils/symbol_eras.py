from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import polars as pl


class SymbolStatsLike(Protocol):
    symbol: str
    first_day: str
    last_day: str
    observed_days: set[str]
    daily_rows: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class EraBuildConfig:
    completed_days: list[str]
    first_window_day: str
    last_window_day: str
    major_gap_days: int
    min_coverage: float


def build_symbol_eras(
    stats_by_symbol: dict[str, SymbolStatsLike],
    symbol_rows: list[dict[str, Any]],
    config: EraBuildConfig,
) -> list[dict[str, Any]]:
    class_by_symbol = {row["symbol"]: row["classification"] for row in symbol_rows}
    eras: list[dict[str, Any]] = []
    for symbol in sorted(stats_by_symbol):
        stats = stats_by_symbol[symbol]
        segments = split_observed_days(sorted(stats.observed_days), config.major_gap_days)
        for index, observed in enumerate(segments, start=1):
            eras.append(
                build_symbol_era_row(
                    stats,
                    observed,
                    source_classification=class_by_symbol.get(symbol, "unknown"),
                    era_index=index,
                    era_count=len(segments),
                    config=config,
                )
            )
    return sorted(eras, key=lambda row: (row["symbol"], row["era_index"]))


def split_observed_days(observed_days: list[str], major_gap_days: int) -> list[list[str]]:
    if not observed_days:
        return []
    segments: list[list[str]] = [[observed_days[0]]]
    for prev_day, next_day in zip(observed_days, observed_days[1:], strict=False):
        if calendar_gap_days(prev_day, next_day) > major_gap_days:
            segments.append([])
        segments[-1].append(next_day)
    return segments


def build_symbol_era_row(
    stats: SymbolStatsLike,
    observed_days: list[str],
    *,
    source_classification: str,
    era_index: int,
    era_count: int,
    config: EraBuildConfig,
) -> dict[str, Any]:
    first_day = observed_days[0]
    last_day = observed_days[-1]
    expected_days = [day for day in config.completed_days if first_day <= day <= last_day]
    missing_days = sorted(set(expected_days) - set(observed_days))
    coverage = len(observed_days) / len(expected_days) if expected_days else 0.0
    daily_rows = [stats.daily_rows[day] for day in observed_days]
    main_rows = sum(int(row.get("main_rows") or 0) for row in daily_rows)
    trade_rows = sum(int(row.get("trade_rows") or 0) for row in daily_rows)
    return {
        "symbol": stats.symbol,
        "symbol_era_id": f"{stats.symbol}#{era_index:03d}",
        "era_index": era_index,
        "era_count": era_count,
        "source_classification": source_classification,
        "first_day": first_day,
        "last_day": last_day,
        "observed_days": len(observed_days),
        "expected_days_in_era": len(expected_days),
        "coverage_ratio": round(coverage, 6),
        "missing_days_inside_era": len(missing_days),
        "main_rows": main_rows,
        "trade_rows": trade_rows,
        "min_timestamp": _min_present(row.get("min_timestamp") for row in daily_rows),
        "max_timestamp": _max_present(row.get("max_timestamp") for row in daily_rows),
        "split_from_symbol": era_count > 1,
        "recommended_use": recommended_use(
            first_day=first_day,
            last_day=last_day,
            first_window_day=config.first_window_day,
            last_window_day=config.last_window_day,
            coverage=coverage,
            min_coverage=config.min_coverage,
            era_count=era_count,
        ),
        "identity_status": identity_status(era_count, source_classification),
        "identity_note": "ticker-era only; join FIGI/CUSIP/CIK before issuer-level claims",
    }


def recommended_use(
    *,
    first_day: str,
    last_day: str,
    first_window_day: str,
    last_window_day: str,
    coverage: float,
    min_coverage: float,
    era_count: int,
) -> str:
    if (
        era_count == 1
        and first_day == first_window_day
        and last_day == last_window_day
        and coverage >= min_coverage
    ):
        return "long_window_candidate"
    return "point_in_time_era"


def identity_status(era_count: int, source_classification: str) -> str:
    risky_classes = {
        "intermittent_full_window_candidate",
        "intermittent_or_reused_candidate",
        "partial_window_candidate",
    }
    if era_count > 1 or source_classification in risky_classes:
        return "needs_security_master"
    return "ticker_continuity_candidate"


def write_symbol_era_outputs(output_root: Path, rows: list[dict[str, Any]]) -> None:
    write_symbol_era_csv(output_root / "symbol_eras.csv", rows)
    (output_root / "symbol_eras.jsonl").write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + ("\n" if rows else ""),
        encoding="utf-8",
    )
    frame = pl.DataFrame(rows) if rows else pl.DataFrame(schema=SYMBOL_ERA_SCHEMA)
    frame.write_parquet(output_root / "symbol_eras.parquet", compression="zstd")


def write_symbol_era_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(SYMBOL_ERA_SCHEMA))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in SYMBOL_ERA_SCHEMA})


def calendar_gap_days(prev_day: str, next_day: str) -> int:
    from datetime import datetime

    return (datetime.strptime(next_day, "%Y%m%d") - datetime.strptime(prev_day, "%Y%m%d")).days


def _min_present(values) -> int | None:
    present = [int(value) for value in values if value is not None]
    return min(present) if present else None


def _max_present(values) -> int | None:
    present = [int(value) for value in values if value is not None]
    return max(present) if present else None


SYMBOL_ERA_SCHEMA = {
    "symbol": pl.String,
    "symbol_era_id": pl.String,
    "era_index": pl.Int64,
    "era_count": pl.Int64,
    "source_classification": pl.String,
    "first_day": pl.String,
    "last_day": pl.String,
    "observed_days": pl.Int64,
    "expected_days_in_era": pl.Int64,
    "coverage_ratio": pl.Float64,
    "missing_days_inside_era": pl.Int64,
    "main_rows": pl.Int64,
    "trade_rows": pl.Int64,
    "min_timestamp": pl.Int64,
    "max_timestamp": pl.Int64,
    "split_from_symbol": pl.Boolean,
    "recommended_use": pl.String,
    "identity_status": pl.String,
    "identity_note": pl.String,
}
