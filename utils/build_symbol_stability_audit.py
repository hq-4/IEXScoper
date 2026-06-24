from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.iextools_backfill_core import existing_tops_days, tops_output_paths

DEFAULT_OUTPUT_ROOT = Path("reports/symbol-stability")
DEFAULT_START_DAY = "20160101"
DEFAULT_END_DAY = "20260622"
DEFAULT_MIN_COVERAGE = 0.95
DEFAULT_MAJOR_GAP_DAYS = 14
MAIN_FILE_SUFFIX = "_IEXTP1_TOPS1.6.parquet"
TRADE_TYPE = "TradeReport"


@dataclass
class SymbolStats:
    symbol: str
    first_day: str = ""
    last_day: str = ""
    observed_days: set[str] = field(default_factory=set)
    main_rows: int = 0
    trade_rows: int = 0
    min_timestamp: int | None = None
    max_timestamp: int | None = None

    def add_day(self, day: str, row: dict[str, Any]) -> None:
        self.observed_days.add(day)
        self.main_rows += int(row.get("main_rows") or 0)
        self.trade_rows += int(row.get("trade_rows") or 0)
        self.first_day = min(filter(None, [self.first_day, day])) if self.first_day else day
        self.last_day = max(self.last_day, day)
        self.min_timestamp = _min_optional(self.min_timestamp, row.get("min_timestamp"))
        self.max_timestamp = _max_optional(self.max_timestamp, row.get("max_timestamp"))


@dataclass(frozen=True)
class AuditConfig:
    parquet_root: Path
    output_root: Path
    start_day: str
    end_day: str
    min_coverage: float
    major_gap_days: int
    limit_days: int | None


@dataclass(frozen=True)
class CollectionResult:
    stats_by_symbol: dict[str, SymbolStats]
    scanned_days: list[str]
    skipped_days: list[dict[str, str]]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet-root", default="/media/tn/pq")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--start-day", default=DEFAULT_START_DAY)
    parser.add_argument("--end-day", default=DEFAULT_END_DAY)
    parser.add_argument("--min-coverage", type=float, default=DEFAULT_MIN_COVERAGE)
    parser.add_argument("--major-gap-days", type=int, default=DEFAULT_MAJOR_GAP_DAYS)
    parser.add_argument("--limit-days", type=int)
    args = parser.parse_args()

    config = AuditConfig(
        parquet_root=Path(args.parquet_root),
        output_root=Path(args.output_root),
        start_day=args.start_day,
        end_day=args.end_day,
        min_coverage=args.min_coverage,
        major_gap_days=args.major_gap_days,
        limit_days=args.limit_days,
    )
    setup_logging(str(config.output_root / "symbol_stability_audit.jsonl"))
    logger = get_logger(__name__)
    result = build_symbol_stability_audit(config)
    logger.info(
        "symbol stability audit complete",
        extra={"event": "symbol_stability_audit_complete", "detail": result["summary"]},
    )
    return 0


def build_symbol_stability_audit(config: AuditConfig) -> dict[str, Any]:
    config.output_root.mkdir(parents=True, exist_ok=True)
    days = discover_completed_days(config)
    collection = collect_symbol_stats(config.parquet_root, days)
    rows = build_symbol_rows(
        collection.stats_by_symbol,
        collection.scanned_days,
        min_coverage=config.min_coverage,
        major_gap_days=config.major_gap_days,
    )
    summary = build_summary(config, days, collection.scanned_days, collection.skipped_days, rows)
    write_outputs(config.output_root, summary, rows)
    return {"summary": summary, "rows": rows}


def discover_completed_days(config: AuditConfig) -> list[str]:
    days = [
        day
        for day in sorted(existing_tops_days(config.parquet_root))
        if config.start_day <= day <= config.end_day
    ]
    if config.limit_days is not None:
        return days[: config.limit_days]
    return days


def collect_symbol_stats(parquet_root: Path, days: list[str]) -> CollectionResult:
    stats_by_symbol: dict[str, SymbolStats] = {}
    scanned_days: list[str] = []
    skipped_days: list[dict[str, str]] = []
    logger = get_logger(__name__)
    for index, day in enumerate(days, start=1):
        main_path, _ = tops_output_paths(parquet_root, day)
        if not main_path.exists():
            continue
        try:
            rows = summarize_day_symbols(main_path)
        except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
            skipped = {"day": day, "path": str(main_path), "error": f"{type(exc).__name__}: {exc}"}
            skipped_days.append(skipped)
            logger.warning(
                "symbol day skipped",
                extra={
                    "event": "symbol_stability_day_skipped",
                    "day": day,
                    "detail": skipped,
                },
            )
            continue
        for row in rows:
            symbol = row["symbol"]
            stats = stats_by_symbol.setdefault(symbol, SymbolStats(symbol=symbol))
            stats.add_day(day, row)
        scanned_days.append(day)
        logger.info(
            "symbol day scanned",
            extra={
                "event": "symbol_stability_day_scanned",
                "day": day,
                "detail": {"index": index, "day_count": len(days), "path": str(main_path)},
            },
        )
    return CollectionResult(stats_by_symbol, scanned_days, skipped_days)


def summarize_day_symbols(path: Path) -> list[dict[str, Any]]:
    columns = pl.scan_parquet(str(path)).collect_schema().names()
    required = ["symbol", "timestamp"]
    missing = [column for column in required if column not in columns]
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    type_expr = (
        (pl.col("type") == TRADE_TYPE).cast(pl.Int64).sum().alias("trade_rows")
        if "type" in columns
        else pl.lit(None).alias("trade_rows")
    )
    frame = (
        pl.scan_parquet(str(path))
        .select(["symbol", "timestamp", *([] if "type" not in columns else ["type"])])
        .filter(pl.col("symbol").is_not_null())
        .group_by("symbol")
        .agg(
            pl.len().alias("main_rows"),
            type_expr,
            pl.col("timestamp").min().alias("min_timestamp"),
            pl.col("timestamp").max().alias("max_timestamp"),
        )
        .collect()
    )
    return frame.to_dicts()


def build_symbol_rows(
    stats_by_symbol: dict[str, SymbolStats],
    completed_days: list[str],
    *,
    min_coverage: float,
    major_gap_days: int,
) -> list[dict[str, Any]]:
    if not completed_days:
        return []
    first_window_day = completed_days[0]
    last_window_day = completed_days[-1]
    rows = [
        build_symbol_row(
            stats,
            completed_days,
            first_window_day=first_window_day,
            last_window_day=last_window_day,
            min_coverage=min_coverage,
            major_gap_days=major_gap_days,
        )
        for stats in stats_by_symbol.values()
    ]
    return sorted(rows, key=lambda row: (row["classification"], row["symbol"]))


def build_symbol_row(
    stats: SymbolStats,
    completed_days: list[str],
    *,
    first_window_day: str,
    last_window_day: str,
    min_coverage: float,
    major_gap_days: int,
) -> dict[str, Any]:
    observed = sorted(stats.observed_days)
    expected = [day for day in completed_days if stats.first_day <= day <= stats.last_day]
    missing_inside = sorted(set(expected) - stats.observed_days)
    major_gaps = major_gap_ranges(observed, major_gap_days)
    coverage = len(observed) / len(expected) if expected else 0.0
    classification = classify_symbol(
        first_day=stats.first_day,
        last_day=stats.last_day,
        first_window_day=first_window_day,
        last_window_day=last_window_day,
        coverage=coverage,
        major_gap_count=len(major_gaps),
        min_coverage=min_coverage,
    )
    return {
        "symbol": stats.symbol,
        "classification": classification,
        "first_day": stats.first_day,
        "last_day": stats.last_day,
        "observed_days": len(observed),
        "expected_days_in_era": len(expected),
        "coverage_ratio": round(coverage, 6),
        "missing_days_inside_era": len(missing_inside),
        "major_gap_count": len(major_gaps),
        "largest_gap_calendar_days": largest_gap_days(observed),
        "main_rows": stats.main_rows,
        "trade_rows": stats.trade_rows,
        "min_timestamp": stats.min_timestamp,
        "max_timestamp": stats.max_timestamp,
        "sample_missing_days_inside_era": missing_inside[:10],
        "sample_major_gaps": major_gaps[:10],
        "identity_note": "ticker-era only; enrich with CIK/FIGI/CUSIP before issuer-level claims",
    }


def classify_symbol(
    *,
    first_day: str,
    last_day: str,
    first_window_day: str,
    last_window_day: str,
    coverage: float,
    major_gap_count: int,
    min_coverage: float,
) -> str:
    if first_day == first_window_day and last_day == last_window_day:
        if coverage >= min_coverage and major_gap_count == 0:
            return "stable_candidate"
        return "intermittent_full_window_candidate"
    if major_gap_count > 0:
        return "intermittent_or_reused_candidate"
    if first_day > first_window_day and last_day == last_window_day:
        return "ipo_or_new_listing_candidate"
    if first_day == first_window_day and last_day < last_window_day:
        return "delisted_or_acquired_candidate"
    return "partial_window_candidate"


def major_gap_ranges(observed_days: list[str], threshold_days: int) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    for prev_day, next_day in zip(observed_days, observed_days[1:], strict=False):
        gap = calendar_gap_days(prev_day, next_day)
        if gap > threshold_days:
            gaps.append({"from_day": prev_day, "to_day": next_day, "calendar_gap_days": gap})
    return gaps


def largest_gap_days(observed_days: list[str]) -> int:
    if len(observed_days) < 2:
        return 0
    return max(calendar_gap_days(prev, nxt) for prev, nxt in zip(observed_days, observed_days[1:]))


def calendar_gap_days(prev_day: str, next_day: str) -> int:
    return (parse_day(next_day) - parse_day(prev_day)).days


def build_summary(
    config: AuditConfig,
    discovered_days: list[str],
    scanned_days: list[str],
    skipped_days: list[dict[str, str]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["classification"]] = counts.get(row["classification"], 0) + 1
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "parquet_root": str(config.parquet_root),
        "start_day": config.start_day,
        "end_day": config.end_day,
        "completed_day_count": len(discovered_days),
        "scanned_day_count": len(scanned_days),
        "skipped_day_count": len(skipped_days),
        "first_completed_day": discovered_days[0] if discovered_days else None,
        "last_completed_day": discovered_days[-1] if discovered_days else None,
        "first_scanned_day": scanned_days[0] if scanned_days else None,
        "last_scanned_day": scanned_days[-1] if scanned_days else None,
        "symbol_count": len(rows),
        "classification_counts": dict(sorted(counts.items())),
        "skipped_days": skipped_days,
        "method": "main TOPS Parquet ticker-era continuity audit",
        "limitations": [
            "Ticker strings are not issuer identifiers.",
            "Rows are classified from observed TOPS main-file symbols only.",
            "Use an external security master for CIK/FIGI/CUSIP-level identity.",
        ],
    }


def write_outputs(output_root: Path, summary: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    (output_root / "symbol_stability_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )
    (output_root / "symbol_stability_rows.jsonl").write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + ("\n" if rows else ""),
        encoding="utf-8",
    )
    write_csv(output_root / "symbol_stability_rows.csv", rows)
    write_markdown(output_root / "symbol_stability_report.md", summary, rows)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "symbol",
        "classification",
        "first_day",
        "last_day",
        "observed_days",
        "expected_days_in_era",
        "coverage_ratio",
        "missing_days_inside_era",
        "major_gap_count",
        "largest_gap_calendar_days",
        "main_rows",
        "trade_rows",
        "identity_note",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def write_markdown(path: Path, summary: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    top_gaps = sorted(rows, key=lambda row: row["major_gap_count"], reverse=True)[:20]
    lines = [
        "# Symbol Stability Audit",
        "",
        "This is a ticker-era continuity audit from TOPS main Parquet files.",
        "It is not a substitute for CIK/FIGI/CUSIP security-master identity mapping.",
        "",
        f"- Window: `{summary['start_day']}` to `{summary['end_day']}`",
        f"- Completed TOPS days scanned: `{summary['completed_day_count']}`",
        f"- Readable TOPS days used: `{summary['scanned_day_count']}`",
        f"- Skipped TOPS days: `{summary['skipped_day_count']}`",
        f"- Symbols observed: `{summary['symbol_count']}`",
        "",
        "## Classification Counts",
        "",
    ]
    lines.extend(
        f"- `{name}`: `{count}`" for name, count in summary["classification_counts"].items()
    )
    if summary["skipped_days"]:
        lines.extend(["", "## Skipped Days", ""])
        lines.extend(
            f"- `{row['day']}`: `{row['error']}` ({row['path']})"
            for row in summary["skipped_days"][:50]
        )
    lines.extend(
        [
            "",
            "## Highest Gap Counts",
            "",
            "| symbol | classification | first | last | coverage | major gaps | largest gap |",
        ]
    )
    lines.extend(["|---|---|---:|---:|---:|---:|---:|"])
    for row in top_gaps:
        lines.append(
            "| {symbol} | {classification} | {first_day} | {last_day} | "
            "{coverage_ratio} | {major_gap_count} | {largest_gap_calendar_days} |".format(**row)
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_day(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def _min_optional(left: int | None, right: Any) -> int | None:
    if right is None:
        return left
    right_int = int(right)
    return right_int if left is None else min(left, right_int)


def _max_optional(left: int | None, right: Any) -> int | None:
    if right is None:
        return left
    right_int = int(right)
    return right_int if left is None else max(left, right_int)


if __name__ == "__main__":
    raise SystemExit(main())
