"""Session-validity detection for TOPS daily Parquet captures.

IEX occasionally publishes weekend test-session captures (e.g. 20170826) where every
symbol emits OperationalHalt/TradingStatus noise with almost no TradeReports. Treating
those days as real trading sessions shatters ticker-era continuity, so downstream era
construction must quarantine them. [REH][PA]
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from utils.iextools_backfill_core import tops_output_paths

TRADE_TYPE = "TradeReport"
MIN_TRADE_SHARE = 0.5
REASON_WEEKEND = "weekend_session"
REASON_NO_TRADES = "no_trade_reports"
REASON_LOW_SHARE = "low_trade_share"


def is_weekend(day: str) -> bool:
    return datetime.strptime(day, "%Y%m%d").date().weekday() >= 5


def summarize_session(path: Path) -> dict[str, Any]:
    """Count total and TradeReport rows for one TOPS main Parquet file."""
    columns = pl.scan_parquet(str(path)).collect_schema().names()
    if "type" not in columns:
        raise ValueError(f"{path} missing required column: type")
    frame = pl.scan_parquet(str(path)).group_by("type").agg(pl.len().alias("rows")).collect()
    counts = {row["type"]: int(row["rows"]) for row in frame.to_dicts()}
    total = sum(counts.values())
    return {"total_rows": total, "trade_rows": counts.get(TRADE_TYPE, 0), "type_counts": counts}


def classify_session(
    day: str,
    *,
    total_rows: int,
    trade_rows: int,
    min_trade_share: float = MIN_TRADE_SHARE,
) -> dict[str, Any]:
    """Classify one session day as valid or quarantined with a machine-readable reason."""
    trade_share = trade_rows / total_rows if total_rows else 0.0
    reason = None
    if is_weekend(day):
        reason = REASON_WEEKEND
    elif trade_rows == 0:
        reason = REASON_NO_TRADES
    elif trade_share < min_trade_share:
        reason = REASON_LOW_SHARE
    return {
        "day": day,
        "valid": reason is None,
        "reason": reason,
        "total_rows": total_rows,
        "trade_rows": trade_rows,
        "trade_share": round(trade_share, 6),
    }


def build_validity_manifest(
    parquet_root: Path,
    days: list[str],
    *,
    min_trade_share: float = MIN_TRADE_SHARE,
) -> dict[str, Any]:
    """Scan days and build a quarantine manifest."""
    records = []
    for day in days:
        main_path, _ = tops_output_paths(parquet_root, day)
        stats = summarize_session(main_path)
        records.append(
            classify_session(
                day,
                total_rows=stats["total_rows"],
                trade_rows=stats["trade_rows"],
                min_trade_share=min_trade_share,
            )
        )
    quarantined = [record for record in records if not record["valid"]]
    return {
        "parquet_root": str(parquet_root),
        "min_trade_share": min_trade_share,
        "scanned_day_count": len(records),
        "valid_day_count": len(records) - len(quarantined),
        "quarantined_day_count": len(quarantined),
        "quarantined_days": quarantined,
    }


def write_validity_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_quarantined_days(path: Path) -> set[str]:
    """Return the quarantined day set from a manifest, or empty when the file is absent."""
    if not path.exists():
        return set()
    manifest = json.loads(path.read_text(encoding="utf-8"))
    return {record["day"] for record in manifest.get("quarantined_days", [])}
