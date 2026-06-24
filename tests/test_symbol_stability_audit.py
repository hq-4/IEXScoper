from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import utils.build_symbol_stability_audit as audit
from utils.build_symbol_stability_audit import (
    AuditConfig,
    build_symbol_stability_audit,
    classify_symbol,
    major_gap_ranges,
)


def test_classify_symbol_stable_partial_and_gap() -> None:
    assert (
        classify_symbol(
            first_day="20250102",
            last_day="20250106",
            first_window_day="20250102",
            last_window_day="20250106",
            coverage=1.0,
            major_gap_count=0,
            min_coverage=0.95,
        )
        == "stable_candidate"
    )
    assert (
        classify_symbol(
            first_day="20250103",
            last_day="20250106",
            first_window_day="20250102",
            last_window_day="20250106",
            coverage=1.0,
            major_gap_count=0,
            min_coverage=0.95,
        )
        == "ipo_or_new_listing_candidate"
    )
    assert (
        classify_symbol(
            first_day="20250102",
            last_day="20250106",
            first_window_day="20250102",
            last_window_day="20250106",
            coverage=0.6,
            major_gap_count=1,
            min_coverage=0.95,
        )
        == "intermittent_full_window_candidate"
    )


def test_major_gap_ranges_detects_calendar_gap() -> None:
    assert major_gap_ranges(["20250102", "20250103", "20250120"], 14) == [
        {"from_day": "20250103", "to_day": "20250120", "calendar_gap_days": 17}
    ]


def test_build_symbol_stability_audit_outputs_rows(tmp_path: Path) -> None:
    parquet_root = tmp_path / "pq"
    _write_day(parquet_root, "20250102", ["AAA", "AAA", "OLD"])
    _write_day(parquet_root, "20250103", ["AAA", "NEW"])
    _write_day(parquet_root, "20250106", ["AAA", "NEW"])

    result = build_symbol_stability_audit(
        AuditConfig(
            parquet_root=parquet_root,
            output_root=tmp_path / "report",
            start_day="20250102",
            end_day="20250106",
            min_coverage=0.95,
            major_gap_days=14,
            limit_days=None,
        )
    )

    rows = {row["symbol"]: row for row in result["rows"]}
    assert rows["AAA"]["classification"] == "stable_candidate"
    assert rows["NEW"]["classification"] == "ipo_or_new_listing_candidate"
    assert rows["OLD"]["classification"] == "delisted_or_acquired_candidate"
    assert (tmp_path / "report" / "symbol_stability_summary.json").exists()
    assert (tmp_path / "report" / "symbol_stability_rows.csv").exists()
    assert (tmp_path / "report" / "symbol_stability_report.md").exists()


def test_build_symbol_stability_audit_skips_unreadable_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parquet_root = tmp_path / "pq"
    _write_day(parquet_root, "20250102", ["AAA"])
    _write_day(parquet_root, "20250103", ["AAA"])

    original = audit.summarize_day_symbols

    def fail_one_day(path: Path) -> list[dict[str, object]]:
        if "20250103" in path.name:
            raise ValueError("bad parquet")
        return original(path)

    monkeypatch.setattr(audit, "summarize_day_symbols", fail_one_day)

    result = build_symbol_stability_audit(
        AuditConfig(
            parquet_root=parquet_root,
            output_root=tmp_path / "report",
            start_day="20250102",
            end_day="20250103",
            min_coverage=0.95,
            major_gap_days=14,
            limit_days=None,
        )
    )

    assert result["summary"]["completed_day_count"] == 2
    assert result["summary"]["scanned_day_count"] == 1
    assert result["summary"]["skipped_day_count"] == 1
    assert result["summary"]["skipped_days"][0]["day"] == "20250103"


def _write_day(parquet_root: Path, day: str, symbols: list[str]) -> None:
    target_dir = parquet_root / day[:4] / day[4:6]
    target_dir.mkdir(parents=True, exist_ok=True)
    main_path = target_dir / f"{day}_IEXTP1_TOPS1.6.parquet"
    quote_path = target_dir / f"{day}_IEXTP1_TOPS1.6_QuoteUpdate.parquet"
    frame = pl.DataFrame(
        {
            "type": ["TradeReport"] * len(symbols),
            "timestamp": list(range(len(symbols))),
            "symbol": symbols,
        }
    )
    frame.write_parquet(main_path)
    pl.DataFrame({"symbol": symbols}).write_parquet(quote_path)
