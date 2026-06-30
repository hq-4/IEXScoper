from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_dead_ticker_review_queue import (
    DeadTickerReviewConfig,
    build_dead_ticker_review_queue,
)


def test_build_dead_ticker_review_queue_classifies_evidence_and_hints(tmp_path: Path) -> None:
    sec_path = tmp_path / "sec.parquet"
    iex_path = tmp_path / "iex.parquet"
    output_root = tmp_path / "out"
    _write_sec(sec_path)
    _write_iex(iex_path)

    result = build_dead_ticker_review_queue(
        DeadTickerReviewConfig(sec_eras_path=sec_path, iex_eras_path=iex_path, output_root=output_root)
    )

    rows = {
        row["symbol_era_id"]: row
        for row in pl.read_parquet(output_root / "dead_ticker_review_queue.parquet").to_dicts()
    }
    assert "STABLE#001" not in rows
    assert rows["DEAD#001"]["identity_evidence_status"] == "historical_identity_unresolved"
    assert rows["DEAD#001"]["review_priority"] == 1
    assert rows["AACIU#001"]["instrument_hint"] == "probable_unit"
    assert rows["AACIW#001"]["instrument_hint"] == "probable_warrant"
    assert rows["CUR#001"]["identity_evidence_status"] == "current_sec_and_iex_evidence"
    assert result["summary"]["review_era_count"] == 4
    assert (output_root / "dead_ticker_review_report.md").exists()


def _write_sec(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["DEAD", "AACIU", "AACIW", "CUR", "STABLE"],
            "symbol_era_id": ["DEAD#001", "AACIU#001", "AACIW#001", "CUR#001", "STABLE#001"],
            "source_classification": [
                "delisted_or_acquired_candidate",
                "intermittent_or_reused_candidate",
                "intermittent_or_reused_candidate",
                "intermittent_full_window_candidate",
                "stable_candidate",
            ],
            "first_day": ["20170103"] * 5,
            "last_day": ["20180103"] * 5,
            "observed_days": [10, 20, 30, 40, 50],
            "trade_rows": [1000, 2000, 3000, 4000, 5000],
            "main_rows": [1100, 2100, 3100, 4100, 5100],
            "sec_current_confidence": [
                "sec_unmatched",
                "sec_unmatched",
                "sec_unmatched",
                "sec_current_match",
                "sec_current_match",
            ],
            "sec_cik": [None, None, None, "0000000001", "0000000002"],
            "sec_name": [None, None, None, "CURRENT INC", "STABLE INC"],
            "sec_ticker": [None, None, None, "CUR", "STABLE"],
            "sec_exchange": [None, None, None, "NYSE", "Nasdaq"],
        }
    ).write_parquet(path)


def _write_iex(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol_era_id": ["DEAD#001", "AACIU#001", "AACIW#001", "CUR#001", "STABLE#001"],
            "iex_entity_confidence": [
                "iex_snapshot_unmatched",
                "iex_snapshot_unmatched",
                "iex_snapshot_unmatched",
                "iex_snapshot_overlap",
                "iex_snapshot_overlap",
            ],
            "iex_latest_issuer": [None, None, None, "CURRENT INC", "STABLE INC"],
            "iex_product_hint": [None, None, None, "operating_or_other", "operating_or_other"],
            "iex_seen_in_latest": [False, False, False, True, True],
        }
    ).write_parquet(path)
