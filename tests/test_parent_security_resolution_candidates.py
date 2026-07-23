from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_parent_security_resolution_candidates import (
    ParentSecurityResolutionConfig,
    build_parent_security_resolution_candidates,
)


def test_parent_security_resolution_candidates_link_root_evidence(tmp_path: Path) -> None:
    priority_path = tmp_path / "priority.parquet"
    sec_path = tmp_path / "sec.parquet"
    iex_path = tmp_path / "iex.parquet"
    output_path = tmp_path / "parent_candidates.csv"
    summary_path = tmp_path / "summary.json"
    _write_priority(priority_path)
    _write_sec(sec_path)
    _write_iex(iex_path)

    result = build_parent_security_resolution_candidates(
        ParentSecurityResolutionConfig(
            priority_queue_path=priority_path,
            sec_eras_path=sec_path,
            iex_eras_path=iex_path,
            output_path=output_path,
            summary_path=summary_path,
        )
    )

    rows = {row["symbol_era_id"]: row for row in pl.read_csv(output_path).to_dicts()}
    assert result["summary"]["candidate_count"] == 1
    assert rows["ABC-A#001"]["resolution_status"] == "terminal_disposition"
    assert rows["ABC-A#001"]["resolution_disposition"] == "terminal_parent_security_linked"
    assert rows["ABC-A#001"]["historical_issuer_name"] == "ABC Corp"
    assert "XYZW#001" not in rows


def _write_priority(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["ABC-A", "XYZW"],
            "symbol_era_id": ["ABC-A#001", "XYZW#001"],
            "research_route": [
                "preferred_redemption_or_delisting",
                "warrant_unit_right_security_action",
            ],
            "instrument_type": ["probable_preferred", "probable_warrant"],
            "first_day": ["20200102", "20210102"],
            "last_day": ["20220102", "20230102"],
        }
    ).write_parquet(path)


def _write_sec(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["ABC", "XYZ"],
            "symbol_era_id": ["ABC#001", "XYZ#001"],
            "sec_current_confidence": ["sec_current_match", "sec_unmatched"],
            "sec_cik": ["0000000001", None],
            "sec_name": ["ABC Corp", None],
        }
    ).write_parquet(path)


def _write_iex(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol_era_id": ["ABC#001", "XYZ#001"],
            "iex_entity_confidence": ["iex_snapshot_unmatched", "iex_snapshot_unmatched"],
            "iex_latest_issuer": [None, None],
        }
    ).write_parquet(path)
