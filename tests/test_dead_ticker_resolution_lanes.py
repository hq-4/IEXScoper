from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_dead_ticker_resolution_lanes import (
    ResolutionLaneConfig,
    build_resolution_lanes,
)


def test_build_resolution_lanes_profiles_backlog(tmp_path: Path) -> None:
    priority_path = tmp_path / "priority.parquet"
    output_root = tmp_path / "lanes"
    _write_priority(priority_path)

    result = build_resolution_lanes(
        ResolutionLaneConfig(priority_queue_path=priority_path, output_root=output_root)
    )

    lanes = pl.read_parquet(output_root / "resolution_lanes.parquet")
    by_id = {row["symbol_era_id"]: row["resolution_lane"] for row in lanes.to_dicts()}
    assert by_id["AAA#001"] == "operating_terminal_event"
    assert by_id["BBB#001"] == "operating_sparse_intermit"
    assert by_id["AACIU#001"] == "warrant_unit_right_action"
    assert result["summary"]["lane_counts"]["operating_terminal_event"] == 1
    assert (output_root / "backlog_profile.csv").exists()
    assert (output_root / "operating_terminal_event.csv").exists()


def _write_priority(path: Path) -> None:
    pl.DataFrame(
        {
            "priority_rank": [1, 2, 3],
            "symbol": ["AAA", "BBB", "AACIU"],
            "symbol_era_id": ["AAA#001", "BBB#001", "AACIU#001"],
            "source_classification": [
                "delisted_or_acquired_candidate",
                "intermittent_or_reused_candidate",
                "partial_window_candidate",
            ],
            "research_route": [
                "operating_company_sec_event",
                "operating_company_sec_event",
                "warrant_unit_right_security_action",
            ],
            "instrument_type": [
                "probable_operating_company",
                "probable_operating_company",
                "probable_unit",
            ],
            "trade_rows": [100_000, 99, 5_000],
            "last_day": ["20240102", "20170102", "20220102"],
        }
    ).write_parquet(path)
