from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_dead_ticker_priority_queue import (
    PriorityQueueConfig,
    build_priority_queue,
)


def test_build_priority_queue_ranks_unresolved_operating_delisted_rows(
    tmp_path: Path,
) -> None:
    review_path = tmp_path / "review.parquet"
    output_root = tmp_path / "priority"
    _write_review_queue(review_path)

    result = build_priority_queue(
        PriorityQueueConfig(review_queue_path=review_path, output_root=output_root, top_n=2)
    )

    rows = pl.read_parquet(output_root / "unresolved_priority_queue.parquet").to_dicts()
    assert [row["symbol"] for row in rows] == ["AAA", "BBB", "ZZZ"]
    assert rows[0]["priority_rank"] == 1
    assert rows[0]["is_probable_operating"] is True
    assert rows[0]["is_delisted_candidate"] is True
    assert result["summary"]["unresolved_era_count"] == 3
    assert result["summary"]["top_n"] == 2
    assert (output_root / "unresolved_priority_report.md").exists()
    assert (output_root / "unresolved_priority_top.csv").exists()


def _write_review_queue(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["ZZZ", "BBB", "AAA", "DONE"],
            "symbol_era_id": ["ZZZ#001", "BBB#001", "AAA#001", "DONE#001"],
            "source_classification": [
                "delisted_or_acquired_candidate",
                "intermittent_or_reused_candidate",
                "delisted_or_acquired_candidate",
                "delisted_or_acquired_candidate",
            ],
            "first_day": ["20170103", "20180103", "20190103", "20200103"],
            "last_day": ["20171229", "20181231", "20191231", "20201231"],
            "observed_days": [10, 20, 30, 40],
            "trade_rows": [100_000, 200, 100, 1_000_000],
            "main_rows": [110_000, 300, 200, 1_100_000],
            "identity_evidence_status": [
                "historical_identity_unresolved",
                "historical_identity_unresolved",
                "historical_identity_unresolved",
                "manual_verified_historical_identity",
            ],
            "instrument_hint": [
                "probable_fund_or_trust",
                "probable_operating_or_other",
                "probable_operating_or_other",
                "probable_operating_or_other",
            ],
        }
    ).write_parquet(path)
