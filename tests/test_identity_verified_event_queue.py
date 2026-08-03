from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from utils.build_identity_verified_event_queue import (
    EventQueueConfig,
    build_event_queue,
)


def test_build_event_queue_filters_joins_and_ranks_by_volume(tmp_path: Path) -> None:
    event_gap_path = tmp_path / "event_gap.csv"
    identity_facts_path = tmp_path / "identity_facts.jsonl"
    review_path = tmp_path / "review.csv"
    output_path = tmp_path / "event_queue.csv"
    summary_path = tmp_path / "summary.json"
    _write_event_gap(event_gap_path)
    _write_identity_facts(identity_facts_path)
    _write_review(review_path)

    result = build_event_queue(
        _config(event_gap_path, identity_facts_path, review_path, output_path, summary_path)
    )

    rows = pl.read_csv(output_path, infer_schema_length=0).to_dicts()
    assert [row["symbol"] for row in rows] == ["BIG", "CAND"]
    assert rows[0]["priority_rank"] == "1"
    assert rows[0]["first_day"] == "20170103"
    assert rows[0]["last_day"] == "20240105"
    assert rows[0]["verified_identity_issuer"] == "BIG Corp"
    assert result["summary"]["row_count"] == 2
    assert result["summary"]["trade_rows"] == 1_000_100
    assert result["summary"]["event_status_counts"] == {
        "event_candidate": 1,
        "unresolved": 1,
    }
    assert (tmp_path / "event_queue_top.csv").exists()


def test_build_event_queue_rejects_symbol_mismatch(tmp_path: Path) -> None:
    event_gap_path = tmp_path / "event_gap.csv"
    identity_facts_path = tmp_path / "identity_facts.jsonl"
    review_path = tmp_path / "review.csv"
    _write_event_gap(event_gap_path)
    _write_identity_facts(identity_facts_path)
    review = _review_rows().with_columns(
        pl.when(pl.col("symbol_era_id") == "BIG#001")
        .then(pl.lit("WRONG"))
        .otherwise(pl.col("symbol"))
        .alias("symbol")
    )
    review.write_csv(review_path)

    with pytest.raises(ValueError, match="symbol mismatch"):
        build_event_queue(
            _config(
                event_gap_path,
                identity_facts_path,
                review_path,
                tmp_path / "out.csv",
                tmp_path / "summary.json",
                top_n=10,
            )
        )


def _config(
    event_gap_path: Path,
    identity_facts_path: Path,
    review_path: Path,
    output_path: Path,
    summary_path: Path,
    top_n: int = 1,
) -> EventQueueConfig:
    return EventQueueConfig(
        event_gap_path=event_gap_path,
        identity_facts_path=identity_facts_path,
        review_path=review_path,
        output_path=output_path,
        summary_path=summary_path,
        top_n=top_n,
    )


def _write_event_gap(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["BIG", "DONE", "NOID", "CLOSED", "CAND"],
            "symbol_era_id": [
                "BIG#001",
                "DONE#001",
                "NOID#001",
                "CLOSED#001",
                "CAND#001",
            ],
            "trade_rows": ["1000000", "500", "400", "300", "100"],
            "identity_status": ["verified", "verified", "unresolved", "verified", "verified"],
            "event_status": [
                "unresolved",
                "verified",
                "unresolved",
                "unresolved",
                "event_candidate",
            ],
            "research_status": [
                "action_required",
                "action_required",
                "action_required",
                "research_closed",
                "action_required",
            ],
            "next_resolver": ["known_identity_event_salvage_v2"] * 5,
        }
    ).write_csv(path)


def _write_identity_facts(path: Path) -> None:
    rows = [
        {
            "symbol": symbol,
            "symbol_era_id": f"{symbol}#001",
            "entity_id": str(index),
            "issuer": f"{symbol} Corp",
            "source": f"https://www.sec.gov/{symbol}",
            "evidence_method": "test",
            "fact_id": f"identity:{symbol}",
            "created_at": "2026-01-01T00:00:00+00:00",
            "verification_state": "verified",
        }
        for index, symbol in enumerate(["BIG", "DONE", "CLOSED", "CAND"], start=1)
    ]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def _write_review(path: Path) -> None:
    _review_rows().write_csv(path)


def _review_rows() -> pl.DataFrame:
    symbols = ["BIG", "DONE", "NOID", "CLOSED", "CAND"]
    return pl.DataFrame(
        {
            "symbol": symbols,
            "symbol_era_id": [f"{symbol}#001" for symbol in symbols],
            "first_day": ["20170103"] * len(symbols),
            "last_day": ["20240105"] * len(symbols),
            "trade_rows": [1_000_000, 500, 400, 300, 100],
            "historical_issuer_name": [f"{symbol} Corp" for symbol in symbols],
        }
    )
