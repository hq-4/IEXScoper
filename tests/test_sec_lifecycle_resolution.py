from __future__ import annotations

from pathlib import Path
from typing import Any
import json

import polars as pl

from utils.build_lifecycle_event_search_batch import (
    LifecycleEventSearchBatchConfig,
    build_lifecycle_event_search_batch,
)
from utils.build_lifecycle_override_candidates import (
    LifecycleCandidateConfig,
    build_lifecycle_override_candidates,
)
from utils.build_sec_lifecycle_text_evidence import (
    LifecycleTextEvidenceConfig,
    build_sec_lifecycle_text_evidence,
)
from utils.run_sec_lifecycle_resolution_iterations import prior_run_attempts


def test_lifecycle_batch_and_candidates_preserve_anchor_metadata(tmp_path: Path) -> None:
    input_path = tmp_path / "workplan.csv"
    window_path = tmp_path / "lifecycle_window.csv"
    summary_path = tmp_path / "summary.json"
    triage_path = tmp_path / "triage.parquet"
    candidates_path = tmp_path / "candidates.csv"
    _workplan().write_csv(input_path)

    build_lifecycle_event_search_batch(
        LifecycleEventSearchBatchConfig(
            input_path=input_path,
            output_path=window_path,
            summary_path=summary_path,
            limit=None,
            first_lookback_days=10,
            first_lookahead_days=20,
            last_lookback_days=30,
            last_lookahead_days=40,
        )
    )
    window = pl.read_csv(window_path, infer_schema_length=0)
    assert window["lifecycle_anchor"].to_list() == ["first", "last"]
    assert window["original_first_day"].to_list() == ["20240105", "20240105"]
    assert window["original_last_day"].to_list() == ["20240607", "20240607"]

    _triage(window).write_parquet(triage_path)
    result = build_lifecycle_override_candidates(
        LifecycleCandidateConfig(
            template_path=window_path,
            triage_path=triage_path,
            output_path=candidates_path,
            min_bucket="manual_review_lead",
        )
    )
    candidates = pl.read_csv(candidates_path, infer_schema_length=0)
    assert result["candidate_count"] == 2
    assert set(candidates["lifecycle_anchor"].to_list()) == {"first", "last"}
    assert "proposed_historical_issuer_name" in candidates.columns
    assert candidates["primary_source_url"].str.contains("/Archives/edgar/data/1/").all()


def test_lifecycle_text_evidence_conservative_buckets(
    tmp_path: Path, monkeypatch: Any
) -> None:
    verifier_path = tmp_path / "verifier.csv"
    output_dir = tmp_path / "out"
    _verifier_rows().write_csv(verifier_path)

    def fake_fetch_text(url: str, config: Any) -> str:
        return _texts()[url.rsplit("/", maxsplit=1)[-1]]

    monkeypatch.setattr("utils.build_sec_lifecycle_text_evidence.fetch_text", fake_fetch_text)
    result = build_sec_lifecycle_text_evidence(
        LifecycleTextEvidenceConfig(
            verifier_path=verifier_path,
            output_dir=output_dir,
            user_agent="IEXScoper test admin@example.test",
            timeout_seconds=2,
            sleep_seconds=0,
            max_rows=None,
            input_buckets=("strong_review_candidate", "weak_review_candidate"),
        )
    )

    reviewed = {
        row["symbol"]: row
        for row in pl.read_csv(output_dir / "lifecycle_text_evidence_review.csv").to_dicts()
    }
    auto = pl.read_csv(output_dir / "lifecycle_text_auto_verified.csv")
    assert result["summary"]["auto_ready_count"] == 3
    assert reviewed["NEW"]["lifecycle_text_bucket"] == "lifecycle_text_verified_ready"
    assert reviewed["OLD"]["lifecycle_text_bucket"] == "lifecycle_text_verified_ready"
    assert reviewed["OUTSIDE"]["lifecycle_text_bucket"] == "lifecycle_text_verified_ready"
    assert reviewed["FAR"]["lifecycle_text_bucket"] == "date_mismatch_review"
    assert reviewed["MISS"]["lifecycle_text_bucket"] == "missing_lifecycle_date_review"
    assert reviewed["COLL"]["lifecycle_text_bucket"] == "reject_symbol_collision"
    assert set(auto["symbol"].to_list()) == {"NEW", "OLD", "OUTSIDE"}
    assert set(auto["research_status"].to_list()) == {"verified"}


def test_lifecycle_prior_run_attempts_reads_sibling_summaries(tmp_path: Path) -> None:
    root = tmp_path / "run-002"
    prior = tmp_path / "run-001"
    prior.mkdir()
    input_path = prior / "iter_001_input.csv"
    pl.DataFrame({"symbol_era_id": ["AAA#001", "BBB#001"]}).write_csv(input_path)
    (prior / "sec_lifecycle_iterations_summary.json").write_text(
        json.dumps({"iterations": [{"input_path": str(input_path)}]}) + "\n",
        encoding="utf-8",
    )

    assert prior_run_attempts(root) == {"AAA#001", "BBB#001"}


def _workplan() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "priority_rank": ["1"],
            "symbol": ["NEW"],
            "symbol_era_id": ["NEW#001"],
            "first_day": ["20240105"],
            "last_day": ["20240607"],
            "trade_rows": ["1000"],
        }
    )


def _triage(window: pl.DataFrame) -> pl.DataFrame:
    return window.select(["priority_rank", "symbol", "symbol_era_id", "first_day", "last_day"]).with_columns(
        pl.lit("merger").alias("query"),
        pl.lit(1).alias("triage_rank"),
        pl.lit("manual_review_lead").alias("triage_bucket"),
        pl.lit(15).alias("triage_score"),
        pl.lit("weak_form; inside_symbol_era; no_entity_match").alias("triage_reason"),
        pl.lit("1").alias("cik"),
        pl.lit("New Corp (NEW)").alias("entity"),
        pl.lit("8-K").alias("form"),
        pl.lit("2024-01-05").alias("filed_at"),
        pl.lit("0000000001-24-000001").alias("accession_no"),
    )


def _verifier_rows() -> pl.DataFrame:
    rows = [
        ("NEW", "first", "20240105", "20240607", "new.htm"),
        ("OLD", "last", "20240105", "20240607", "old.htm"),
        ("OUTSIDE", "last", "20240105", "20240607", "outside.htm"),
        ("FAR", "last", "20240105", "20240607", "far.htm"),
        ("MISS", "first", "20240105", "20240607", "miss.htm"),
        ("COLL", "last", "20240105", "20240607", "coll.htm"),
    ]
    return pl.DataFrame(
        {
            "symbol": [row[0] for row in rows],
            "symbol_era_id": [f"{row[0]}#001" for row in rows],
            "lifecycle_anchor": [row[1] for row in rows],
            "original_first_day": [row[2] for row in rows],
            "original_last_day": [row[3] for row in rows],
            "research_status": ["candidate_needs_review"] * len(rows),
            "verifier_bucket": ["strong_review_candidate"] * len(rows),
            "entity": [f"{row[0]} Corp ({row[0]}) (CIK 1)" for row in rows],
            "proposed_historical_issuer_name": [f"{row[0]} Corp" for row in rows],
            "proposed_historical_identity_status": ["manual_verified_historical_identity"] * len(rows),
            "proposed_historical_event_type": ["operating_lifecycle_terminal_lead"] * len(rows),
            "proposed_historical_event_date": ["2024-06-07"] * len(rows),
            "proposed_historical_successor": [""] * len(rows),
            "primary_source_url": ["https://www.sec.gov/Archives/edgar/data/1/000/"] * len(rows),
            "secondary_source_url": [""] * len(rows),
            "research_note": ["note"] * len(rows),
            "verifier_document_url": [f"https://www.sec.gov/archive/{row[4]}" for row in rows],
        }
    )


def _texts() -> dict[str, str]:
    return {
        "new.htm": "On January 5, 2024, New Corp common stock began trading under the symbol NEW.",
        "old.htm": "On June 7, 2024, Old Corp ticker OLD completed the merger and shares ceased trading.",
        "outside.htm": (
            "A separate party (NOISE) appears in an unrelated paragraph. " + ("filler " * 80) +
            "On June 7, 2024, Outside Corp ticker OUTSIDE completed the merger."
        ),
        "far.htm": "On January 1, 2024, Far Corp ticker FAR completed the merger.",
        "miss.htm": "Miss Corp common stock began trading under the symbol MISS.",
        "coll.htm": "On June 7, 2024, Collision Corp (OTHER) completed the merger.",
    }
