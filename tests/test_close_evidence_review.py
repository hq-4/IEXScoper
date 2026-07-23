from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_close_evidence_review import (
    CloseEvidenceReviewConfig,
    build_close_evidence_review,
)


def test_build_close_evidence_review_filters_to_near_close_evidence(tmp_path: Path) -> None:
    input_path = tmp_path / "strong.csv"
    output_path = tmp_path / "review.csv"
    auto_path = tmp_path / "auto.csv"
    summary_path = tmp_path / "summary.json"
    _write_strong_review(input_path)

    result = build_close_evidence_review(
        CloseEvidenceReviewConfig(
            input_path=input_path,
            output_path=output_path,
            auto_verified_path=auto_path,
            summary_path=summary_path,
            max_abs_days=5,
            forms=("8-K", "425"),
        )
    )

    reviewed = {
        row["symbol"]: row for row in pl.read_csv(output_path, infer_schema_length=0).to_dicts()
    }
    auto = pl.read_csv(auto_path, infer_schema_length=0)
    assert result["summary"]["auto_verified_count"] == 2
    assert reviewed["AAA"]["close_evidence_bucket"] == "close_evidence_ready"
    assert reviewed["BBB"]["close_evidence_bucket"] == "close_evidence_ready"
    assert reviewed["FAR"]["close_evidence_bucket"] == "outside_close_window"
    assert reviewed["PROXY"]["close_evidence_bucket"] == "unsupported_close_form"
    assert reviewed["MISS"]["close_evidence_bucket"] == "missing_completion_or_delisting_evidence"
    assert reviewed["HOME"]["close_evidence_bucket"] == "reject_symbol_collision"
    assert set(auto["symbol"].to_list()) == {"AAA", "BBB"}
    assert set(auto["research_status"].to_list()) == {"verified"}


def _write_strong_review(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB", "FAR", "PROXY", "MISS", "HOME"],
            "symbol_era_id": [
                "AAA#001",
                "BBB#001",
                "FAR#001",
                "PROXY#001",
                "MISS#001",
                "HOME#001",
            ],
            "research_status": ["candidate_needs_review"] * 6,
            "verifier_bucket": [
                "strong_review_candidate",
                "strong_review_candidate",
                "strong_review_candidate",
                "strong_review_candidate",
                "strong_review_candidate",
                "strong_review_candidate",
            ],
            "verifier_flags": [
                "issuer_name_match|symbol_match|event_language|completion_language",
                "issuer_name_match|symbol_match|event_language|delisting_language",
                "issuer_name_match|symbol_match|event_language|completion_language",
                "issuer_name_match|symbol_match|event_language|completion_language",
                "issuer_name_match|symbol_match|event_language",
                "issuer_name_match|symbol_match|event_language|completion_language",
            ],
            "form": ["8-K", "425", "8-K", "DEFM14A", "8-K", "8-K"],
            "filed_at": [
                "2024-01-02",
                "2024-01-10",
                "2023-12-01",
                "2024-01-03",
                "2024-01-03",
                "2024-01-03",
            ],
            "original_last_day": [
                "20240105",
                "20240112",
                "20240105",
                "20240105",
                "20240105",
                "20240105",
            ],
            "last_day": [
                "20240105",
                "20240112",
                "20240105",
                "20240105",
                "20240105",
                "20240105",
            ],
            "entity": [
                "AAA CORP (AAA) (CIK 1)",
                "BBB CORP (BBB) (CIK 2)",
                "FAR CORP (FAR) (CIK 3)",
                "PROXY CORP (PROXY) (CIK 4)",
                "MISS CORP (MISS) (CIK 5)",
                "HOME BANCSHARES INC (HOMB) (CIK 6)",
            ],
            "proposed_historical_identity_status": ["manual_verified_historical_identity"] * 6,
            "proposed_historical_issuer_name": ["AAA", "BBB", "FAR", "PROXY", "MISS", "HOME"],
            "proposed_historical_event_type": ["merger_or_acquisition_lead"] * 6,
            "proposed_historical_event_date": ["2024-01-02"] * 6,
            "proposed_historical_successor": [None] * 6,
            "primary_source_url": ["https://www.sec.gov/archive/"] * 6,
            "secondary_source_url": [None] * 6,
            "research_note": ["note"] * 6,
            "verifier_document_url": [
                f"https://www.sec.gov/{symbol}.htm"
                for symbol in ["a", "b", "f", "p", "m", "h"]
            ],
        }
    ).write_csv(path)
