from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_strict_terminal_review import (
    StrictTerminalReviewConfig,
    build_strict_terminal_review,
)


def test_build_strict_terminal_review_separates_verified_and_collisions(
    tmp_path: Path,
) -> None:
    verifier_path = tmp_path / "verifier.csv"
    output_path = tmp_path / "review.csv"
    auto_path = tmp_path / "auto.csv"
    summary_path = tmp_path / "summary.json"
    _write_verifier(verifier_path)

    result = build_strict_terminal_review(
        StrictTerminalReviewConfig(
            verifier_path=verifier_path,
            output_path=output_path,
            auto_verified_path=auto_path,
            summary_path=summary_path,
            max_close_days_from_last=14,
        )
    )

    reviewed = {
        row["symbol"]: row for row in pl.read_csv(output_path, infer_schema_length=0).to_dicts()
    }
    auto = pl.read_csv(auto_path, infer_schema_length=0)
    assert result["summary"]["auto_verified_count"] == 2
    assert reviewed["AAA"]["strict_terminal_bucket"] == "strict_verified_ready"
    assert reviewed["BBB"]["strict_terminal_bucket"] == "strict_verified_ready"
    assert reviewed["FAR"]["strict_terminal_bucket"] == "strong_needs_close_or_completion_review"
    assert reviewed["HOME"]["strict_terminal_bucket"] == "reject_symbol_collision"
    assert reviewed["MULTI"]["strict_terminal_bucket"] == "reject_symbol_collision"
    assert reviewed["WORD"]["strict_terminal_bucket"] == "strong_needs_close_or_completion_review"
    assert reviewed["OLD"]["strict_terminal_bucket"] == "strong_needs_close_or_completion_review"
    assert reviewed["WEAK"]["strict_terminal_bucket"] == "reject_weak_terminal_evidence"
    assert set(auto["symbol"].to_list()) == {"AAA", "BBB"}
    assert set(auto["research_status"].to_list()) == {"verified"}


def _write_verifier(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB", "FAR", "HOME", "MULTI", "WORD", "OLD", "WEAK"],
            "symbol_era_id": [
                "AAA#001",
                "BBB#001",
                "FAR#001",
                "HOME#001",
                "MULTI#001",
                "WORD#001",
                "OLD#001",
                "WEAK#001",
            ],
            "research_status": ["candidate_needs_review"] * 8,
            "triage_bucket": [
                "high_confidence_lead",
                "medium_confidence_lead",
                "medium_confidence_lead",
                "high_confidence_lead",
                "high_confidence_lead",
                "high_confidence_lead",
                "high_confidence_lead",
                "medium_confidence_lead",
            ],
            "verifier_bucket": [
                "strong_review_candidate",
                "strong_review_candidate",
                "strong_review_candidate",
                "strong_review_candidate",
                "strong_review_candidate",
                "strong_review_candidate",
                "strong_review_candidate",
                "weak_review_candidate",
            ],
            "verifier_score": ["90", "80", "80", "90", "90", "90", "90", "30"],
            "verifier_flags": [
                "issuer_name_match|symbol_match|event_language|completion_language",
                "issuer_name_match|symbol_match|event_language|delisting_language",
                "issuer_name_match|symbol_match|event_language|delisting_language",
                "issuer_name_match|symbol_match|event_language|completion_language",
                "issuer_name_match|symbol_match|event_language|completion_language",
                "issuer_name_match|symbol_match|event_language|completion_language",
                "issuer_name_match|symbol_match|event_language|completion_language",
                "issuer_name_match|symbol_match",
            ],
            "form": ["8-K", "25-NSE", "25-NSE", "425", "425", "425", "425", "DEF 14A"],
            "filed_at": [
                "2024-01-02",
                "2024-01-10",
                "2023-10-01",
                "2024-01-02",
                "2024-01-02",
                "2024-01-02",
                "2023-01-01",
                "2024-01-02",
            ],
            "original_last_day": [
                "20240105",
                "20240112",
                "20240112",
                "20240105",
                "20240105",
                "20240105",
                "20240105",
                "20240105",
            ],
            "last_day": [
                "20240105",
                "20240112",
                "20240112",
                "20240105",
                "20240105",
                "20240105",
                "20240105",
                "20240105",
            ],
            "entity": [
                "AAA CORP (AAA) (CIK 1)",
                "BBB CORP (BBB) (CIK 2)",
                "FAR CORP (FAR) (CIK 6)",
                "HOME BANCSHARES INC (HOMB) (CIK 3)",
                "OTHER CORP (OTH, OTH-P) (CIK 7)",
                "WORD HOLDINGS INC (CIK 8)",
                "OLD CORP (OLD) (CIK 4)",
                "WEAK CORP (WEAK) (CIK 5)",
            ],
            "proposed_historical_identity_status": ["manual_verified_historical_identity"] * 8,
            "proposed_historical_issuer_name": [
                "AAA",
                "BBB",
                "FAR",
                "HOME",
                "MULTI",
                "WORD",
                "OLD",
                "WEAK",
            ],
            "proposed_historical_event_type": ["merger_or_acquisition_lead"] * 8,
            "proposed_historical_event_date": ["2024-01-02"] * 8,
            "proposed_historical_successor": [None] * 8,
            "primary_source_url": ["https://www.sec.gov/archive/"] * 8,
            "secondary_source_url": [None] * 8,
            "research_note": ["note"] * 8,
            "verifier_document_url": [
                f"https://www.sec.gov/{symbol}.htm"
                for symbol in ["a", "b", "f", "h", "m", "word", "o", "w"]
            ],
        }
    ).write_csv(path)
