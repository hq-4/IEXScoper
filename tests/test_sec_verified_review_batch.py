from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_sec_verified_review_batch import ReviewBatchConfig, build_review_batch


def test_build_review_batch_defaults_to_strong_rows(tmp_path: Path) -> None:
    verifier_path = tmp_path / "verified.csv"
    output_path = tmp_path / "batch.csv"
    _verifier_rows().write_csv(verifier_path)

    result = build_review_batch(
        ReviewBatchConfig(
            verifier_path=verifier_path,
            output_path=output_path,
            min_bucket="strong_review_candidate",
            max_rows=None,
        )
    )

    rows = pl.read_csv(output_path, infer_schema_length=0).to_dicts()
    assert result["row_count"] == 2
    assert result["bucket_counts"] == {"strong_review_candidate": 2}
    assert [row["symbol"] for row in rows] == ["CCC", "AAA"]
    assert {row["research_status"] for row in rows} == {"candidate_needs_review"}


def test_build_review_batch_can_include_moderate_rows(tmp_path: Path) -> None:
    verifier_path = tmp_path / "verified.csv"
    output_path = tmp_path / "batch.csv"
    _verifier_rows().write_csv(verifier_path)

    build_review_batch(
        ReviewBatchConfig(
            verifier_path=verifier_path,
            output_path=output_path,
            min_bucket="moderate_review_candidate",
            max_rows=3,
        )
    )

    rows = pl.read_csv(output_path, infer_schema_length=0).to_dicts()
    assert [row["symbol"] for row in rows] == ["CCC", "AAA", "BBB"]


def _verifier_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "priority_rank": ["2", "3", "1", "4"],
            "symbol": ["AAA", "BBB", "CCC", "DDD"],
            "symbol_era_id": ["AAA#001", "BBB#001", "CCC#001", "DDD#001"],
            "research_status": ["candidate_needs_review"] * 4,
            "verifier_bucket": [
                "strong_review_candidate",
                "moderate_review_candidate",
                "strong_review_candidate",
                "weak_review_candidate",
            ],
            "verifier_score": ["100", "80", "115", "20"],
            "verifier_flags": ["issuer|event", "issuer|event", "issuer|event", "issuer"],
            "verifier_document_url": [
                "https://www.sec.gov/a.htm",
                "https://www.sec.gov/b.htm",
                "https://www.sec.gov/c.htm",
                "https://www.sec.gov/d.htm",
            ],
        }
    )
