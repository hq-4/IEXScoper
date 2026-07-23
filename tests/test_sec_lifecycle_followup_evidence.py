from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from utils.build_sec_lifecycle_followup_evidence import (
    LifecycleFollowupEvidenceConfig,
    build_sec_lifecycle_followup_evidence,
)


def test_lifecycle_followup_recovers_missing_date_evidence(
    tmp_path: Path, monkeypatch: Any
) -> None:
    input_path = tmp_path / "lifecycle_review.csv"
    output_dir = tmp_path / "out"
    _lifecycle_review_row().write_csv(input_path)

    def fake_get(url: str, *, headers: dict[str, str], timeout: float) -> FakeResponse:
        assert headers["User-Agent"] == "IEXScoper test admin@example.test"
        assert url.endswith("/CIK0000000001.json")
        return FakeResponse(
            {
                "filings": {
                    "recent": {
                        "form": ["10-Q", "8-K"],
                        "filingDate": ["2024-04-01", "2024-01-06"],
                        "accessionNumber": ["0000000001-24-000010", "0000000001-24-000001"],
                        "primaryDocument": ["q.htm", "start.htm"],
                    }
                }
            }
        )

    def fake_fetch_text(url: str, config: Any) -> str:
        assert url.endswith("/000000000124000001/start.htm")
        return "On January 5, 2024, AAA Corp common stock began trading under the symbol AAA."

    monkeypatch.setattr("utils.sec_terminal_followup_sources.requests.get", fake_get)
    monkeypatch.setattr("utils.build_sec_lifecycle_followup_evidence.fetch_text", fake_fetch_text)

    result = build_sec_lifecycle_followup_evidence(
        LifecycleFollowupEvidenceConfig(
            input_path=input_path,
            output_dir=output_dir,
            user_agent="IEXScoper test admin@example.test",
            timeout_seconds=2,
            sleep_seconds=0,
            max_rows=None,
            days_before=30,
            days_after=60,
            max_docs_per_row=4,
            forms=("8-K",),
            input_buckets=("missing_lifecycle_date_review",),
        )
    )

    reviewed = pl.read_csv(output_dir / "lifecycle_followup_evidence_review.csv")
    auto = pl.read_csv(output_dir / "lifecycle_followup_auto_verified.csv")
    assert result["summary"]["auto_ready_count"] == 1
    assert reviewed["followup_form"].to_list() == ["8-K"]
    assert reviewed["followup_lifecycle_text_bucket"].to_list() == [
        "lifecycle_text_verified_ready"
    ]
    assert auto["symbol"].to_list() == ["AAA"]
    assert auto["research_status"].to_list() == ["verified"]


def test_lifecycle_followup_missing_cik_stays_review(tmp_path: Path) -> None:
    input_path = tmp_path / "lifecycle_review.csv"
    output_dir = tmp_path / "out"
    row = _lifecycle_review_row().with_columns(
        pl.lit("AAA CORP").alias("entity"),
        pl.lit("").alias("primary_source_url"),
        pl.lit("").alias("verifier_document_url"),
    )
    row.write_csv(input_path)

    result = build_sec_lifecycle_followup_evidence(
        LifecycleFollowupEvidenceConfig(
            input_path=input_path,
            output_dir=output_dir,
            user_agent="IEXScoper test admin@example.test",
            timeout_seconds=2,
            sleep_seconds=0,
            max_rows=None,
            days_before=30,
            days_after=60,
            max_docs_per_row=4,
            forms=("8-K",),
            input_buckets=("missing_lifecycle_date_review",),
        )
    )

    reviewed = pl.read_csv(output_dir / "lifecycle_followup_evidence_review.csv")
    assert result["summary"]["auto_ready_count"] == 0
    assert reviewed["followup_lifecycle_text_bucket"].to_list() == ["missing_cik"]


def test_lifecycle_followup_handles_mixed_empty_and_date_values(
    tmp_path: Path, monkeypatch: Any
) -> None:
    input_path = tmp_path / "lifecycle_review.csv"
    output_dir = tmp_path / "out"
    pl.concat([_lifecycle_review_row(), _lifecycle_review_row().with_columns(
        pl.lit("BBB").alias("symbol"),
        pl.lit("BBB#001").alias("symbol_era_id"),
        pl.lit("BBB CORP (BBB) (CIK 0000000002)").alias("entity"),
    )]).write_csv(input_path)

    def fake_get(url: str, *, headers: dict[str, str], timeout: float) -> FakeResponse:
        cik = url.rsplit("CIK", maxsplit=1)[-1].removesuffix(".json")
        accession = f"{int(cik):010d}-24-000001"
        return FakeResponse({"filings": {"recent": {
            "form": ["8-K"], "filingDate": ["2024-01-06"],
            "accessionNumber": [accession], "primaryDocument": ["doc.htm"],
        }}})

    def fake_fetch_text(url: str, config: Any) -> str:
        if "/2/" in url:
            return "BBB Corp common stock began trading under the symbol BBB."
        return "On January 5, 2024, AAA Corp common stock began trading under the symbol AAA."

    monkeypatch.setattr("utils.sec_terminal_followup_sources.requests.get", fake_get)
    monkeypatch.setattr("utils.build_sec_lifecycle_followup_evidence.fetch_text", fake_fetch_text)

    result = build_sec_lifecycle_followup_evidence(
        LifecycleFollowupEvidenceConfig(
            input_path=input_path,
            output_dir=output_dir,
            user_agent="IEXScoper test admin@example.test",
            timeout_seconds=2,
            sleep_seconds=0,
            max_rows=None,
            days_before=30,
            days_after=60,
            max_docs_per_row=4,
            forms=("8-K",),
            input_buckets=("missing_lifecycle_date_review",),
        )
    )

    reviewed = pl.read_csv(output_dir / "lifecycle_followup_evidence_review.csv")
    assert result["summary"]["document_row_count"] == 2
    assert reviewed.height == 2
    assert set(reviewed["followup_lifecycle_text_bucket"].to_list()) == {
        "lifecycle_text_verified_ready",
        "missing_lifecycle_date_review",
    }


class FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self.payload


def _lifecycle_review_row() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["AAA"],
            "symbol_era_id": ["AAA#001"],
            "lifecycle_anchor": ["first"],
            "original_first_day": ["20240105"],
            "original_last_day": ["20240607"],
            "entity": ["AAA CORP (AAA) (CIK 0000000001)"],
            "research_status": ["candidate_needs_review"],
            "proposed_historical_identity_status": ["manual_verified_historical_identity"],
            "proposed_historical_issuer_name": ["AAA Corp"],
            "proposed_historical_event_type": ["trading_commencement_or_symbol_lifecycle_lead"],
            "proposed_historical_event_date": ["2024-01-05"],
            "proposed_historical_successor": [""],
            "primary_source_url": ["https://www.sec.gov/Archives/edgar/data/1/old/"],
            "secondary_source_url": [""],
            "research_note": ["note"],
            "verifier_document_url": ["https://www.sec.gov/Archives/edgar/data/1/old/doc.htm"],
            "lifecycle_text_bucket": ["missing_lifecycle_date_review"],
            "lifecycle_text_flags": ["issuer_name_match|first_day_lifecycle_language"],
            "lifecycle_text_snippet": ["AAA Corp common stock began trading under the symbol AAA."],
        }
    )
