from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from utils.build_sec_terminal_followup_evidence import (
    FollowupEvidenceConfig,
    build_sec_terminal_followup_evidence,
)


def test_followup_evidence_fetches_submissions_and_scores_document(
    tmp_path: Path, monkeypatch: Any
) -> None:
    input_path = tmp_path / "questionable.csv"
    output_dir = tmp_path / "out"
    _questionable_row().write_csv(input_path)

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
                        "primaryDocument": ["q.htm", "close.htm"],
                    }
                }
            }
        )

    def fake_fetch_text(url: str, config: Any) -> str:
        assert url.endswith("/000000000124000001/close.htm")
        return "On January 5, 2024, AAA Corp ticker AAA completed the merger."

    monkeypatch.setattr("utils.sec_terminal_followup_sources.requests.get", fake_get)
    monkeypatch.setattr("utils.build_sec_terminal_followup_evidence.fetch_text", fake_fetch_text)

    result = build_sec_terminal_followup_evidence(
        FollowupEvidenceConfig(
            input_path=input_path,
            output_dir=output_dir,
            user_agent="IEXScoper test admin@example.test",
            timeout_seconds=2,
            sleep_seconds=0,
            max_rows=None,
            days_before=30,
            days_after=60,
            max_docs_per_row=4,
            forms=("8-K", "25-NSE"),
        )
    )

    reviewed = pl.read_csv(output_dir / "terminal_followup_evidence_review.csv")
    auto = pl.read_csv(output_dir / "terminal_followup_auto_verified.csv")
    assert result["summary"]["auto_ready_count"] == 1
    assert reviewed["followup_form"].to_list() == ["8-K"]
    assert reviewed["followup_terminal_text_bucket"].to_list() == [
        "terminal_text_verified_ready"
    ]
    assert auto["symbol"].to_list() == ["AAA"]


class FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self.payload


def _questionable_row() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["AAA"],
            "symbol_era_id": ["AAA#001"],
            "entity": ["AAA CORP (AAA) (CIK 0000000001)"],
            "original_last_day": ["20240105"],
            "last_day": ["20240105"],
            "research_status": ["candidate_needs_review"],
            "proposed_historical_issuer_name": ["AAA Corp"],
            "primary_source_url": ["https://www.sec.gov/Archives/edgar/data/1/old/"],
            "verifier_document_url": ["https://www.sec.gov/Archives/edgar/data/1/old/doc.htm"],
            "terminal_text_bucket": ["missing_terminal_date_review"],
            "terminal_text_flags": ["issuer_name_match|terminal_language|completion_language"],
            "terminal_text_snippet": ["closing and effective time"],
            "research_note": ["note"],
        }
    )
