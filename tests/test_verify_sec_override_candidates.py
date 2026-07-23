from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from utils.verify_sec_override_candidates import (
    bucket_for_score,
    evidence_flags,
    resolve_user_agent,
    verify_sec_override_candidates,
)
from utils.sec_candidate_verifier_schema import VerifyConfig


def test_verify_sec_override_candidates_scores_strong_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidates_path = tmp_path / "candidates.csv"
    output_path = tmp_path / "verified.csv"
    summary_path = tmp_path / "summary.json"
    _candidate_rows().write_csv(candidates_path)

    def fake_get(url: str, *, headers: dict[str, str], timeout: float) -> FakeResponse:
        assert headers["User-Agent"] == "IEXScoper test admin@example.test"
        assert timeout == 2
        if url.endswith("/index.json"):
            return FakeResponse(
                {
                    "directory": {
                        "item": [
                            {"name": "index.html"},
                            {"name": "0000001-24-000001-index-headers.html"},
                            {"name": "d123dex231.htm"},
                            {"name": "company-xex21d1.htm"},
                            {"name": "company_ex23-1.htm"},
                            {"name": "primary-document.htm"},
                        ]
                    }
                }
            )
        return FakeResponse(
            "<html>AAA Corp completed the merger and the transaction closed.</html>"
        )

    monkeypatch.setattr("utils.verify_sec_override_candidates.requests.get", fake_get)

    result = verify_sec_override_candidates(
        VerifyConfig(
            candidates_path=candidates_path,
            output_path=output_path,
            summary_path=summary_path,
            user_agent="IEXScoper test admin@example.test",
            timeout_seconds=2,
            sleep_seconds=0,
            max_rows=None,
        )
    )

    rows = pl.read_csv(output_path, infer_schema_length=0).to_dicts()
    assert result["summary"]["bucket_counts"] == {"strong_review_candidate": 1}
    assert rows[0]["research_status"] == "candidate_needs_review"
    assert rows[0]["verifier_bucket"] == "strong_review_candidate"
    assert "completion_language" in rows[0]["verifier_flags"]
    assert rows[0]["verifier_document_url"].endswith("/primary-document.htm")


def test_verify_sec_override_candidates_records_fetch_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidates_path = tmp_path / "candidates.csv"
    _candidate_rows().write_csv(candidates_path)

    def fake_get(url: str, *, headers: dict[str, str], timeout: float) -> FakeResponse:
        return FakeResponse({}, status_code=503)

    monkeypatch.setattr("utils.verify_sec_override_candidates.requests.get", fake_get)

    result = verify_sec_override_candidates(
        VerifyConfig(
            candidates_path=candidates_path,
            output_path=tmp_path / "verified.csv",
            summary_path=tmp_path / "summary.json",
            user_agent="IEXScoper test admin@example.test",
            timeout_seconds=2,
            sleep_seconds=0,
            max_rows=None,
        )
    )

    row = result["rows"][0]
    assert row["verifier_bucket"] == "fetch_error"
    assert "503 error" in row["verifier_error"]


def test_verify_sec_override_candidates_handles_late_fetch_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidates_path = tmp_path / "candidates.csv"
    _many_candidate_rows(105).write_csv(candidates_path)

    def fake_get(url: str, *, headers: dict[str, str], timeout: float) -> FakeResponse:
        if "/104/" in url:
            return FakeResponse({"directory": {"item": [{"name": "index.html"}]}})
        if url.endswith("/index.json"):
            return FakeResponse({"directory": {"item": [{"name": "primary-document.htm"}]}})
        return FakeResponse("<html>AAA Corp completed the merger and transaction closed.</html>")

    monkeypatch.setattr("utils.verify_sec_override_candidates.requests.get", fake_get)

    result = verify_sec_override_candidates(
        VerifyConfig(
            candidates_path=candidates_path,
            output_path=tmp_path / "verified.csv",
            summary_path=tmp_path / "summary.json",
            user_agent="IEXScoper test admin@example.test",
            timeout_seconds=2,
            sleep_seconds=0,
            max_rows=None,
        )
    )

    assert result["summary"]["row_count"] == 105
    assert result["summary"]["bucket_counts"]["fetch_error"] == 1
    assert result["rows"][-1]["verifier_bucket"] == "fetch_error"


def test_resolve_user_agent_requires_real_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    with pytest.raises(ValueError, match="SEC_USER_AGENT"):
        resolve_user_agent(None)
    assert resolve_user_agent("contact@example.test") == "contact@example.test"


def test_evidence_bucket_requires_identity_and_event() -> None:
    flags = evidence_flags(
        {
            "symbol": "AAA",
            "proposed_historical_issuer_name": "AAA Corp",
            "form": "425",
        },
        "AAA CORP COMPLETED THE MERGER",
    )
    assert bucket_for_score(90, flags) == "strong_review_candidate"
    assert bucket_for_score(60, ("event_language",)) == "moderate_review_candidate"


class FakeResponse:
    def __init__(self, payload: Any, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code
        self.text = payload if isinstance(payload, str) else ""

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"{self.status_code} error")

    def json(self) -> Any:
        return self.payload


def _candidate_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["AAA"],
            "symbol_era_id": ["AAA#001"],
            "research_status": ["candidate_needs_review"],
            "proposed_historical_issuer_name": ["AAA Corp"],
            "proposed_historical_event_type": ["merger_or_acquisition_lead"],
            "primary_source_url": ["https://www.sec.gov/Archives/edgar/data/1/0001/"],
            "form": ["425"],
        }
    )


def _many_candidate_rows(count: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["AAA"] * count,
            "symbol_era_id": [f"AAA#{index:03d}" for index in range(count)],
            "research_status": ["candidate_needs_review"] * count,
            "proposed_historical_issuer_name": ["AAA Corp"] * count,
            "proposed_historical_event_type": ["merger_or_acquisition_lead"] * count,
            "primary_source_url": [
                f"https://www.sec.gov/Archives/edgar/data/1/{index}/"
                for index in range(count)
            ],
            "form": ["425"] * count,
        }
    )
