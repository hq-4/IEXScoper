from __future__ import annotations

from typing import Any

from utils.sec_terminal_followup_sources import SecRequestConfig, SecSubmissionsClient


def test_historical_shards_are_date_selected_and_accessions_deduplicated(monkeypatch: Any) -> None:
    calls: list[str] = []

    def fake_get(url: str, **_: Any) -> FakeResponse:
        calls.append(url)
        if url.endswith("CIK0000000001.json"):
            return FakeResponse(
                {
                    "filings": {
                        "recent": recent(["2024-01-05"], ["0000000001-24-000001"]),
                        "files": [
                            {
                                "name": "CIK0000000001-submissions-001.json",
                                "filingFrom": "2018-01-01",
                                "filingTo": "2020-12-31",
                            },
                            {
                                "name": "CIK0000000001-submissions-002.json",
                                "filingFrom": "2010-01-01",
                                "filingTo": "2012-12-31",
                            },
                        ],
                    }
                }
            )
        return FakeResponse(recent(["2020-06-01"], ["0000000001-24-000001"]))

    monkeypatch.setattr("utils.sec_terminal_followup_sources.requests.get", fake_get)
    client = SecSubmissionsClient(SecRequestConfig("test test@example.test", 2, 0))

    filings = client.filings("1", lower="2020-01-01", upper="2020-12-31")
    again = client.filings("1", lower="2020-01-01", upper="2020-12-31")

    assert len(filings) == 1
    assert filings[0]["accession_no"] == "0000000001-24-000001"
    assert calls == [
        "https://data.sec.gov/submissions/CIK0000000001.json",
        "https://data.sec.gov/submissions/CIK0000000001-submissions-001.json",
    ]
    assert again == filings


def recent(dates: list[str], accessions: list[str]) -> dict[str, list[str]]:
    return {
        "form": ["8-K"] * len(dates),
        "filingDate": dates,
        "accessionNumber": accessions,
        "primaryDocument": ["event.htm"] * len(dates),
    }


class FakeResponse:
    status_code = 200

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self.payload
