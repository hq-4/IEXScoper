from __future__ import annotations

from pathlib import Path
from typing import Any

from utils.resolution_v2_network import CachedPrimaryClient, NetworkConfig
from utils.resolution_v2_registry import EvidenceRegistry
from utils.sec_company_search_client import lookup_cik_by_ticker, search_company_ciks

# Real shape of a browse-edgar atom response, including SEC's own `title="ARRAY(0x...)"`
# rendering bug — the parser must never depend on `title` for the company name.
SINGLE_MATCH_ATOM = """<?xml version="1.0" encoding="ISO-8859-1" ?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry title="ARRAY(0x55d0ddbc4998)">
    <content type="text/xml">
      <company-info name="ARRAY(0x55d0ddc5a170)">
        <cik>0000104599</cik>
        <sic>5731</sic>
      </company-info>
    </content>
    <id>urn:tag:www.sec.gov:cik=0000104599</id>
  </entry>
</feed>"""

MULTI_MATCH_ATOM = """<?xml version="1.0" encoding="ISO-8859-1" ?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry><content type="text/xml"><company-info><cik>0000104599</cik></company-info></content></entry>
  <entry><content type="text/xml"><company-info><cik>0000928597</cik></company-info></content></entry>
  <entry><content type="text/xml"><company-info><cik>0001993159</cik></company-info></content></entry>
</feed>"""

NO_MATCH_ATOM = """<?xml version="1.0" encoding="ISO-8859-1" ?>
<feed xmlns="http://www.w3.org/2005/Atom">
</feed>"""

# A filing-history view (same CIK repeated across many filing entries) — this happens
# when the query matches exactly one company and browse-edgar shows its filings
# instead of a company list. Distinct-CIK dedup must collapse this to one candidate.
FILING_HISTORY_ATOM = """<?xml version="1.0" encoding="ISO-8859-1" ?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry><id>urn:tag:www.sec.gov:cik=0000104599</id><cik>0000104599</cik></entry>
  <entry><id>urn:tag:www.sec.gov:cik=0000104599</id><cik>0000104599</cik></entry>
  <entry><id>urn:tag:www.sec.gov:cik=0000104599</id><cik>0000104599</cik></entry>
</feed>"""


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None


def _client(tmp_path: Path) -> CachedPrimaryClient:
    registry = EvidenceRegistry(tmp_path / "registry.sqlite")
    config = NetworkConfig(user_agent="test test@example.test", delay_seconds=0)
    return CachedPrimaryClient(config, registry)


def test_search_company_ciks_single_exact_match(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda *a, **k: FakeResponse(SINGLE_MATCH_ATOM),
    )

    result = search_company_ciks(_client(tmp_path), "Circuit City Stores")

    assert result == ["104599"]


def test_search_company_ciks_filing_history_collapses_to_one_candidate(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda *a, **k: FakeResponse(FILING_HISTORY_ATOM),
    )

    result = search_company_ciks(_client(tmp_path), "Circuit City Stores")

    assert result == ["104599"]


def test_search_company_ciks_multiple_matches_returns_all_distinct(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", lambda *a, **k: FakeResponse(MULTI_MATCH_ATOM)
    )

    result = search_company_ciks(_client(tmp_path), "Circuit City")

    assert set(result) == {"104599", "928597", "1993159"}
    assert len(result) == 3


def test_search_company_ciks_no_match_returns_empty(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", lambda *a, **k: FakeResponse(NO_MATCH_ATOM)
    )

    assert search_company_ciks(_client(tmp_path), "Nonexistent Company Xyz") == []


def test_search_company_ciks_blank_name_skips_network(tmp_path: Path, monkeypatch: Any) -> None:
    calls = []
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda *a, **k: calls.append(1) or FakeResponse(NO_MATCH_ATOM),
    )

    assert search_company_ciks(_client(tmp_path), "") == []
    assert search_company_ciks(_client(tmp_path), "   ") == []
    assert calls == []


def test_search_company_ciks_second_call_is_cache_hit(tmp_path: Path, monkeypatch: Any) -> None:
    calls = []

    def fake_get(url: str, **_: Any) -> FakeResponse:
        calls.append(url)
        return FakeResponse(SINGLE_MATCH_ATOM)

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)
    client = _client(tmp_path)

    search_company_ciks(client, "Circuit City Stores")
    search_company_ciks(client, "Circuit City Stores")

    assert len(calls) == 1


def test_lookup_cik_by_ticker_returns_single_cik(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda *a, **k: FakeResponse(SINGLE_MATCH_ATOM),
    )

    assert lookup_cik_by_ticker(_client(tmp_path), "CC") == "104599"


def test_lookup_cik_by_ticker_no_registration_returns_none(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", lambda *a, **k: FakeResponse(NO_MATCH_ATOM)
    )

    assert lookup_cik_by_ticker(_client(tmp_path), "ZZZZZ") is None


def test_lookup_cik_by_ticker_blank_ticker_skips_network(tmp_path: Path, monkeypatch: Any) -> None:
    calls = []
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda *a, **k: calls.append(1) or FakeResponse(NO_MATCH_ATOM),
    )

    assert lookup_cik_by_ticker(_client(tmp_path), "") is None
    assert lookup_cik_by_ticker(_client(tmp_path), "   ") is None
    assert calls == []


def test_lookup_cik_by_ticker_uses_cik_param_not_company(tmp_path: Path, monkeypatch: Any) -> None:
    """Distinguishes a ticker-registry lookup from a name search at the request level —
    the two must never collide in the cache."""
    seen_params = []

    def fake_get(url: str, **kwargs: Any) -> FakeResponse:
        seen_params.append(kwargs.get("params"))
        return FakeResponse(SINGLE_MATCH_ATOM)

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)

    lookup_cik_by_ticker(_client(tmp_path), "CC")

    assert seen_params[0]["CIK"] == "CC"
    assert "company" not in seen_params[0]
