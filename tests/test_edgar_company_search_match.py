from __future__ import annotations

from pathlib import Path
from typing import Any

from utils.edgar_company_search_match import (
    STATUS_AMBIGUOUS,
    STATUS_FETCH_ERROR,
    STATUS_MATCHED,
    STATUS_NAME_MISMATCH,
    STATUS_NO_CANDIDATES,
    match_issuer_name,
)
from utils.resolution_v2_network import CachedPrimaryClient, NetworkConfig
from utils.resolution_v2_registry import EvidenceRegistry
from utils.sec_sic_client import fetch_sic

SEARCH_URL = "https://www.sec.gov/cgi-bin/browse-edgar"


class FakeResponse:
    def __init__(self, payload: Any = None, text: str = "") -> None:
        self._payload = payload
        self.text = text
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._payload


def _client(tmp_path: Path, *, retries: int = 3) -> CachedPrimaryClient:
    registry = EvidenceRegistry(tmp_path / "registry.sqlite")
    config = NetworkConfig(user_agent="test test@example.test", delay_seconds=0, retries=retries)
    return CachedPrimaryClient(config, registry)


def _fake_get(search_atom: str, submissions_payload: dict[str, Any] | None):
    def fake_get(url: str, **_: Any) -> FakeResponse:
        if url == SEARCH_URL:
            return FakeResponse(text=search_atom)
        return FakeResponse(payload=submissions_payload)

    return fake_get


def test_match_issuer_name_no_candidates(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", _fake_get("<feed></feed>", None)
    )

    result = match_issuer_name(_client(tmp_path), "Nonexistent Company Xyz")

    assert result["match_status"] == STATUS_NO_CANDIDATES
    assert result["matched_cik"] is None


def test_match_issuer_name_ambiguous_candidates(tmp_path: Path, monkeypatch: Any) -> None:
    atom = (
        "<feed>"
        "<entry><content type='text/xml'><company-info><cik>0000000001</cik></company-info></content></entry>"
        "<entry><content type='text/xml'><company-info><cik>0000000002</cik></company-info></content></entry>"
        "</feed>"
    )
    monkeypatch.setattr("utils.resolution_v2_network.requests.get", _fake_get(atom, None))

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co")

    assert result["match_status"] == STATUS_AMBIGUOUS
    assert result["candidate_count"] == 2
    assert result["matched_cik"] is None


def test_match_issuer_name_single_candidate_name_matches(tmp_path: Path, monkeypatch: Any) -> None:
    atom = "<feed><entry><content type='text/xml'><company-info><cik>0000104599</cik></company-info></content></entry></feed>"
    submissions = {
        "sic": "5731",
        "sicDescription": "Retail-Electronics",
        "name": "CIRCUIT CITY STORES INC",
    }
    monkeypatch.setattr("utils.resolution_v2_network.requests.get", _fake_get(atom, submissions))

    result = match_issuer_name(_client(tmp_path), "Circuit City Stores")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "104599"
    assert result["sic"] == "5731"
    assert result["candidate_name"] == "CIRCUIT CITY STORES INC"


def test_match_issuer_name_single_candidate_name_matches_after_descriptor_strip(
    tmp_path: Path, monkeypatch: Any
) -> None:
    atom = "<feed><entry><content type='text/xml'><company-info><cik>0000313216</cik></company-info></content></entry></feed>"
    submissions = {"sic": "3826", "sicDescription": "Instruments", "name": "ABB LTD"}
    monkeypatch.setattr("utils.resolution_v2_network.requests.get", _fake_get(atom, submissions))

    result = match_issuer_name(_client(tmp_path), "ABB LTD-SPON ADR")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "313216"


def test_match_issuer_name_single_candidate_name_mismatch(tmp_path: Path, monkeypatch: Any) -> None:
    """A single search candidate isn't automatically trusted — its actual registrant
    name still has to match, or this stays unresolved rather than guessed."""
    atom = "<feed><entry><content type='text/xml'><company-info><cik>0000999999</cik></company-info></content></entry></feed>"
    submissions = {
        "sic": "1234",
        "sicDescription": "Something Else",
        "name": "TOTALLY DIFFERENT CO",
    }
    monkeypatch.setattr("utils.resolution_v2_network.requests.get", _fake_get(atom, submissions))

    result = match_issuer_name(_client(tmp_path), "Circuit City Stores")

    assert result["match_status"] == STATUS_NAME_MISMATCH
    assert result["matched_cik"] is None
    assert result["candidate_name"] == "TOTALLY DIFFERENT CO"


def test_match_issuer_name_search_fetch_error_does_not_raise(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A transient SEC 503 on the search request must not propagate out of
    `match_issuer_name` — a multi-thousand-name batch run has to survive one bad name,
    not abort and lose every result already collected."""
    import requests

    class FailingResponse:
        status_code = 503

        def raise_for_status(self) -> None:
            raise requests.exceptions.HTTPError(response=self)

    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", lambda *_a, **_k: FailingResponse()
    )

    result = match_issuer_name(_client(tmp_path, retries=1), "Some Company Inc")

    assert result["match_status"] == STATUS_FETCH_ERROR
    assert result["matched_cik"] is None


def test_match_issuer_name_validation_fetch_error_does_not_raise(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A transient SEC 503 on the *validation* (submissions) request, after the search
    itself succeeded with a single candidate, must also surface as `fetch_error` rather
    than a false `name_mismatch`."""
    import requests

    atom = "<feed><entry><content type='text/xml'><company-info><cik>0000104599</cik></company-info></content></entry></feed>"

    class FailingResponse:
        status_code = 503

        def raise_for_status(self) -> None:
            raise requests.exceptions.HTTPError(response=self)

    def fake_get(url: str, **_: Any) -> Any:
        if url == SEARCH_URL:
            return FakeResponse(text=atom)
        return FailingResponse()

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)

    result = match_issuer_name(_client(tmp_path, retries=1), "Circuit City Stores")

    assert result["match_status"] == STATUS_FETCH_ERROR
    assert result["matched_cik"] is None


def test_match_issuer_name_reuses_cached_sic_fetch(tmp_path: Path, monkeypatch: Any) -> None:
    """If the CIK's submissions data was already fetched (e.g. by the main SIC pass),
    validating a name match here is a free cache hit, not a new request."""
    atom = "<feed><entry><content type='text/xml'><company-info><cik>0000104599</cik></company-info></content></entry></feed>"
    submissions = {"sic": "5731", "sicDescription": "Retail", "name": "CIRCUIT CITY STORES INC"}
    calls = []

    def fake_get(url: str, **_: Any) -> FakeResponse:
        calls.append(url)
        if url == SEARCH_URL:
            return FakeResponse(text=atom)
        return FakeResponse(payload=submissions)

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)
    client = _client(tmp_path)

    fetch_sic(client, "104599")  # pre-warm the cache, as the main SIC pass would
    calls.clear()

    result = match_issuer_name(client, "Circuit City Stores")

    assert result["match_status"] == STATUS_MATCHED
    assert calls == [SEARCH_URL]  # only the search call, submissions was a cache hit
