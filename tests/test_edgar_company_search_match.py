from __future__ import annotations

from pathlib import Path
from typing import Any

from utils.edgar_company_search_match import (
    STATUS_AMBIGUOUS,
    STATUS_FETCH_ERROR,
    STATUS_MATCHED,
    STATUS_NO_CANDIDATES,
    STATUS_NO_VALIDATED_MATCH,
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


def _atom(ciks: list[str]) -> str:
    entries = "".join(
        f"<entry><content type='text/xml'><company-info><cik>{int(c):010d}</cik>"
        "</company-info></content></entry>"
        for c in ciks
    )
    return f"<feed>{entries}</feed>"


def _cik_from_submissions_url(url: str) -> str:
    digits = url.rsplit("CIK", 1)[1].split(".")[0]
    return str(int(digits))


def _fake_get(search_by_query: dict[str, str], submissions_by_cik: dict[str, dict[str, Any]]):
    """`search_by_query` maps the exact `company` search param to an atom string;
    missing keys default to an empty feed. `submissions_by_cik` maps unpadded CIK to a
    submissions payload."""

    def fake_get(url: str, **kwargs: Any) -> FakeResponse:
        if url == SEARCH_URL:
            company = kwargs["params"]["company"]
            return FakeResponse(text=search_by_query.get(company, "<feed></feed>"))
        cik = _cik_from_submissions_url(url)
        return FakeResponse(payload=submissions_by_cik.get(cik))

    return fake_get


def test_match_issuer_name_no_candidates_at_any_truncation_level(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.setattr("utils.resolution_v2_network.requests.get", _fake_get({}, {}))

    result = match_issuer_name(_client(tmp_path), "Nonexistent Company Xyz")

    assert result["match_status"] == STATUS_NO_CANDIDATES
    assert result["matched_cik"] is None


def test_match_issuer_name_too_many_candidates_skips_validation(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A query returning more candidates than the validate-individually cap is reported
    ambiguous without spending a single validation request — a shorter query would only
    return more, never fewer."""
    calls = []
    atom = _atom([str(n) for n in range(1, 10)])  # 9 candidates > MAX_CANDIDATES_TO_VALIDATE

    def fake_get(url: str, **kwargs: Any) -> FakeResponse:
        calls.append(url)
        return FakeResponse(text=atom)

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co")

    assert result["match_status"] == STATUS_AMBIGUOUS
    assert result["candidate_count"] == 9
    assert result["matched_cik"] is None
    assert calls == [SEARCH_URL]  # no submissions fetches at all


def test_match_issuer_name_two_candidates_both_validate_is_genuinely_ambiguous(
    tmp_path: Path, monkeypatch: Any
) -> None:
    atom = _atom(["1", "2"])
    submissions = {
        "1": {"sic": "1000", "sicDescription": "A", "name": "Ambiguous Co"},
        "2": {"sic": "2000", "sicDescription": "B", "name": "Ambiguous Co"},
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", _fake_get({"Ambiguous Co": atom}, submissions)
    )

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co")

    assert result["match_status"] == STATUS_AMBIGUOUS
    assert result["candidate_count"] == 2
    assert result["matched_cik"] is None


def test_match_issuer_name_single_candidate_matches_on_first_query(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """No truncation needed — a fast path that costs exactly one search request."""
    calls = []
    atom = _atom(["104599"])
    submissions = {"104599": {"sic": "5731", "sicDescription": "Retail-Electronics",
                               "name": "CIRCUIT CITY STORES INC"}}

    def fake_get(url: str, **kwargs: Any) -> FakeResponse:
        calls.append((url, kwargs.get("params", {}).get("company")))
        if url == SEARCH_URL:
            return FakeResponse(text=atom)
        return FakeResponse(payload=submissions["104599"])

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)

    result = match_issuer_name(_client(tmp_path), "Circuit City Stores")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "104599"
    assert result["sic"] == "5731"
    search_calls = [c for c in calls if c[0] == SEARCH_URL]
    assert len(search_calls) == 1  # matched on the very first, untruncated query


def test_match_issuer_name_matches_after_descriptor_strip(tmp_path: Path, monkeypatch: Any) -> None:
    atom = _atom(["313216"])
    submissions = {"313216": {"sic": "3826", "sicDescription": "Instruments", "name": "ABB LTD"}}
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"ABB LTD": atom}, submissions),  # raw "ABB LTD-SPON ADR" query gets nothing
    )

    result = match_issuer_name(_client(tmp_path), "ABB LTD-SPON ADR")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "313216"


def test_match_issuer_name_finds_match_via_progressive_truncation(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The real bug this fix targets: the raw name (with a trailing legal-suffix word
    the registrant's exact string doesn't share, here compounded by a SEC jurisdiction
    tag) returns nothing, but the truncated 2-word query finds the real registrant, and
    validation succeeds despite the "/TX" tag in the actual name."""
    atom = _atom(["1839341"])
    submissions = {
        "1839341": {"sic": "7372", "sicDescription": "Software", "name": "Core Scientific, Inc./tx"}
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"CORE SCIENTIFIC": atom}, submissions),  # "CORE SCIENTIFIC INC" gets nothing
    )

    result = match_issuer_name(_client(tmp_path), "CORE SCIENTIFIC INC")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "1839341"


def test_match_issuer_name_rejects_name_match_with_blank_sic(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Real SEC data has a genuine name collision this guards against: searching
    "Confluent, Inc." (the real Kafka company) also turns up an unrelated same-named
    shell with a blank SIC. A candidate whose name matches but has no SIC on record is
    never accepted, even alone with no competing candidate."""
    atom = _atom(["1171179"])
    submissions = {"1171179": {"sic": "", "sicDescription": "", "name": "CONFLUENT INC"}}
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"CONFLUENT INC": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "CONFLUENT INC-CLASS A")

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["matched_cik"] is None


def test_match_issuer_name_accepts_name_match_with_real_sic(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The counterpart to the blank-SIC rejection above: a candidate with a real SIC on
    record is accepted exactly as before — the guard only rejects blank-SIC candidates,
    it doesn't add friction to the normal case."""
    atom = _atom(["1699838"])
    submissions = {
        "1699838": {"sic": "7372", "sicDescription": "Software", "name": "Confluent, Inc."}
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"CONFLUENT INC": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "CONFLUENT INC-CLASS A")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "1699838"


def test_match_issuer_name_no_validated_match_when_nothing_matches_any_variant(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Candidates exist at some truncation level, but none of their real names match —
    distinguishable from STATUS_NO_CANDIDATES (EDGAR found literally nothing)."""
    atom = _atom(["999999"])
    submissions = {"999999": {"sic": "1234", "sicDescription": "X", "name": "TOTALLY DIFFERENT CO"}}
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Circuit City": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "Circuit City Stores")

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["matched_cik"] is None


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
    than a false negative."""
    import requests

    atom = _atom(["104599"])

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


def test_match_issuer_name_matches_via_former_name(tmp_path: Path, monkeypatch: Any) -> None:
    """The real gap this fix targets: the registrant's *current* name no longer matches
    the era's historical issuer name because the company renamed or merged since, but
    SEC's own `formerNames` history — real shape, Cabot Oil & Gas Corp's 2021 merger
    into Coterra Energy — carries the exact queried name."""
    atom = _atom(["858470"])
    submissions = {
        "858470": {
            "sic": "1311",
            "sicDescription": "Crude Petroleum & Natural Gas",
            "name": "Coterra Energy Inc.",
            "formerNames": [
                {
                    "name": "CABOT OIL & GAS CORP",
                    "from": "1994-05-12T04:00:00.000Z",
                    "to": "2021-09-29T04:00:00.000Z",
                }
            ],
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"CABOT OIL & GAS CORP": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "CABOT OIL & GAS CORP")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "858470"
    assert result["candidate_name"] == "CABOT OIL & GAS CORP"


def test_match_issuer_name_former_name_still_requires_real_sic(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The blank-SIC collision guard applies to a `formerNames` match exactly like a
    current-name match — a renamed shell with no SIC on record still isn't trusted."""
    atom = _atom(["1"])
    submissions = {
        "1": {
            "sic": "",
            "sicDescription": "",
            "name": "Some Shell Co",
            "formerNames": [{"name": "OLD ISSUER NAME CORP", "from": "2000", "to": "2020"}],
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"OLD ISSUER NAME CORP": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "OLD ISSUER NAME CORP")

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["matched_cik"] is None


def test_match_issuer_name_two_former_name_matches_is_ambiguous(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Two distinct real registrants both having once carried the queried name is
    genuine ambiguity, not a match to guess between — `formerNames` doesn't loosen the
    existing ambiguity rule."""
    atom = _atom(["1", "2"])
    submissions = {
        "1": {
            "sic": "1000",
            "sicDescription": "A",
            "name": "New Name One",
            "formerNames": [{"name": "SHARED OLD NAME INC", "from": "2000", "to": "2010"}],
        },
        "2": {
            "sic": "2000",
            "sicDescription": "B",
            "name": "New Name Two",
            "formerNames": [{"name": "SHARED OLD NAME INC", "from": "2010", "to": "2020"}],
        },
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"SHARED OLD NAME INC": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "SHARED OLD NAME INC")

    assert result["match_status"] == STATUS_AMBIGUOUS
    assert result["matched_cik"] is None


def test_match_issuer_name_truncates_a_two_word_name_to_one_word(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The real gap this fix targets: a name that's already exactly 2 words after
    descriptor-stripping ("ZENDESK INC") could never truncate further under the old
    2-word floor. If the full query doesn't literally prefix-match the registrant's real
    punctuation ("Zendesk, Inc."), the only remaining query is the single distinctive
    word — real shape, live-verified against SEC before shipping."""
    atom = _atom(["1463172"])
    submissions = {
        "1463172": {"sic": "7374", "sicDescription": "Data Processing", "name": "Zendesk, Inc."}
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"ZENDESK": atom}, submissions),  # "ZENDESK INC" (2-word) gets nothing
    )

    result = match_issuer_name(_client(tmp_path), "ZENDESK INC")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "1463172"


def test_match_issuer_name_one_word_query_still_rejects_blank_sic_shell(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The 1-word floor is only safe because the existing guards don't change — a
    single-word query surfacing an unrelated blank-SIC entity (real shape: searching
    "HOLOGIC" alone first turns up a limited partnership, not the real Hologic, Inc.)
    still isn't trusted just because it was the only candidate."""
    atom = _atom(["1566252"])
    submissions = {
        "1566252": {"sic": "", "sicDescription": "", "name": "Hologic Limited Partnership"}
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"HOLOGIC": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "HOLOGIC INC")

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["matched_cik"] is None


def test_match_issuer_name_one_word_query_over_candidate_cap_stays_ambiguous(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The real `CONFLUENT` case: a 1-word query for a genuinely common root returns more
    candidates than `MAX_CANDIDATES_TO_VALIDATE`, so it's reported ambiguous via the
    existing count guard exactly as a broad 2-word query already was — the 1-word floor
    doesn't bypass that guard."""
    atom = _atom([str(n) for n in range(1, 10)])  # 9 candidates > MAX_CANDIDATES_TO_VALIDATE
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"CONFLUENT": atom}, {}),
    )

    result = match_issuer_name(_client(tmp_path), "CONFLUENT")

    assert result["match_status"] == STATUS_AMBIGUOUS
    assert result["matched_cik"] is None


def test_match_issuer_name_reuses_cached_sic_fetch(tmp_path: Path, monkeypatch: Any) -> None:
    """If the CIK's submissions data was already fetched (e.g. by the main SIC pass),
    validating a name match here is a free cache hit, not a new request."""
    atom = _atom(["104599"])
    submissions = {"104599": {"sic": "5731", "sicDescription": "Retail", "name": "CIRCUIT CITY STORES INC"}}
    calls = []

    def fake_get(url: str, **kwargs: Any) -> FakeResponse:
        calls.append(url)
        if url == SEARCH_URL:
            return FakeResponse(text=atom)
        return FakeResponse(payload=submissions["104599"])

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)
    client = _client(tmp_path)

    fetch_sic(client, "104599")  # pre-warm the cache, as the main SIC pass would
    calls.clear()

    result = match_issuer_name(client, "Circuit City Stores")

    assert result["match_status"] == STATUS_MATCHED
    assert calls == [SEARCH_URL]  # only the search call, submissions was a cache hit
