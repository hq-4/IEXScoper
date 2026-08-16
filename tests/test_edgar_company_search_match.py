from __future__ import annotations

from pathlib import Path
from typing import Any

from utils.edgar_company_search_match import (
    BASIS_TICKER_LOOKUP,
    MAX_CANDIDATES_TO_VALIDATE,
    STATUS_AMBIGUOUS,
    STATUS_FETCH_ERROR,
    STATUS_MATCHED,
    STATUS_NO_CANDIDATES,
    STATUS_NO_VALIDATED_MATCH,
    _expand_query_abbreviations,
    _search_query_variants,
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


def _fake_get(
    search_by_query: dict[str, str],
    submissions_by_cik: dict[str, dict[str, Any]],
    ticker_by_symbol: dict[str, str] | None = None,
):
    """`search_by_query` maps the exact `company` search param to an atom string;
    missing keys default to an empty feed. `submissions_by_cik` maps unpadded CIK to a
    submissions payload. `ticker_by_symbol` maps the exact `CIK` search param (used for
    the Phase 30 ticker-lookup fallback, never a name search) to an atom string;
    missing keys default to an empty feed too."""
    ticker_by_symbol = ticker_by_symbol or {}

    def fake_get(url: str, **kwargs: Any) -> FakeResponse:
        if url == SEARCH_URL:
            params = kwargs["params"]
            if "CIK" in params:
                return FakeResponse(text=ticker_by_symbol.get(params["CIK"], "<feed></feed>"))
            company = params["company"]
            return FakeResponse(text=search_by_query.get(company, "<feed></feed>"))
        cik = _cik_from_submissions_url(url)
        return FakeResponse(payload=submissions_by_cik.get(cik))

    return fake_get


def test_expand_query_abbreviations_substitutes_known_tokens() -> None:
    assert _expand_query_abbreviations("MICHAELS COS INC") == "MICHAELS COMPANIES INC"
    assert _expand_query_abbreviations("NORTHERN TECHNOLOGIES INTL") == (
        "NORTHERN TECHNOLOGIES INTERNATIONAL"
    )
    # Case-insensitive token match, but the expansion itself is always uppercase.
    assert _expand_query_abbreviations("Juniper Industrial Hldgs") == (
        "Juniper Industrial HOLDINGS"
    )


def test_expand_query_abbreviations_returns_none_when_nothing_to_expand() -> None:
    # No caller adds a redundant identical variant when there's no abbreviation present.
    assert _expand_query_abbreviations("MICHAELS STORES INC") is None


def test_search_query_variants_includes_abbreviation_expansion() -> None:
    variants = _search_query_variants("MICHAELS COS INC/THE")
    assert "MICHAELS COMPANIES INC/THE" in variants
    # The expanded form sits right after its source variant, not at the very end.
    source_index = variants.index("MICHAELS COS INC/THE")
    assert variants[source_index + 1] == "MICHAELS COMPANIES INC/THE"


def test_match_issuer_name_no_candidates_at_any_truncation_level(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.setattr("utils.resolution_v2_network.requests.get", _fake_get({}, {}))

    result = match_issuer_name(_client(tmp_path), "Nonexistent Company Xyz")

    assert result["match_status"] == STATUS_NO_CANDIDATES
    assert result["matched_cik"] is None


def test_match_issuer_name_ticker_fallback_runs_after_ambiguous_over_cap_result(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Regression test (Phase 30): an over-cap `ambiguous_candidates` result returns
    early from *inside* `_match_by_name`'s loop -- the ticker fallback must still run
    afterward, not only when name search runs dry with zero candidates."""
    over_cap = MAX_CANDIDATES_TO_VALIDATE + 1
    over_cap_atom = _atom([str(n) for n in range(1, over_cap + 1)])
    ticker_atom = _atom(["1564902"])
    submissions = {
        "1564902": {
            "sic": "7990",
            "sicDescription": "Services-Miscellaneous Amusement & Recreation",
            "name": "United Parks & Resorts Inc.",
            "formerNames": [{"name": "SeaWorld Entertainment, Inc.", "from": "2012", "to": "2024"}],
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get(
            {"SEAWORLD ENTERTAINMENT INC": over_cap_atom, "SEAWORLD": over_cap_atom},
            submissions,
            ticker_by_symbol={"SEAS": ticker_atom},
        ),
    )

    result = match_issuer_name(_client(tmp_path), "SEAWORLD ENTERTAINMENT INC", ticker="SEAS")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "1564902"
    assert result["match_basis"] == BASIS_TICKER_LOOKUP


def test_match_issuer_name_no_ticker_stays_unresolved_when_name_search_exhausts(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Same setup as the ticker-fallback success test below, but with no `ticker`
    argument -- proves the fallback is opt-in, not automatic."""
    atom = _atom(["1564902"])
    submissions = {
        "1564902": {
            "sic": "7990",
            "sicDescription": "Services-Miscellaneous Amusement & Recreation",
            "name": "United Parks & Resorts Inc.",
            "formerNames": [{"name": "SeaWorld Entertainment, Inc.", "from": "2012", "to": "2024"}],
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({}, submissions, ticker_by_symbol={"SEAS": atom}),
    )

    result = match_issuer_name(_client(tmp_path), "SEAWORLD ENTERTAINMENT INC")

    assert result["match_status"] == STATUS_NO_CANDIDATES
    assert result["matched_cik"] is None


def test_match_issuer_name_resolves_via_ticker_fallback_when_name_search_exhausts(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The real SEAS/SeaWorld case (Phase 30): no name-search query variant returns
    this CIK at all, but the ticker resolves it directly, and the candidate's real
    name matches via `former_names` -- exactly the same acceptance path a name-search
    hit would go through."""
    atom = _atom(["1564902"])
    submissions = {
        "1564902": {
            "sic": "7990",
            "sicDescription": "Services-Miscellaneous Amusement & Recreation",
            "name": "United Parks & Resorts Inc.",
            "formerNames": [{"name": "SeaWorld Entertainment, Inc.", "from": "2012", "to": "2024"}],
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({}, submissions, ticker_by_symbol={"SEAS": atom}),
    )

    result = match_issuer_name(_client(tmp_path), "SEAWORLD ENTERTAINMENT INC", ticker="SEAS")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "1564902"
    assert result["match_basis"] == BASIS_TICKER_LOOKUP


def test_match_issuer_name_ticker_fallback_recovers_extra_trailing_token_case(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The real Phase 22/30 case: OpenFIGI's 28-char truncation lands mid-word on the
    trailing legal suffix itself ("INC" -> "IN"), leaving a whole extra token
    `_names_match` deliberately never recovers (Phase 22's documented limitation for
    the broad name-search path). The ticker fallback's broader `_names_match_broad`
    does recover it -- safe here because the ticker registry already narrowed the
    field to exactly one candidate before this check ever runs."""
    atom = _atom(["1270073"])
    submissions = {
        "1270073": {
            "sic": "2834",
            "sicDescription": "Pharmaceutical Preparations",
            "name": "INTERCEPT PHARMACEUTICALS, INC.",
            "formerNames": [],
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({}, submissions, ticker_by_symbol={"ICPT": atom}),
    )

    result = match_issuer_name(_client(tmp_path), "INTERCEPT PHARMACEUTICALS IN", ticker="ICPT")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "1270073"
    assert result["match_basis"] == BASIS_TICKER_LOOKUP


def test_match_issuer_name_ticker_fallback_broad_match_still_rejects_unrelated_company(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The broader `_names_match_broad` check must not become a rubber stamp: a
    same-first-word, genuinely different company (no shared trailing-token relation)
    is still rejected."""
    atom = _atom(["1270073"])
    submissions = {
        "1270073": {
            "sic": "2834",
            "sicDescription": "Pharmaceutical Preparations",
            "name": "INTERCEPT DIFFERENT COMPANY ENTIRELY INC",
            "formerNames": [],
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({}, submissions, ticker_by_symbol={"ICPT": atom}),
    )

    result = match_issuer_name(_client(tmp_path), "INTERCEPT PHARMACEUTICALS IN", ticker="ICPT")

    assert result["match_status"] == STATUS_NO_CANDIDATES
    assert result["matched_cik"] is None


def test_match_issuer_name_ticker_fallback_rejects_reused_ticker_name_mismatch(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A ticker now held by a completely unrelated company must not be trusted just
    because it's the only candidate the fallback found -- the same name-match gate
    that protects every other candidate source applies here too."""
    atom = _atom(["9999999"])
    submissions = {
        "9999999": {
            "sic": "7372",
            "sicDescription": "Prepackaged Software",
            "name": "Completely Unrelated Software Inc",
            "formerNames": [],
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({}, submissions, ticker_by_symbol={"SEAS": atom}),
    )

    result = match_issuer_name(_client(tmp_path), "SEAWORLD ENTERTAINMENT INC", ticker="SEAS")

    assert result["match_status"] == STATUS_NO_CANDIDATES
    assert result["matched_cik"] is None


def test_match_issuer_name_ticker_fallback_rejects_blank_sic(
    tmp_path: Path, monkeypatch: Any
) -> None:
    atom = _atom(["1564902"])
    submissions = {
        "1564902": {
            "sic": "",
            "sicDescription": "",
            "name": "United Parks & Resorts Inc.",
            "formerNames": [{"name": "SeaWorld Entertainment, Inc.", "from": "2012", "to": "2024"}],
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({}, submissions, ticker_by_symbol={"SEAS": atom}),
    )

    result = match_issuer_name(_client(tmp_path), "SEAWORLD ENTERTAINMENT INC", ticker="SEAS")

    assert result["match_status"] == STATUS_NO_CANDIDATES
    assert result["matched_cik"] is None


def test_match_issuer_name_too_many_candidates_skips_validation(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A query returning more candidates than the validate-individually cap is reported
    ambiguous without spending a single validation request — a shorter query would only
    return more, never fewer."""
    calls = []
    over_cap = MAX_CANDIDATES_TO_VALIDATE + 1
    atom = _atom([str(n) for n in range(1, over_cap + 1)])

    def fake_get(url: str, **kwargs: Any) -> FakeResponse:
        calls.append(url)
        return FakeResponse(text=atom)

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co")

    assert result["match_status"] == STATUS_AMBIGUOUS
    assert result["candidate_count"] == over_cap
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


def test_match_issuer_name_finds_candidate_only_via_abbreviation_expansion(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """"MICHAELS COS INC/THE" itself (and every unexpanded truncation) returns zero
    EDGAR candidates -- only the "COS" -> "COMPANIES" expanded query has a fake
    response wired at all, proving the match genuinely depends on
    `_search_query_variants`'s abbreviation expansion, not some other fallback."""
    atom = _atom(["1593936"])
    submissions = {"1593936": {"sic": "5945", "sicDescription": "Hobby Stores", "name": "Michaels Companies Inc"}}
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"MICHAELS COMPANIES INC/THE": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "MICHAELS COS INC/THE")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "1593936"


def _filings(
    dates: list[str], *, has_older_shards: bool = False, form: str = "10-K"
) -> dict[str, Any]:
    """Real shape: `filings.recent` is parallel columnar arrays. `form` defaults to a
    substantive type (`10-K`); pass e.g. `"SC 13G"` to simulate an ownership-disclosure-
    only filer for the Phase 19 `blank_sic_lead_high_confidence` tests."""
    return {
        "recent": {"form": [form] * len(dates), "filingDate": dates},
        "files": [{"name": "shard-001.json"}] if has_older_shards else [],
    }


ERA_SPAN = ("2016-12-12", "2023-11-22")


def test_match_issuer_name_disambiguates_two_way_tie_via_filing_activity(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The real case this targets: two genuinely different registrants both validate by
    name (the real Continental Resources vs an unrelated "Continental Resources Group,
    Inc." shell) — but only one of them has any real filing activity overlapping the
    era. The shell's last filing (a 15-12G deregistration) predates the era entirely."""
    atom = _atom(["732834", "1430975"])
    submissions = {
        "732834": {
            "sic": "1311",
            "sicDescription": "Crude Petroleum & Natural Gas",
            "name": "Continental Resources, Inc.",
            "filings": _filings(["2009-12-28", "2018-05-01", "2026-07-31"]),
        },
        "1430975": {
            "sic": "1000",
            "sicDescription": "Metal Mining",
            "name": "Continental Resources, Inc.",
            "filings": _filings(["2008-06-30", "2013-03-05"]),
        },
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Continental Resources": atom}, submissions),
    )

    result = match_issuer_name(
        _client(tmp_path), "Continental Resources Inc", era_span=ERA_SPAN
    )

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "732834"
    assert result["match_basis"] == "filing_activity_tiebreak"


def test_match_issuer_name_filing_activity_both_plausible_stays_ambiguous(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Both plausible, and neither's own filing window fully spans `ERA_SPAN` (a single
    filing date each, nowhere near either boundary) — no containment signal either, so
    this stays genuinely ambiguous rather than guessed."""
    atom = _atom(["1", "2"])
    submissions = {
        "1": {"sic": "1000", "sicDescription": "A", "name": "Ambiguous Co",
              "filings": _filings(["2018-01-01"])},
        "2": {"sic": "2000", "sicDescription": "B", "name": "Ambiguous Co",
              "filings": _filings(["2019-01-01"])},
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", _fake_get({"Ambiguous Co": atom}, submissions)
    )

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_AMBIGUOUS
    assert result["matched_cik"] is None


def test_match_issuer_name_filing_window_containment_breaks_a_multi_plausible_tie(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Phase 20: the real `LAREDO PETROLEUM INC` case — two candidates both validate by
    name and both have some filing landing in the era, but only one candidate's own
    filing window fully spans it (a holdco-reorg successor CIK whose history covers the
    whole era, vs. the original CIK whose filings stop partway through). The containing
    candidate wins, labeled with a distinct match_basis."""
    atom = _atom(["1519352", "1528129"])
    submissions = {
        "1519352": {
            "sic": "1311", "sicDescription": "Crude Petroleum & Natural Gas",
            "name": "Laredo Petroleum, Inc.",
            # stops well before ERA_SPAN's end (2023-11-22) -- plausible, not containing
            "filings": _filings(["2011-05-06", "2017-01-01", "2019-01-31"]),
        },
        "1528129": {
            "sic": "1311", "sicDescription": "Crude Petroleum & Natural Gas",
            "name": "Vital Energy, Inc.",
            "formerNames": [{"name": "Laredo Petroleum, Inc.", "from": "2016", "to": "2023"}],
            # spans the entire ERA_SPAN and beyond -- fully contains it
            "filings": _filings(["2016-05-19", "2019-06-01", "2025-12-29"]),
        },
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Laredo Petroleum": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "Laredo Petroleum Inc", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "1528129"
    assert result["match_basis"] == "filing_window_containment_tiebreak"


def test_match_issuer_name_filing_window_containment_needs_exactly_one_winner(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The real `LIFE STORAGE INC` case — a REIT parent and its operating partnership
    both file continuously through and past the whole era (a genuinely different shape
    from succession: both candidates' windows fully contain it). Containment can't pick
    between two full containments any more than plain plausibility could — stays
    ambiguous, no guess."""
    atom = _atom(["1", "2"])
    submissions = {
        "1": {
            "sic": "6500", "sicDescription": "Real Estate", "name": "Ambiguous Co LP",
            "filings": _filings(["2010-01-01", "2018-01-01", "2025-01-01"]),
        },
        "2": {
            "sic": "6798", "sicDescription": "REIT", "name": "Ambiguous Co LP",
            "filings": _filings(["2005-01-01", "2019-01-01", "2025-01-01"]),
        },
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", _fake_get({"Ambiguous Co LP": atom}, submissions)
    )

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co LP", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_AMBIGUOUS
    assert result["matched_cik"] is None


def test_match_issuer_name_without_era_span_ignores_filing_activity(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Backwards compatibility: no `era_span` supplied stays byte-identical to today's
    ambiguous outcome, even with clearly disjoint filing histories present."""
    atom = _atom(["732834", "1430975"])
    submissions = {
        "732834": {
            "sic": "1311", "sicDescription": "A", "name": "Continental Resources, Inc.",
            "filings": _filings(["2009-12-28", "2018-05-01"]),
        },
        "1430975": {
            "sic": "1000", "sicDescription": "B", "name": "Continental Resources, Inc.",
            "filings": _filings(["2008-06-30", "2013-03-05"]),
        },
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Continental Resources": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "Continental Resources Inc")

    assert result["match_status"] == STATUS_AMBIGUOUS
    assert result["matched_cik"] is None


def test_match_issuer_name_quiet_filer_inside_bracketing_history_stays_ambiguous(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The key fail-closed case: a candidate whose filing history brackets the era but
    has no filing landing inside the window is `ACTIVITY_UNKNOWN`, not rejected — a real
    filer can legitimately be quiet across a short era."""
    atom = _atom(["1", "2"])
    submissions = {
        "1": {
            "sic": "1000", "sicDescription": "A", "name": "Ambiguous Co",
            "filings": _filings(["2010-01-01", "2025-01-01"]),  # brackets, nothing inside
        },
        "2": {
            "sic": "2000", "sicDescription": "B", "name": "Ambiguous Co",
            "filings": _filings(["2005-01-01", "2006-01-01"]),  # provably disjoint
        },
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", _fake_get({"Ambiguous Co": atom}, submissions)
    )

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_AMBIGUOUS
    assert result["matched_cik"] is None


def test_match_issuer_name_unfetched_older_shard_stays_ambiguous(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A candidate whose known filings all postdate the era, but with older shard
    history that wasn't fetched, is `ACTIVITY_UNKNOWN` — "didn't look back far enough"
    is never treated as "no activity"."""
    atom = _atom(["1", "2"])
    submissions = {
        "1": {
            "sic": "1000", "sicDescription": "A", "name": "Ambiguous Co",
            "filings": _filings(["2024-01-01"], has_older_shards=True),
        },
        "2": {
            "sic": "2000", "sicDescription": "B", "name": "Ambiguous Co",
            "filings": _filings(["2005-01-01"]),  # provably disjoint, no older shard
        },
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", _fake_get({"Ambiguous Co": atom}, submissions)
    )

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_AMBIGUOUS
    assert result["matched_cik"] is None


def test_match_issuer_name_validates_all_candidates_before_tiebreak(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The early break at 2 validated candidates is gone: a 3rd name-validating
    candidate is the real one, so the tie-break needs to see it."""
    atom = _atom(["1", "2", "3"])
    submissions = {
        "1": {"sic": "1000", "sicDescription": "A", "name": "Ambiguous Co",
              "filings": _filings(["2005-01-01"])},
        "2": {"sic": "2000", "sicDescription": "B", "name": "Ambiguous Co",
              "filings": _filings(["2006-01-01"])},
        "3": {"sic": "3000", "sicDescription": "C", "name": "Ambiguous Co",
              "filings": _filings(["2018-01-01"])},
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", _fake_get({"Ambiguous Co": atom}, submissions)
    )

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "3"


def test_match_issuer_name_single_candidate_provably_disjoint_is_rejected(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Phase 16: the real gap the 81-name existing-match audit found — a single
    validated candidate whose own filing history provably can't overlap `era_span`
    (real shape: `AETNA INC`, filings ending 2015 for an era starting 2016-12-12) is no
    longer accepted on name+SIC alone. With no shorter query left to try, it correctly
    falls through to `no_validated_match` rather than a confidently-wrong `matched`."""
    atom = _atom(["104599"])
    submissions = {
        "104599": {
            "sic": "5731", "sicDescription": "Retail-Electronics", "name": "CIRCUIT CITY STORES INC",
            "filings": _filings(["2005-01-01"]),  # disjoint from ERA_SPAN
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Circuit City Stores": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "Circuit City Stores", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["matched_cik"] is None


def test_match_issuer_name_single_candidate_without_era_span_still_matches(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Backwards compatibility: no `era_span` supplied means `_provably_disjoint` fails
    open, so a single validated candidate matches exactly as before Phase 16 — same
    "no era_span, no new behavior" contract the tie-break guard already honors."""
    atom = _atom(["104599"])
    submissions = {
        "104599": {
            "sic": "5731", "sicDescription": "Retail-Electronics", "name": "CIRCUIT CITY STORES INC",
            "filings": _filings(["2005-01-01"]),
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Circuit City Stores": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "Circuit City Stores")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "104599"
    assert result["match_basis"] == "single_validated_candidate"


def test_match_issuer_name_single_candidate_quiet_filer_still_matches(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Fail-closed-on-uncertainty guard: a single candidate whose filing history
    brackets the era but has no filing landing inside it is `ACTIVITY_UNKNOWN`, not
    `ACTIVITY_DISJOINT` — never rejected, same "a real filer can be quiet across a short
    window" reasoning the tie-break guard already applies."""
    atom = _atom(["104599"])
    submissions = {
        "104599": {
            "sic": "5731", "sicDescription": "Retail-Electronics", "name": "CIRCUIT CITY STORES INC",
            "filings": _filings(["2010-01-01", "2025-01-01"]),  # brackets, nothing inside
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Circuit City Stores": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "Circuit City Stores", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "104599"


def test_match_issuer_name_single_candidate_disjoint_falls_through_to_broader_query(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The upside this design enables: rejecting a provably-wrong single candidate
    doesn't just lose the match — it lets the loop try a shorter query, which here
    surfaces the real second candidate and resolves correctly via the existing
    filing-activity tie-break instead of settling for the wrong CIK."""
    full_atom = _atom(["1"])  # only the wrong shell reachable at the full query
    short_atom = _atom(["1", "2"])  # the broader query also finds the real company
    submissions = {
        "1": {
            "sic": "1000", "sicDescription": "A", "name": "Ambiguous Co Inc",
            "filings": _filings(["2005-01-01"]),  # disjoint
        },
        "2": {
            "sic": "2000", "sicDescription": "B", "name": "Ambiguous Co",
            "filings": _filings(["2018-01-01"]),  # plausible
        },
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Ambiguous Co Inc": full_atom, "Ambiguous Co": short_atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co Inc", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "2"
    assert result["match_basis"] == "filing_activity_tiebreak"


def test_match_issuer_name_single_candidate_fetch_error_fails_open(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A filing-activity fetch failure never rejects — the call is normally a guaranteed
    cache hit (the identical payload `_validate_candidates` just fetched via `fetch_sic`
    moments earlier), so an error here signals something unrelated, not evidence against
    the candidate. Patched directly at `fetch_filing_activity`, since the real client
    can't independently fail it without also failing the SIC fetch that runs first (both
    read the identical cached payload)."""
    atom = _atom(["104599"])
    submissions = {"104599": {"sic": "5731", "sicDescription": "Retail", "name": "CIRCUIT CITY STORES INC"}}
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Circuit City Stores": atom}, submissions),
    )
    monkeypatch.setattr(
        "utils.edgar_company_search_match.fetch_filing_activity",
        lambda *a, **k: {"fetch_status": "fetch_error"},
    )

    result = match_issuer_name(_client(tmp_path), "Circuit City Stores", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "104599"


def test_match_issuer_name_both_candidates_disjoint_stays_ambiguous(
    tmp_path: Path, monkeypatch: Any
) -> None:
    atom = _atom(["1", "2"])
    submissions = {
        "1": {"sic": "1000", "sicDescription": "A", "name": "Ambiguous Co",
              "filings": _filings(["2005-01-01"])},
        "2": {"sic": "2000", "sicDescription": "B", "name": "Ambiguous Co",
              "filings": _filings(["2006-01-01"])},
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", _fake_get({"Ambiguous Co": atom}, submissions)
    )

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_AMBIGUOUS
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


def test_match_issuer_name_matches_after_spaced_class_descriptor_strip(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """`"SWEETGREEN INC - CLASS A"` has a space before the hyphen on the unabbreviated
    "CLASS" word -- a real worklist row the original tight `-CLASS [A-Z]$` pattern
    missed even though the equivalent `-CL A` abbreviated pattern already tolerated the
    same spacing."""
    atom = _atom(["1477815"])
    submissions = {
        "1477815": {"sic": "5812", "sicDescription": "Retail-Eating Places", "name": "Sweetgreen, Inc."}
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"SWEETGREEN": atom}, submissions),  # raw/stripped 2-word queries get nothing
    )

    result = match_issuer_name(_client(tmp_path), "SWEETGREEN INC - CLASS A")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "1477815"


def test_match_issuer_name_matches_via_spaced_jurisdiction_tag(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """SEC's own submissions payload returns a spaced "/ XX" jurisdiction tag (e.g.
    `"Alight Inc. / DE"`), not just the tight "/XX" the `Core Scientific, Inc./tx`
    precedent covered -- left alone, the leftover "DE" token blocks the match."""
    atom = _atom(["1753676"])
    submissions = {
        "1753676": {"sic": "7374", "sicDescription": "Services-Computer Processing",
                     "name": "Alight Inc. / DE"}
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"ALIGHT INC": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "ALIGHT INC - CLASS A")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "1753676"


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


def test_match_issuer_name_does_not_recover_extra_trailing_token_truncation(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Documents a real, deliberate limit: `INTERCEPT PHARMACEUTICALS IN` was this fix's
    original motivating example (OpenFIGI's 28-char ceiling cut "INC" down to "IN"), but
    it turns out to need the *same* "extra trailing token" mechanism that produces the
    confirmed `TPG PACE`/`PRIME NUMBER` false positives — "IN" is a whole extra token
    relative to the candidate's suffix-stripped name, not a same-position partial
    truncation of an existing token, and there is no structural way to tell "IN is
    truncation noise" apart from "BENEFICIAL FIN is a real distinguishing name" from
    token shape alone. Correctly stays unmatched — same "better genuinely unresolved
    than confidently wrong" posture as everything else here — rather than risk
    reintroducing the false-positive branch to recover this one case."""
    atom = _atom(["1270073"])
    submissions = {
        "1270073": {
            "sic": "2834", "sicDescription": "Pharmaceutical Preparations",
            "name": "INTERCEPT PHARMACEUTICALS, INC.",
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"INTERCEPT PHARMACEUTICALS IN": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "INTERCEPT PHARMACEUTICALS IN")

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["matched_cik"] is None


def test_match_issuer_name_matches_mid_word_truncation_not_just_trailing_letter(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The real `TPG PACE BENEFICIAL FIN` case — "FIN" is a partial truncation of
    "FINANCE" at the same token position, not a bare extra letter."""
    atom = _atom(["1819399"])
    submissions = {
        "1819399": {
            "sic": "6770", "sicDescription": "Blank Checks",
            "name": "TPG Pace Beneficial Finance Corp.",
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"TPG PACE BENEFICIAL FIN": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "TPG PACE BENEFICIAL FIN")

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "1819399"


def test_match_issuer_name_rejects_short_candidate_prefixing_a_different_sibling(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Critical regression guard: the real false positive this fix's own quantification
    caught before shipping. "TPG Pace Holdings Corp." normalizes down to just "TPG PACE"
    (both "Holdings" and "Corp." are legal suffixes) — it must NOT spuriously match the
    unrelated sibling SPAC "TPG PACE BENEFICIAL FIN" just because it's a short prefix of
    that name. Different token count at the divergence point (not a same-position
    partial-truncation of the final word), so `_is_safe_final_token_truncation` must
    reject it even though `sec_name_cik_lookup._is_prefix_relation` (Tier D's broader,
    index-uniqueness-guarded version) would accept it."""
    atom = _atom(["1"])
    submissions = {
        "1": {
            "sic": "6770", "sicDescription": "Blank Checks",
            "name": "TPG Pace Holdings Corp.",
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"TPG PACE BENEFICIAL FIN": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "TPG PACE BENEFICIAL FIN")

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["matched_cik"] is None


def test_match_issuer_name_rejects_short_candidate_prefixing_unrelated_company(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The other real false positive caught: "Prime Number Holding Ltd" collapses to
    "PRIME NUMBER" and must not spuriously match the unrelated "Prime Number
    Acquisition..." family."""
    atom = _atom(["1"])
    submissions = {
        "1": {
            "sic": "6770", "sicDescription": "Blank Checks",
            "name": "Prime Number Holding Ltd",
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"PRIME NUMBER ACQUISITIO": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "PRIME NUMBER ACQUISITIO")

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["matched_cik"] is None


def test_match_issuer_name_truncation_requires_minimum_partial_length(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """SPAC-sequel-numbering guard, same floor Tier D already relies on: "II" is a
    literal string prefix of "III", but a genuinely different company, not a truncation
    of the same name — `MIN_PARTIAL_TOKEN_CHARS` blocks matching on a token this short
    regardless of what it contains."""
    atom = _atom(["1"])
    submissions = {
        "1": {
            "sic": "6770", "sicDescription": "Blank Checks",
            "name": "Example Acquisition Corp III",
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"EXAMPLE ACQUISITION CORP II": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "EXAMPLE ACQUISITION CORP II")

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
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
    atom = _atom([str(n) for n in range(1, MAX_CANDIDATES_TO_VALIDATE + 2)])
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


def test_match_issuer_name_flags_identity_disproven_when_single_candidate_rejected(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Phase 18: the real `UTX` case — the name is provably wrong for the era (its one
    validated candidate's filing history is disjoint), and no shorter query finds a
    replacement. `identity_disproven` surfaces that proof even though the final status
    is the same `no_validated_match` a plain name-mismatch would also produce."""
    atom = _atom(["104599"])
    submissions = {
        "104599": {
            "sic": "5731", "sicDescription": "Retail-Electronics", "name": "CIRCUIT CITY STORES INC",
            "filings": _filings(["2005-01-01"]),  # disjoint from ERA_SPAN
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Circuit City Stores": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "Circuit City Stores", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["identity_disproven"] is True


def test_match_issuer_name_identity_disproven_stays_true_after_tiebreak_recovery(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The disproven flag records that the *original* name-only candidate was rejected,
    even when a later, broader query recovers a different, correct match via the
    tie-break — both facts are true and both are worth keeping visible."""
    full_atom = _atom(["1"])  # only the wrong shell reachable at the full query
    short_atom = _atom(["1", "2"])  # the broader query also finds the real company
    submissions = {
        "1": {
            "sic": "1000", "sicDescription": "A", "name": "Ambiguous Co Inc",
            "filings": _filings(["2005-01-01"]),  # disjoint
        },
        "2": {
            "sic": "2000", "sicDescription": "B", "name": "Ambiguous Co",
            "filings": _filings(["2018-01-01"]),  # plausible
        },
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Ambiguous Co Inc": full_atom, "Ambiguous Co": short_atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co Inc", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_MATCHED
    assert result["matched_cik"] == "2"
    assert result["identity_disproven"] is True


def test_match_issuer_name_identity_disproven_false_for_ordinary_mismatch(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The flag is specific to a *proven* rejection, not any unmatched outcome — a plain
    name mismatch (no era_span involvement at all) leaves it False."""
    atom = _atom(["999999"])
    submissions = {"999999": {"sic": "1234", "sicDescription": "X", "name": "TOTALLY DIFFERENT CO"}}
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Circuit City": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "Circuit City Stores")

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["identity_disproven"] is False


def test_match_issuer_name_identity_disproven_false_for_ordinary_match(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A clean single-candidate match (nothing ever rejected) reports False, not just
    absent — every result row carries the field regardless of outcome."""
    atom = _atom(["104599"])
    submissions = {
        "104599": {
            "sic": "5731", "sicDescription": "Retail-Electronics", "name": "CIRCUIT CITY STORES INC",
            "filings": _filings(["2018-01-01"]),  # inside ERA_SPAN -- plausible, not disjoint
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"Circuit City Stores": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "Circuit City Stores", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_MATCHED
    assert result["identity_disproven"] is False


def test_match_issuer_name_surfaces_blank_sic_lead_without_matching(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Phase 19: the real `FIRST REPUBLIC BANK` case — an exact name match (after the
    `/CA` jurisdiction tag strips, same as `Core Scientific, Inc./tx`) with a blank SIC
    and plausible filing activity is surfaced as a research lead, never accepted as a
    match. Auditing the real population before shipping found "blank SIC + a filing
    lands in the era" too weak a signal to auto-accept on (see the module docstring's
    Phase 19 entry) — this stays informational only."""
    atom = _atom(["1132979"])
    submissions = {
        "1132979": {
            "sic": "", "sicDescription": "", "name": "FIRST REPUBLIC BANK",
            "entityType": "other",
            "filings": _filings(["2014-02-12", "2018-05-01", "2022-02-10"], form="SC 13G"),
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"FIRST REPUBLIC BANK/CA": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "FIRST REPUBLIC BANK/CA", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["matched_cik"] is None
    assert result["blank_sic_lead_cik"] == "1132979"
    assert result["blank_sic_lead_name"] == "FIRST REPUBLIC BANK"
    assert result["blank_sic_lead_high_confidence"] is False  # entityType="other", no 10-K/10-Q


def test_match_issuer_name_blank_sic_lead_high_confidence_when_operating_with_substantive_filing(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The other half of the real population: a blank-SIC candidate SEC itself
    classifies as `entityType="operating"` with a genuine substantive filing (not just
    an ownership-disclosure form) landing in the era is flagged high-confidence, so a
    researcher knows which leads are worth checking first."""
    atom = _atom(["1"])
    submissions = {
        "1": {
            "sic": "", "sicDescription": "", "name": "Ambiguous Co",
            "entityType": "operating",
            "filings": _filings(["2018-05-01"], form="10-K"),  # substantive, inside era
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", _fake_get({"Ambiguous Co": atom}, submissions)
    )

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["blank_sic_lead_cik"] == "1"
    assert result["blank_sic_lead_high_confidence"] is True


def test_match_issuer_name_no_blank_sic_lead_without_era_span(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Backwards compatibility: with no `era_span` supplied, no lead is ever surfaced —
    behavior stays byte-identical to before Phase 19."""
    atom = _atom(["1132979"])
    submissions = {
        "1132979": {
            "sic": "", "sicDescription": "", "name": "FIRST REPUBLIC BANK",
            "filings": _filings(["2018-05-01"], form="SC 13G"),
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"FIRST REPUBLIC BANK/CA": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "FIRST REPUBLIC BANK/CA")

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["matched_cik"] is None
    assert result["blank_sic_lead_cik"] is None


def test_match_issuer_name_blank_sic_lead_survives_a_later_over_cap_query(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Regression guard for the real `FIRST REPUBLIC BANK/CA` bug caught in Phase 19's
    own real run: a lead found at a narrower, earlier query level ("FIRST REPUBLIC", 2
    candidates) must survive the loop continuing to a broader, later query that hits the
    over-`MAX_CANDIDATES_TO_VALIDATE` cap ("FIRST", 100 candidates) — losing it there
    would silently drop a real research lead just because a less useful query ran after
    the useful one."""
    over_cap = MAX_CANDIDATES_TO_VALIDATE + 1
    submissions = {
        "1132979": {
            "sic": "", "sicDescription": "", "name": "FIRST REPUBLIC BANK",
            "filings": _filings(["2018-05-01"], form="SC 13G"),
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get(
            {
                "FIRST REPUBLIC": _atom(["1132979"]),
                "FIRST": _atom([str(n) for n in range(1, over_cap + 1)]),
            },
            submissions,
        ),
    )

    result = match_issuer_name(_client(tmp_path), "FIRST REPUBLIC BANK/CA", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_AMBIGUOUS
    assert result["blank_sic_lead_cik"] == "1132979"
    assert result["blank_sic_lead_name"] == "FIRST REPUBLIC BANK"


def test_match_issuer_name_no_blank_sic_lead_when_disjoint(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A blank-SIC candidate whose filing history is provably disjoint from the era
    isn't surfaced as a lead either — only real evidence of era overlap counts."""
    atom = _atom(["1"])
    submissions = {
        "1": {
            "sic": "", "sicDescription": "", "name": "Ambiguous Co",
            "filings": _filings(["2005-01-01"], form="SC 13G"),  # disjoint from ERA_SPAN
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", _fake_get({"Ambiguous Co": atom}, submissions)
    )

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["blank_sic_lead_cik"] is None


def test_match_issuer_name_no_blank_sic_lead_when_unknown_activity(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A blank-SIC candidate whose filing history merely brackets the era (no filing
    actually lands inside it) is `ACTIVITY_UNKNOWN`, not `ACTIVITY_PLAUSIBLE` — no lead
    surfaced, same fail-closed posture as every other use of this signal."""
    atom = _atom(["1"])
    submissions = {
        "1": {
            "sic": "", "sicDescription": "", "name": "Ambiguous Co",
            "filings": _filings(["2010-01-01", "2025-01-01"], form="SC 13G"),  # brackets
        }
    }
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", _fake_get({"Ambiguous Co": atom}, submissions)
    )

    result = match_issuer_name(_client(tmp_path), "Ambiguous Co", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["blank_sic_lead_cik"] is None


def test_match_issuer_name_blank_sic_lead_never_becomes_a_match(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Critical regression guard: the module docstring's own motivating case for the
    blank-SIC guard — a shell with zero real filing history — stays fully rejected, no
    lead and no match, since it has no filings at all (ACTIVITY_DISJOINT)."""
    atom = _atom(["1171179"])
    submissions = {"1171179": {"sic": "", "sicDescription": "", "name": "CONFLUENT INC"}}
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        _fake_get({"CONFLUENT INC": atom}, submissions),
    )

    result = match_issuer_name(_client(tmp_path), "CONFLUENT INC-CLASS A", era_span=ERA_SPAN)

    assert result["match_status"] == STATUS_NO_VALIDATED_MATCH
    assert result["matched_cik"] is None
    assert result["blank_sic_lead_cik"] is None
