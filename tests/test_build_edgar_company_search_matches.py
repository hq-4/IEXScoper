from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from utils.build_edgar_company_search_matches import (
    EdgarSearchConfig,
    build_edgar_company_search_matches,
    unresolved_issuer_era_spans,
    unresolved_issuer_names,
    unresolved_issuer_tickers,
)

SEARCH_URL = "https://www.sec.gov/cgi-bin/browse-edgar"

ERAS_SECTOR_ENRICHED_SCHEMA = {
    "symbol_era_id": pl.String,
    "identity_issuer": pl.String,
    "resolved_cik": pl.String,
    "cik_source": pl.String,
    "sic_coverage_status": pl.String,
    "first_day": pl.String,
    "last_day": pl.String,
}


def _write_eras_sector_enriched(tmp_path: Path) -> Path:
    path = tmp_path / "eras_sector_enriched.parquet"
    pl.DataFrame(
        {
            "symbol_era_id": ["AAA#001", "BBB#001", "CCC#001", "DDD#001", "EEE#001"],
            "identity_issuer": ["Alpha Corp", "Beta Fund", "Alpha Corp", None, "Gamma Inc"],
            "resolved_cik": [None, None, "12345", None, None],
            "cik_source": [None, None, "sec_current_ticker_match", None, None],
            "sic_coverage_status": ["no_cik", "fund_no_sic_needed", "sic_and_sector", "no_cik", "no_cik"],
            "first_day": ["20170101", "20180101", "20190101", "20200101", "20210101"],
            "last_day": ["20171231", "20181231", "20191231", "20201231", "20211231"],
        },
        schema=ERAS_SECTOR_ENRICHED_SCHEMA,
    ).write_parquet(path)
    return path


def test_unresolved_issuer_names_dedupes_excludes_resolved_and_funds(tmp_path: Path) -> None:
    path = _write_eras_sector_enriched(tmp_path)

    names = unresolved_issuer_names(path)

    # BBB is a fund (excluded), CCC already has a resolved_cik via Tier C (excluded),
    # DDD has no issuer name at all (excluded); AAA's issuer repeats across two eras and
    # is only counted once.
    assert names == ["Alpha Corp", "Gamma Inc"]


def test_unresolved_issuer_names_reincludes_tier_e_own_prior_matches(tmp_path: Path) -> None:
    """A name Tier E itself resolved on a *previous* run must stay in the search
    population on a rerun — this file gets overwritten each run, and `reconcile_cik`
    rebuilds `cik_source` fresh from it every time, so dropping a previously-Tier-E-
    matched name here would silently erase a real match on the next `reconcile_cik`
    pass, not just skip a redundant search."""
    path = tmp_path / "eras_sector_enriched.parquet"
    pl.DataFrame(
        {
            "symbol_era_id": ["AAA#001", "BBB#001"],
            "identity_issuer": ["Alpha Corp", "Beta Corp"],
            "resolved_cik": ["104599", "999"],
            "cik_source": ["edgar_company_search_matched", "sec_current_ticker_match"],
            "sic_coverage_status": ["sic_and_sector", "sic_and_sector"],
            "first_day": ["20170101", "20180101"],
            "last_day": ["20171231", "20181231"],
        },
        schema=ERAS_SECTOR_ENRICHED_SCHEMA,
    ).write_parquet(path)

    names = unresolved_issuer_names(path)

    # AAA was resolved by Tier E on a prior run — stays in the population so a rerun
    # reproduces it (cheaply, via cache) rather than silently dropping it. BBB was
    # resolved by a different tier (Tier C) — genuinely done, excluded.
    assert names == ["Alpha Corp"]


def test_unresolved_issuer_era_spans_unions_across_eras(tmp_path: Path) -> None:
    path = _write_eras_sector_enriched(tmp_path)

    spans = unresolved_issuer_era_spans(path)

    # "Alpha Corp" spans two unresolved-pool eras (AAA 2017, CCC 2019 -- but CCC already
    # has a resolved_cik via Tier C, so it's excluded from the pool the same way
    # unresolved_issuer_names excludes it): only AAA's 2017 window remains.
    assert spans["Alpha Corp"] == ("2017-01-01", "2017-12-31")
    assert spans["Gamma Inc"] == ("2021-01-01", "2021-12-31")
    # "Beta Fund" is excluded entirely as a fund, same as unresolved_issuer_names.
    assert "Beta Fund" not in spans


def test_unresolved_issuer_era_spans_missing_date_columns_degrades_to_empty(
    tmp_path: Path,
) -> None:
    path = tmp_path / "eras_sector_enriched.parquet"
    pl.DataFrame(
        {
            "symbol_era_id": ["AAA#001"],
            "identity_issuer": ["Alpha Corp"],
            "resolved_cik": [None],
            "cik_source": [None],
            "sic_coverage_status": ["no_cik"],
        },
        schema={
            "symbol_era_id": pl.String,
            "identity_issuer": pl.String,
            "resolved_cik": pl.String,
            "cik_source": pl.String,
            "sic_coverage_status": pl.String,
        },
    ).write_parquet(path)

    assert unresolved_issuer_era_spans(path) == {}


def test_unresolved_issuer_tickers_missing_symbol_column_degrades_to_empty(
    tmp_path: Path,
) -> None:
    # The shared fixture (`_write_eras_sector_enriched`) has no `symbol` column at all.
    path = _write_eras_sector_enriched(tmp_path)

    assert unresolved_issuer_tickers(path) == {}


def test_unresolved_issuer_tickers_maps_unambiguous_names_excludes_shared_ones(
    tmp_path: Path,
) -> None:
    path = tmp_path / "eras_sector_enriched.parquet"
    pl.DataFrame(
        {
            "symbol_era_id": ["AAA#001", "BBB#001", "CCC#001"],
            "symbol": ["AAA", "BBB", "CCC"],
            "identity_issuer": ["Alpha Corp", "Shared Name Corp", "Shared Name Corp"],
            "resolved_cik": [None, None, None],
            "cik_source": [None, None, None],
            "sic_coverage_status": ["no_cik", "no_cik", "no_cik"],
            "first_day": ["20170101", "20180101", "20190101"],
            "last_day": ["20171231", "20181231", "20191231"],
        },
        schema={**ERAS_SECTOR_ENRICHED_SCHEMA, "symbol": pl.String},
    ).write_parquet(path)

    tickers = unresolved_issuer_tickers(path)

    # "Alpha Corp" maps unambiguously to its one symbol; "Shared Name Corp" spans two
    # distinct symbols (BBB, CCC) and is excluded entirely rather than guessing.
    assert tickers == {"Alpha Corp": "AAA"}


class FakeResponse:
    def __init__(self, payload: Any = None, text: str = "") -> None:
        self._payload = payload
        self.text = text
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._payload


def test_build_edgar_company_search_matches_end_to_end(tmp_path: Path, monkeypatch: Any) -> None:
    eras_path = _write_eras_sector_enriched(tmp_path)
    output_root = tmp_path / "out"
    atom = (
        "<feed><entry><content type='text/xml'>"
        "<company-info><cik>0000104599</cik></company-info>"
        "</content></entry></feed>"
    )
    submissions = {
        "sic": "5731",
        "sicDescription": "Retail-Electronics",
        "name": "Alpha Corp",
        # A filing date inside "Alpha Corp"'s own era span (2017) keeps this basic
        # wiring test clear of Phase 16's single-candidate filing-activity guard —
        # dedicated coverage for that guard lives in test_edgar_company_search_match.py.
        "filings": {"recent": {"form": ["10-K"], "filingDate": ["2017-06-15"]}, "files": []},
    }

    def fake_get(url: str, **_: Any) -> FakeResponse:
        if url == SEARCH_URL:
            return FakeResponse(text=atom)
        return FakeResponse(payload=submissions)

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)

    result = build_edgar_company_search_matches(
        EdgarSearchConfig(
            eras_sector_enriched_path=eras_path,
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            user_agent="test test@example.test",
            delay_seconds=0,
        )
    )

    matches = pl.read_parquet(output_root / "edgar_company_search_matches.parquet")
    rows = {row["identity_issuer"]: row for row in matches.iter_rows(named=True)}
    assert rows["Alpha Corp"]["match_status"] == "matched"
    assert rows["Alpha Corp"]["matched_cik"] == "104599"
    assert result["summary"]["total_unresolved_names"] == 2
    assert result["summary"]["names_searched"] == 2
    assert (output_root / "edgar_company_search_summary.json").exists()
    assert (output_root / "edgar_company_search_report.md").exists()


def test_build_edgar_company_search_matches_passes_era_span_through_to_tiebreak(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """End-to-end proof the `unresolved_issuer_era_spans` plumbing actually reaches
    `match_issuer_name`: "Gamma Inc" (era span 2021-01-01..2021-12-31 per the fixture)
    ties between two name-validating candidates, resolvable only via filing activity
    overlapping that span."""
    eras_path = _write_eras_sector_enriched(tmp_path)
    output_root = tmp_path / "out"
    tied_atom = (
        "<feed>"
        "<entry><content type='text/xml'><company-info><cik>0000000001</cik></company-info></content></entry>"
        "<entry><content type='text/xml'><company-info><cik>0000000002</cik></company-info></content></entry>"
        "</feed>"
    )
    submissions = {
        "1": {
            "sic": "1000", "sicDescription": "A", "name": "Gamma Inc",
            "filings": {"recent": {"filingDate": ["2005-01-01"]}, "files": []},  # disjoint
        },
        "2": {
            "sic": "2000", "sicDescription": "B", "name": "Gamma Inc",
            "filings": {"recent": {"filingDate": ["2021-06-01"]}, "files": []},  # inside the era
        },
    }

    def fake_get(url: str, **kwargs: Any) -> FakeResponse:
        if url == SEARCH_URL:
            return FakeResponse(text=tied_atom)
        cik = url.rsplit("CIK", 1)[1].split(".")[0].lstrip("0") or "0"
        return FakeResponse(payload=submissions.get(cik))

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)

    result = build_edgar_company_search_matches(
        EdgarSearchConfig(
            eras_sector_enriched_path=eras_path,
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            user_agent="test test@example.test",
            delay_seconds=0,
        )
    )

    matches = pl.read_parquet(output_root / "edgar_company_search_matches.parquet")
    rows = {row["identity_issuer"]: row for row in matches.iter_rows(named=True)}
    assert rows["Gamma Inc"]["match_status"] == "matched"
    assert rows["Gamma Inc"]["matched_cik"] == "2"
    assert rows["Gamma Inc"]["match_basis"] == "filing_activity_tiebreak"
    assert result["summary"]["match_basis_counts"]["filing_activity_tiebreak"] == 1


def test_build_edgar_company_search_matches_limit_names(tmp_path: Path, monkeypatch: Any) -> None:
    eras_path = _write_eras_sector_enriched(tmp_path)
    output_root = tmp_path / "out"
    calls = []

    def fake_get(url: str, **_: Any) -> FakeResponse:
        calls.append(url)
        return FakeResponse(text="<feed></feed>")

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)

    result = build_edgar_company_search_matches(
        EdgarSearchConfig(
            eras_sector_enriched_path=eras_path,
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            user_agent="test test@example.test",
            delay_seconds=0,
            limit_names=1,
        )
    )

    # "Alpha Corp" truncates to "Alpha" (2 words -> 1-word floor) before giving up, so an
    # always-empty search makes 2 calls for this one name — `names_searched` below is the
    # real signal that `--limit-names 1` only processed one name, not the call count.
    assert len(calls) == 2
    assert result["summary"]["total_unresolved_names"] == 2
    assert result["summary"]["names_searched"] == 1


def test_build_edgar_company_search_matches_skip_fetch(tmp_path: Path) -> None:
    eras_path = _write_eras_sector_enriched(tmp_path)
    output_root = tmp_path / "out"

    result = build_edgar_company_search_matches(
        EdgarSearchConfig(
            eras_sector_enriched_path=eras_path,
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            skip_fetch=True,
        )
    )

    assert result["summary"]["names_searched"] == 0
    matches = pl.read_parquet(output_root / "edgar_company_search_matches.parquet")
    assert matches.height == 0


def test_build_edgar_company_search_matches_missing_input_raises(tmp_path: Path) -> None:
    try:
        build_edgar_company_search_matches(
            EdgarSearchConfig(
                eras_sector_enriched_path=tmp_path / "missing.parquet",
                output_root=tmp_path / "out",
                registry_path=tmp_path / "registry.sqlite",
            )
        )
        raise AssertionError("expected FileNotFoundError")
    except FileNotFoundError:
        pass
