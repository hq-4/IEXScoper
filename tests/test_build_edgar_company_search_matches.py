from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from utils.build_edgar_company_search_matches import (
    EdgarSearchConfig,
    build_edgar_company_search_matches,
    unresolved_issuer_names,
)

SEARCH_URL = "https://www.sec.gov/cgi-bin/browse-edgar"

ERAS_SECTOR_ENRICHED_SCHEMA = {
    "symbol_era_id": pl.String,
    "identity_issuer": pl.String,
    "resolved_cik": pl.String,
    "cik_source": pl.String,
    "sic_coverage_status": pl.String,
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
        },
        schema=ERAS_SECTOR_ENRICHED_SCHEMA,
    ).write_parquet(path)

    names = unresolved_issuer_names(path)

    # AAA was resolved by Tier E on a prior run — stays in the population so a rerun
    # reproduces it (cheaply, via cache) rather than silently dropping it. BBB was
    # resolved by a different tier (Tier C) — genuinely done, excluded.
    assert names == ["Alpha Corp"]


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
    submissions = {"sic": "5731", "sicDescription": "Retail-Electronics", "name": "Alpha Corp"}

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
