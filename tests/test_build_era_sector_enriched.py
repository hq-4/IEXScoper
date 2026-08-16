from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from utils.build_era_sector_enriched import SectorConfig, build_era_sector_enriched

ERA_IDENTITY_SCHEMA = {
    "symbol": pl.String,
    "symbol_era_id": pl.String,
    "source_classification": pl.String,
    "trade_rows": pl.Int64,
    "identity_tier": pl.String,
    "identity_issuer": pl.String,
    "identity_entity_id": pl.String,
    "identity_method": pl.String,
    "identity_instrument": pl.String,
    "identity_source_url": pl.String,
}
SEC_TICKER_CIK_SCHEMA = {
    "symbol_era_id": pl.String,
    "sec_cik": pl.String,
    "sec_current_confidence": pl.String,
}


def _write_inputs(tmp_path: Path) -> tuple[Path, Path]:
    era_identity_path = tmp_path / "eras_identity_enriched.parquet"
    sec_path = tmp_path / "symbol_eras_sec_enriched.parquet"
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC", "DDD"],
            "symbol_era_id": ["AAA#001", "BBB#001", "CCC#001", "DDD#001"],
            "source_classification": [
                "delisted_or_acquired_candidate",
                "stable_candidate",
                "delisted_or_acquired_candidate",
                "delisted_or_acquired_candidate",
            ],
            "trade_rows": [1000, 2000, 3000, 4000],
            "identity_tier": ["verified", None, None, None],
            "identity_issuer": ["Alpha Corp", None, None, None],
            "identity_entity_id": ["1512673", None, None, None],
            "identity_method": ["sec_date_scoped_display_names", None, None, None],
            "identity_instrument": ["probable_operating_company", None, None, None],
            "identity_source_url": [
                "https://www.sec.gov/Archives/edgar/data/1512673/x.htm",
                None,
                None,
                None,
            ],
        },
        schema=ERA_IDENTITY_SCHEMA,
    ).write_parquet(era_identity_path)
    pl.DataFrame(
        {
            "symbol_era_id": ["AAA#001", "BBB#001", "CCC#001", "DDD#001"],
            "sec_cik": [None, "0001287865", "0009999999", None],
            "sec_current_confidence": [None, "sec_current_match", "sec_current_match", None],
        },
        schema=SEC_TICKER_CIK_SCHEMA,
    ).write_parquet(sec_path)
    return era_identity_path, sec_path


def test_build_era_sector_enriched_skip_fetch(tmp_path: Path) -> None:
    era_identity_path, sec_path = _write_inputs(tmp_path)
    output_root = tmp_path / "out"

    result = build_era_sector_enriched(
        SectorConfig(
            era_identity_path=era_identity_path,
            sec_ticker_cik_path=sec_path,
            stable_openfigi_path=tmp_path / "no_stable_classes.parquet",
            sec_company_tickers_path=tmp_path / "no_sec_names.parquet",
            edgar_matches_path=tmp_path / "no_edgar_matches.parquet",
            iex_eras_path=tmp_path / "no_iex.parquet",
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            skip_fetch=True,
        )
    )

    enriched = pl.read_parquet(output_root / "eras_sector_enriched.parquet")
    rows = {row["symbol_era_id"]: row for row in enriched.iter_rows(named=True)}

    # AAA and BBB resolve a CIK (Tier A / Tier C); CCC's current match is on a dead-review
    # class so must stay unresolved; DDD has nothing.
    assert rows["AAA#001"]["resolved_cik"] == "1512673"
    assert rows["BBB#001"]["resolved_cik"] == "1287865"
    assert rows["CCC#001"]["resolved_cik"] is None
    assert rows["DDD#001"]["resolved_cik"] is None
    # No fetch happened, so nothing has a SIC/sector yet even where a CIK resolved.
    assert rows["AAA#001"]["sic_coverage_status"] == "cik_no_sic"
    assert rows["CCC#001"]["sic_coverage_status"] == "no_cik"

    assert result["summary"]["total_eras"] == 4
    assert result["summary"]["distinct_ciks_resolved"] == 2
    assert result["summary"]["distinct_ciks_fetched"] == 0
    assert (output_root / "sector_report.md").exists()
    assert (output_root / "cik_sic_lookup.parquet").exists()


def test_build_era_sector_enriched_with_fetch(tmp_path: Path, monkeypatch: Any) -> None:
    era_identity_path, sec_path = _write_inputs(tmp_path)
    output_root = tmp_path / "out"
    calls = []

    class FakeResponse:
        def __init__(self, payload: dict[str, Any]) -> None:
            self.payload = payload
            self.status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return self.payload

    def fake_get(url: str, **_: Any) -> FakeResponse:
        calls.append(url)
        return FakeResponse(
            {"sic": "7372", "sicDescription": "Services-Prepackaged Software", "name": "Alpha Corp"}
        )

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)

    result = build_era_sector_enriched(
        SectorConfig(
            era_identity_path=era_identity_path,
            sec_ticker_cik_path=sec_path,
            stable_openfigi_path=tmp_path / "no_stable_classes.parquet",
            sec_company_tickers_path=tmp_path / "no_sec_names.parquet",
            iex_eras_path=tmp_path / "no_iex.parquet",
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            user_agent="test test@example.test",
            delay_seconds=0,
        )
    )

    enriched = pl.read_parquet(output_root / "eras_sector_enriched.parquet")
    rows = {row["symbol_era_id"]: row for row in enriched.iter_rows(named=True)}

    assert len(calls) == 2  # only the 2 distinct resolved CIKs, not all 4 eras
    assert rows["AAA#001"]["sic"] == "7372"
    assert rows["AAA#001"]["sector_code"] == "I"
    assert rows["AAA#001"]["sector_name"] == "Services"
    assert rows["AAA#001"]["sic_coverage_status"] == "sic_and_sector"
    assert rows["DDD#001"]["sic_coverage_status"] == "no_cik"
    assert result["summary"]["network_requests"] == 2
    assert result["summary"]["cache_hits"] == 0


def test_fund_reclassification_from_stable_openfigi_universe(tmp_path: Path) -> None:
    """BBB is stable_candidate with no resolved SIC path; if the stable-universe
    OpenFIGI classification says it's a fund, it should read fund_no_sic_needed, not
    no_cik — an ETF doesn't need manual sector research, it needs "this is a fund"."""
    era_identity_path, sec_path = _write_inputs(tmp_path)
    stable_path = tmp_path / "stable_era_classes.parquet"
    pl.DataFrame({"symbol_era_id": ["BBB#001"], "openfigi_class": ["fund_etf"]}).write_parquet(
        stable_path
    )
    output_root = tmp_path / "out"

    build_era_sector_enriched(
        SectorConfig(
            era_identity_path=era_identity_path,
            sec_ticker_cik_path=sec_path,
            stable_openfigi_path=stable_path,
            sec_company_tickers_path=tmp_path / "no_sec_names.parquet",
            iex_eras_path=tmp_path / "no_iex.parquet",
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            skip_fetch=True,
        )
    )

    enriched = pl.read_parquet(output_root / "eras_sector_enriched.parquet")
    rows = {row["symbol_era_id"]: row for row in enriched.iter_rows(named=True)}
    # BBB already resolves a CIK via Tier C in this fixture, so it stays cik_no_sic —
    # the fund reclassification only matters once no CIK/SIC resolved at all.
    assert rows["BBB#001"]["instrument_class"] == "fund_etf"
    # CCC has no CIK (dead-review class, Tier C doesn't apply) and no OpenFIGI class at
    # all in this fixture, so it's still genuinely unresolved.
    assert rows["CCC#001"]["sic_coverage_status"] == "no_cik"


def test_fund_no_sic_needed_when_no_cik_and_no_sic(tmp_path: Path) -> None:
    era_identity_path, sec_path = _write_inputs(tmp_path)
    stable_path = tmp_path / "stable_era_classes.parquet"
    # CCC has no automatic CIK path (dead-review class); mark it as a fund.
    pl.DataFrame({"symbol_era_id": ["CCC#001"], "openfigi_class": ["fund_etf"]}).write_parquet(
        stable_path
    )
    output_root = tmp_path / "out"

    build_era_sector_enriched(
        SectorConfig(
            era_identity_path=era_identity_path,
            sec_ticker_cik_path=sec_path,
            stable_openfigi_path=stable_path,
            sec_company_tickers_path=tmp_path / "no_sec_names.parquet",
            iex_eras_path=tmp_path / "no_iex.parquet",
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            skip_fetch=True,
        )
    )

    enriched = pl.read_parquet(output_root / "eras_sector_enriched.parquet")
    rows = {row["symbol_era_id"]: row for row in enriched.iter_rows(named=True)}
    assert rows["CCC#001"]["sic_coverage_status"] == "fund_no_sic_needed"


def test_tier_d_name_match_resolves_a_cik_end_to_end(tmp_path: Path) -> None:
    """EEE has an OpenFIGI-asserted issuer name (FIGI in identity_entity_id, no CIK
    path via Tiers A-C) — Tier D should still resolve it by matching that name against
    SEC's current company list."""
    era_identity_path = tmp_path / "eras_identity_enriched.parquet"
    sec_path = tmp_path / "symbol_eras_sec_enriched.parquet"
    pl.DataFrame(
        {
            "symbol": ["EEE"],
            "symbol_era_id": ["EEE#001"],
            "source_classification": ["delisted_or_acquired_candidate"],
            "trade_rows": [500],
            "identity_tier": ["openfigi_asserted"],
            "identity_issuer": ["Atlantic American Corp"],
            "identity_entity_id": ["BBG000BLNNH6"],
            "identity_method": ["openfigi_symbol_identity"],
            "identity_instrument": ["equity_common"],
            "identity_source_url": [None],
        },
        schema=ERA_IDENTITY_SCHEMA,
    ).write_parquet(era_identity_path)
    pl.DataFrame(
        {"symbol_era_id": ["EEE#001"], "sec_cik": [None], "sec_current_confidence": [None]},
        schema=SEC_TICKER_CIK_SCHEMA,
    ).write_parquet(sec_path)
    sec_names_path = tmp_path / "sec_company_tickers_exchange.parquet"
    pl.DataFrame(
        {"sec_cik": ["0000008177"], "sec_name": ["Atlantic American Corp"], "sec_ticker": ["AAME"]}
    ).write_parquet(sec_names_path)
    output_root = tmp_path / "out"

    build_era_sector_enriched(
        SectorConfig(
            era_identity_path=era_identity_path,
            sec_ticker_cik_path=sec_path,
            stable_openfigi_path=tmp_path / "no_stable_classes.parquet",
            sec_company_tickers_path=sec_names_path,
            iex_eras_path=tmp_path / "no_iex.parquet",
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            skip_fetch=True,
        )
    )

    enriched = pl.read_parquet(output_root / "eras_sector_enriched.parquet")
    row = enriched.filter(pl.col("symbol_era_id") == "EEE#001").to_dicts()[0]
    assert row["resolved_cik"] == "8177"
    assert row["cik_source"] == "sec_name_matched"
    assert row["cik_tier"] == "D"


def test_tier_e_edgar_matches_resolves_a_cik_end_to_end(tmp_path: Path) -> None:
    """FFF has an OpenFIGI-asserted issuer name absent from SEC's *current* company
    list (so Tier D can't fire) but present in
    `utils/build_edgar_company_search_matches.py`'s output — Tier E should resolve it."""
    era_identity_path = tmp_path / "eras_identity_enriched.parquet"
    sec_path = tmp_path / "symbol_eras_sec_enriched.parquet"
    pl.DataFrame(
        {
            "symbol": ["FFF"],
            "symbol_era_id": ["FFF#001"],
            "source_classification": ["delisted_or_acquired_candidate"],
            "trade_rows": [500],
            "identity_tier": ["openfigi_asserted"],
            "identity_issuer": ["Circuit City Stores"],
            "identity_entity_id": ["BBG000BLNNH7"],
            "identity_method": ["openfigi_symbol_identity"],
            "identity_instrument": ["equity_common"],
            "identity_source_url": [None],
        },
        schema=ERA_IDENTITY_SCHEMA,
    ).write_parquet(era_identity_path)
    pl.DataFrame(
        {"symbol_era_id": ["FFF#001"], "sec_cik": [None], "sec_current_confidence": [None]},
        schema=SEC_TICKER_CIK_SCHEMA,
    ).write_parquet(sec_path)
    edgar_matches_path = tmp_path / "edgar_company_search_matches.parquet"
    pl.DataFrame(
        {"identity_issuer": ["Circuit City Stores"], "matched_cik": ["104599"]}
    ).write_parquet(edgar_matches_path)
    output_root = tmp_path / "out"

    build_era_sector_enriched(
        SectorConfig(
            era_identity_path=era_identity_path,
            sec_ticker_cik_path=sec_path,
            stable_openfigi_path=tmp_path / "no_stable_classes.parquet",
            sec_company_tickers_path=tmp_path / "no_sec_names.parquet",
            edgar_matches_path=edgar_matches_path,
            iex_eras_path=tmp_path / "no_iex.parquet",
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            skip_fetch=True,
        )
    )

    enriched = pl.read_parquet(output_root / "eras_sector_enriched.parquet")
    row = enriched.filter(pl.col("symbol_era_id") == "FFF#001").to_dicts()[0]
    assert row["resolved_cik"] == "104599"
    assert row["cik_source"] == "edgar_company_search_matched"
    assert row["cik_tier"] == "E"
    assert row["identity_disproven"] is False  # matches file predates Phase 18, degrades to False


def test_identity_disproven_flows_through_end_to_end(tmp_path: Path) -> None:
    """Phase 18: the real `UTX` shape — a still-unresolved name whose Tier E search
    proved the OpenFIGI-asserted issuer name wrong for the era — should carry
    `identity_disproven=True` all the way into `eras_sector_enriched.parquet`, even
    though `resolved_cik` stays null (this is informational, not a CIK source)."""
    era_identity_path = tmp_path / "eras_identity_enriched.parquet"
    sec_path = tmp_path / "symbol_eras_sec_enriched.parquet"
    pl.DataFrame(
        {
            "symbol": ["UTX"],
            "symbol_era_id": ["UTX#001"],
            "source_classification": ["delisted_or_acquired_candidate"],
            "trade_rows": [919517],
            "identity_tier": ["openfigi_asserted"],
            "identity_issuer": ["ULTRATREX INC-A"],
            "identity_entity_id": ["BBG01X4WM088"],
            "identity_method": ["openfigi_symbol_identity"],
            "identity_instrument": ["equity_common"],
            "identity_source_url": [None],
        },
        schema=ERA_IDENTITY_SCHEMA,
    ).write_parquet(era_identity_path)
    pl.DataFrame(
        {"symbol_era_id": ["UTX#001"], "sec_cik": [None], "sec_current_confidence": [None]},
        schema=SEC_TICKER_CIK_SCHEMA,
    ).write_parquet(sec_path)
    edgar_matches_path = tmp_path / "edgar_company_search_matches.parquet"
    pl.DataFrame(
        {
            "identity_issuer": ["ULTRATREX INC-A"],
            "matched_cik": [None],
            "identity_disproven": [True],
        }
    ).write_parquet(edgar_matches_path)
    output_root = tmp_path / "out"

    build_era_sector_enriched(
        SectorConfig(
            era_identity_path=era_identity_path,
            sec_ticker_cik_path=sec_path,
            stable_openfigi_path=tmp_path / "no_stable_classes.parquet",
            sec_company_tickers_path=tmp_path / "no_sec_names.parquet",
            edgar_matches_path=edgar_matches_path,
            iex_eras_path=tmp_path / "no_iex.parquet",
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            skip_fetch=True,
        )
    )

    enriched = pl.read_parquet(output_root / "eras_sector_enriched.parquet")
    row = enriched.filter(pl.col("symbol_era_id") == "UTX#001").to_dicts()[0]
    assert row["resolved_cik"] is None  # still genuinely unresolved
    assert row["identity_disproven"] is True  # but the name is proven wrong


def test_blank_sic_lead_flows_through_end_to_end(tmp_path: Path) -> None:
    """Phase 19: the real `FIRST REPUBLIC BANK` shape — a still-unresolved name with a
    blank-SIC research lead — should carry `blank_sic_lead_*` all the way into
    `eras_sector_enriched.parquet`, informational only (`resolved_cik` stays null)."""
    era_identity_path = tmp_path / "eras_identity_enriched.parquet"
    sec_path = tmp_path / "symbol_eras_sec_enriched.parquet"
    pl.DataFrame(
        {
            "symbol": ["FRC"],
            "symbol_era_id": ["FRC#001"],
            "source_classification": ["delisted_or_acquired_candidate"],
            "trade_rows": [1859226],
            "identity_tier": ["openfigi_asserted"],
            "identity_issuer": ["FIRST REPUBLIC BANK/CA"],
            "identity_entity_id": ["BBG000BX43M4"],
            "identity_method": ["openfigi_symbol_identity"],
            "identity_instrument": ["equity_common"],
            "identity_source_url": [None],
        },
        schema=ERA_IDENTITY_SCHEMA,
    ).write_parquet(era_identity_path)
    pl.DataFrame(
        {"symbol_era_id": ["FRC#001"], "sec_cik": [None], "sec_current_confidence": [None]},
        schema=SEC_TICKER_CIK_SCHEMA,
    ).write_parquet(sec_path)
    edgar_matches_path = tmp_path / "edgar_company_search_matches.parquet"
    pl.DataFrame(
        {
            "identity_issuer": ["FIRST REPUBLIC BANK/CA"],
            "matched_cik": [None],
            "blank_sic_lead_cik": ["1132979"],
            "blank_sic_lead_name": ["FIRST REPUBLIC BANK"],
            "blank_sic_lead_high_confidence": [False],
        }
    ).write_parquet(edgar_matches_path)
    output_root = tmp_path / "out"

    build_era_sector_enriched(
        SectorConfig(
            era_identity_path=era_identity_path,
            sec_ticker_cik_path=sec_path,
            stable_openfigi_path=tmp_path / "no_stable_classes.parquet",
            sec_company_tickers_path=tmp_path / "no_sec_names.parquet",
            edgar_matches_path=edgar_matches_path,
            iex_eras_path=tmp_path / "no_iex.parquet",
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            skip_fetch=True,
        )
    )

    enriched = pl.read_parquet(output_root / "eras_sector_enriched.parquet")
    row = enriched.filter(pl.col("symbol_era_id") == "FRC#001").to_dicts()[0]
    assert row["resolved_cik"] is None  # still genuinely unresolved
    assert row["blank_sic_lead_cik"] == "1132979"
    assert row["blank_sic_lead_name"] == "FIRST REPUBLIC BANK"
    assert row["blank_sic_lead_high_confidence"] is False


def test_renamed_ticker_resolves_via_iex_fallback_and_flags_continuity(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """GGG stands in for the real GPS -> GAP case: the identity pillar never asserted
    an issuer name (OpenFIGI's ticker-keyed lookup can't find a renamed-away ticker),
    but IEX's own entity snapshot already captured the real name. That name should
    resolve a CIK via Tier D, and since the CIK's *current* SEC ticker differs from this
    era's own symbol, continuity_status should flag it as a rename rather than a real
    delisting."""
    era_identity_path = tmp_path / "eras_identity_enriched.parquet"
    sec_path = tmp_path / "symbol_eras_sec_enriched.parquet"
    pl.DataFrame(
        {
            "symbol": ["GGG"],
            "symbol_era_id": ["GGG#001"],
            "source_classification": ["delisted_or_acquired_candidate"],
            "trade_rows": [4000],
            "identity_tier": [None],
            "identity_issuer": [None],
            "identity_entity_id": [None],
            "identity_method": [None],
            "identity_instrument": [None],
            "identity_source_url": [None],
        },
        schema=ERA_IDENTITY_SCHEMA,
    ).write_parquet(era_identity_path)
    pl.DataFrame(
        {"symbol_era_id": ["GGG#001"], "sec_cik": [None], "sec_current_confidence": [None]},
        schema=SEC_TICKER_CIK_SCHEMA,
    ).write_parquet(sec_path)
    sec_names_path = tmp_path / "sec_company_tickers_exchange.parquet"
    pl.DataFrame(
        {"sec_cik": ["0000039911"], "sec_name": ["Gap Inc"], "sec_ticker": ["GAP"]}
    ).write_parquet(sec_names_path)
    iex_eras_path = tmp_path / "symbol_eras_iex_enriched.parquet"
    pl.DataFrame(
        {"symbol_era_id": ["GGG#001"], "iex_latest_issuer": ["Gap Inc"]}
    ).write_parquet(iex_eras_path)
    output_root = tmp_path / "out"

    class FakeResponse:
        def __init__(self, payload: dict[str, Any]) -> None:
            self.payload = payload
            self.status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return self.payload

    def fake_get(url: str, **_: Any) -> FakeResponse:
        return FakeResponse(
            {"sic": "5651", "sicDescription": "Retail-Family Clothing", "name": "Gap Inc",
             "tickers": ["GAP"], "exchanges": ["NYSE"]}
        )

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)

    build_era_sector_enriched(
        SectorConfig(
            era_identity_path=era_identity_path,
            sec_ticker_cik_path=sec_path,
            stable_openfigi_path=tmp_path / "no_stable_classes.parquet",
            sec_company_tickers_path=sec_names_path,
            iex_eras_path=iex_eras_path,
            output_root=output_root,
            registry_path=tmp_path / "registry.sqlite",
            user_agent="test test@example.test",
            delay_seconds=0,
        )
    )

    enriched = pl.read_parquet(output_root / "eras_sector_enriched.parquet")
    row = enriched.filter(pl.col("symbol_era_id") == "GGG#001").to_dicts()[0]
    assert row["identity_issuer"] == "Gap Inc"
    assert row["identity_issuer_from_iex_fallback"] is True
    assert row["resolved_cik"] == "39911"
    assert row["cik_source"] == "sec_name_matched"
    assert row["continuity_status"] == "renamed_or_successor"
