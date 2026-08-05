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
