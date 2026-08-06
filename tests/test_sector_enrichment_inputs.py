from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.sector_enrichment_inputs import load_edgar_matches, load_name_matches, load_stable_classes

ERA_IDENTITY_SCHEMA = {
    "symbol_era_id": pl.String,
    "identity_tier": pl.String,
    "identity_issuer": pl.String,
    "identity_entity_id": pl.String,
    "identity_method": pl.String,
}


def test_load_name_matches_missing_file_returns_none(tmp_path: Path) -> None:
    era_identity = pl.DataFrame(schema=ERA_IDENTITY_SCHEMA)
    assert load_name_matches(tmp_path / "missing.parquet", era_identity) is None


def test_load_stable_classes_missing_file_returns_empty_frame(tmp_path: Path) -> None:
    result = load_stable_classes(tmp_path / "missing.parquet")
    assert result.height == 0
    assert result.columns == ["symbol_era_id", "stable_openfigi_class"]


def test_load_edgar_matches_missing_file_returns_none(tmp_path: Path) -> None:
    assert load_edgar_matches(tmp_path / "missing.parquet") is None


def test_load_edgar_matches_reads_issuer_and_cik_columns(tmp_path: Path) -> None:
    path = tmp_path / "edgar_company_search_matches.parquet"
    pl.DataFrame(
        {
            "identity_issuer": ["Circuit City Stores"],
            "match_status": ["matched"],
            "matched_cik": ["104599"],
            "candidate_count": [1],
            "candidate_name": ["CIRCUIT CITY STORES INC"],
            "sic": ["5731"],
            "sic_description": ["Retail-Electronics"],
        }
    ).write_parquet(path)

    result = load_edgar_matches(path)

    assert result is not None
    assert result.columns == ["identity_issuer", "matched_cik"]
    assert result.to_dicts() == [
        {"identity_issuer": "Circuit City Stores", "matched_cik": "104599"}
    ]
