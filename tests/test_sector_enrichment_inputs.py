from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.sector_enrichment_inputs import (
    apply_iex_fallback_issuer,
    load_edgar_matches,
    load_iex_fallback_names,
    load_name_matches,
    load_stable_classes,
)

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


def test_load_iex_fallback_names_missing_file_returns_none(tmp_path: Path) -> None:
    assert load_iex_fallback_names(tmp_path / "missing.parquet") is None


def test_load_iex_fallback_names_filters_nulls_and_renames(tmp_path: Path) -> None:
    path = tmp_path / "symbol_eras_iex_enriched.parquet"
    pl.DataFrame(
        {
            "symbol_era_id": ["BK#001", "ZZZ#001"],
            "iex_latest_issuer": ["BANK OF NEW YORK MELLON CORP", None],
            "iex_entity_confidence": ["current_iex_only_evidence", "iex_snapshot_unmatched"],
        }
    ).write_parquet(path)

    result = load_iex_fallback_names(path)

    assert result is not None
    assert result.to_dicts() == [
        {"symbol_era_id": "BK#001", "iex_fallback_issuer": "BANK OF NEW YORK MELLON CORP"}
    ]


def test_apply_iex_fallback_issuer_none_fallback_adds_false_flag() -> None:
    era_identity = pl.DataFrame(
        {"symbol_era_id": ["AAA#001"], "identity_issuer": [None]},
        schema={"symbol_era_id": pl.String, "identity_issuer": pl.String},
    )
    result = apply_iex_fallback_issuer(era_identity, None)
    assert result["identity_issuer"].to_list() == [None]
    assert result["identity_issuer_from_iex_fallback"].to_list() == [False]


def test_apply_iex_fallback_issuer_backfills_only_null_rows() -> None:
    era_identity = pl.DataFrame(
        {
            "symbol_era_id": ["BK#001", "AAPL#001"],
            "identity_issuer": [None, "Apple Inc"],
        },
        schema={"symbol_era_id": pl.String, "identity_issuer": pl.String},
    )
    fallback = pl.DataFrame(
        {
            "symbol_era_id": ["BK#001", "AAPL#001"],
            "iex_fallback_issuer": ["BANK OF NEW YORK MELLON CORP", "WRONG NAME SHOULD NOT WIN"],
        }
    )

    result = apply_iex_fallback_issuer(era_identity, fallback)
    rows = {row["symbol_era_id"]: row for row in result.iter_rows(named=True)}

    assert rows["BK#001"]["identity_issuer"] == "BANK OF NEW YORK MELLON CORP"
    assert rows["BK#001"]["identity_issuer_from_iex_fallback"] is True
    # Apple already had a real identity_issuer — the fallback must never overwrite it.
    assert rows["AAPL#001"]["identity_issuer"] == "Apple Inc"
    assert rows["AAPL#001"]["identity_issuer_from_iex_fallback"] is False
