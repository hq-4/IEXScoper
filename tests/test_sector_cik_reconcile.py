from __future__ import annotations

import polars as pl
import pytest

from utils.sector_cik_reconcile import (
    CIK_SOURCE_CURRENT_MATCH,
    CIK_SOURCE_LEGACY_URL,
    CIK_SOURCE_NONE,
    CIK_SOURCE_SEC_DATE_SCOPED,
    distinct_ciks,
    reconcile_cik,
    require_columns,
)


ERA_IDENTITY_SCHEMA = {
    "symbol_era_id": pl.String,
    "source_classification": pl.String,
    "identity_tier": pl.String,
    "identity_method": pl.String,
    "identity_entity_id": pl.String,
    "identity_source_url": pl.String,
}
SEC_TICKER_CIK_SCHEMA = {
    "symbol_era_id": pl.String,
    "sec_cik": pl.String,
    "sec_current_confidence": pl.String,
}


def _era_identity(**overrides) -> pl.DataFrame:
    base = {
        "symbol_era_id": ["X#001"],
        "source_classification": ["delisted_or_acquired_candidate"],
        "identity_tier": [None],
        "identity_method": [None],
        "identity_entity_id": [None],
        "identity_source_url": [None],
    }
    base.update(overrides)
    return pl.DataFrame(base, schema=ERA_IDENTITY_SCHEMA)


def _sec_ticker_cik(**overrides) -> pl.DataFrame:
    base = {
        "symbol_era_id": ["X#001"],
        "sec_cik": [None],
        "sec_current_confidence": [None],
    }
    base.update(overrides)
    return pl.DataFrame(base, schema=SEC_TICKER_CIK_SCHEMA)


def _resolve_one(era_identity: pl.DataFrame, sec: pl.DataFrame) -> dict:
    return reconcile_cik(era_identity, sec).to_dicts()[0]


def test_tier_a_sec_date_scoped_verified_wins() -> None:
    row = _resolve_one(
        _era_identity(
            identity_tier=["verified"],
            identity_method=["sec_date_scoped_display_names"],
            identity_entity_id=["123456"],
        ),
        _sec_ticker_cik(sec_cik=["0000999999"], sec_current_confidence=["sec_current_match"]),
    )
    assert row["resolved_cik"] == "123456"
    assert row["cik_source"] == CIK_SOURCE_SEC_DATE_SCOPED
    assert row["cik_tier"] == "A"


def test_tier_b_legacy_override_recovers_cik_from_url() -> None:
    row = _resolve_one(
        _era_identity(
            identity_tier=["verified"],
            identity_method=["legacy_historical_override"],
            identity_entity_id=[""],
            identity_source_url=["https://www.sec.gov/Archives/edgar/data/907654/x.htm"],
        ),
        _sec_ticker_cik(),
    )
    assert row["resolved_cik"] == "907654"
    assert row["cik_source"] == CIK_SOURCE_LEGACY_URL
    assert row["cik_tier"] == "B"


def test_tier_b_no_recoverable_cik_in_url_yields_none() -> None:
    row = _resolve_one(
        _era_identity(
            identity_tier=["verified"],
            identity_method=["legacy_historical_override"],
            identity_entity_id=[""],
            identity_source_url=["local:parent-root-current-evidence"],
        ),
        _sec_ticker_cik(),
    )
    assert row["resolved_cik"] is None
    assert row["cik_source"] == CIK_SOURCE_NONE
    assert row["cik_tier"] is None


def test_tier_c_current_match_applies_to_stable_candidate() -> None:
    row = _resolve_one(
        _era_identity(source_classification=["stable_candidate"]),
        _sec_ticker_cik(sec_cik=["0000555555"], sec_current_confidence=["sec_current_match"]),
    )
    assert row["resolved_cik"] == "555555"
    assert row["cik_source"] == CIK_SOURCE_CURRENT_MATCH
    assert row["cik_tier"] == "C"


def test_tier_c_current_match_applies_to_ipo_or_new_listing() -> None:
    row = _resolve_one(
        _era_identity(source_classification=["ipo_or_new_listing_candidate"]),
        _sec_ticker_cik(sec_cik=["0000555555"], sec_current_confidence=["sec_current_match"]),
    )
    assert row["resolved_cik"] == "555555"
    assert row["cik_tier"] == "C"


@pytest.mark.parametrize(
    "dead_class",
    [
        "delisted_or_acquired_candidate",
        "intermittent_or_reused_candidate",
        "intermittent_full_window_candidate",
        "partial_window_candidate",
    ],
)
def test_tier_c_never_applies_to_dead_ticker_review_classes(dead_class: str) -> None:
    """The scope-boundary guarantee this whole module exists to enforce: a current
    ticker match on a historically dead symbol is very likely a *different* company
    that reused the ticker, so it must never be treated as that dead era's identity."""
    row = _resolve_one(
        _era_identity(source_classification=[dead_class]),
        _sec_ticker_cik(sec_cik=["0000555555"], sec_current_confidence=["sec_current_match"]),
    )
    assert row["resolved_cik"] is None
    assert row["cik_source"] == CIK_SOURCE_NONE
    assert row["cik_tier"] is None


def test_tier_c_requires_exact_current_match_not_multiple() -> None:
    row = _resolve_one(
        _era_identity(source_classification=["stable_candidate"]),
        _sec_ticker_cik(
            sec_cik=["0000555555"], sec_current_confidence=["sec_multiple_current_matches"]
        ),
    )
    assert row["resolved_cik"] is None
    assert row["cik_source"] == CIK_SOURCE_NONE


def test_corroborated_and_openfigi_asserted_have_no_automatic_cik_path() -> None:
    """These tiers carry a Bloomberg FIGI in identity_entity_id, not a CIK — must never
    be mistaken for one even though the column holds a non-null string."""
    row = _resolve_one(
        _era_identity(
            identity_tier=["openfigi_asserted"],
            identity_method=["openfigi_symbol_identity"],
            identity_entity_id=["BBG000BLNNH6"],
        ),
        _sec_ticker_cik(),
    )
    assert row["resolved_cik"] is None
    assert row["cik_source"] == CIK_SOURCE_NONE


def test_all_null_row_resolves_cleanly() -> None:
    row = _resolve_one(_era_identity(), _sec_ticker_cik())
    assert row["resolved_cik"] is None
    assert row["cik_source"] == CIK_SOURCE_NONE
    assert row["cik_tier"] is None


def test_sec_cik_padding_is_normalized_to_unpadded() -> None:
    row = _resolve_one(
        _era_identity(source_classification=["stable_candidate"]),
        _sec_ticker_cik(sec_cik=["0001090872"], sec_current_confidence=["sec_current_match"]),
    )
    assert row["resolved_cik"] == "1090872"
    assert not row["resolved_cik"].startswith("0")


def test_distinct_ciks_dedupes_and_sorts() -> None:
    reconciled = pl.DataFrame(
        {
            "symbol_era_id": ["A#001", "B#001", "C#001", "D#001"],
            "resolved_cik": ["999", "111", "111", None],
            "cik_source": [
                CIK_SOURCE_SEC_DATE_SCOPED,
                CIK_SOURCE_CURRENT_MATCH,
                CIK_SOURCE_CURRENT_MATCH,
                CIK_SOURCE_NONE,
            ],
            "cik_tier": ["A", "C", "C", None],
        }
    )
    assert distinct_ciks(reconciled) == ["111", "999"]


def test_require_columns_raises_on_missing() -> None:
    with pytest.raises(ValueError, match="missing required columns"):
        require_columns(pl.DataFrame({"symbol_era_id": ["A#001"]}), ("symbol_era_id", "sec_cik"))
