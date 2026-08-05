"""Reconcile the three unreconciled CIK sources in this repo into one best-CIK-per-era
table, confidence-tiered, with strict scoping so a current-listing ticker match is never
used as a stand-in for historical identity on a dead ticker.

The three sources, highest confidence first:

- **Tier A** — `data/resolution/identity_facts.jsonl` rows with `verification_state=
  verified` and `evidence_method=sec_date_scoped_display_names`: a real, date-scoped
  CIK straight from the live SEC-lane resolver, in `identity_entity_id`.
- **Tier B** — same file, `evidence_method=legacy_historical_override`: `identity_entity_id`
  was migrated empty, but the CIK is usually recoverable from the `identity_source_url`
  EDGAR archive URL via `utils.sec_identity_evidence.parse_cik_from_archive_url`.
- **Tier C** — `reports/sec-ticker-cik/symbol_eras_sec_enriched.parquet.sec_cik`: a
  current-ticker-symbol match, current-listing-biased. Only trustworthy for eras whose
  ticker is still trading under the same identity today — restricted to
  `stable_candidate`/`ipo_or_new_listing_candidate` and never applied to the four
  dead-ticker review classes, where a "current" match is very likely a *different*
  company that reused the symbol.

Every fact-derived identity fact carries no CIK at all past these three sources (the
bulk of the OpenFIGI pillar's dead-ticker coverage — `corroborated`/`openfigi_asserted`
identity facts — has a Bloomberg FIGI in `identity_entity_id`, not a CIK), so an era with
none of Tiers A/B/C resolves to no CIK rather than a fuzzy name-based guess. [CA][IV][KBT]
"""

from __future__ import annotations

import polars as pl

from utils.sec_identity_evidence import parse_cik_from_archive_url

ACTIVE_CLASSES = ("stable_candidate", "ipo_or_new_listing_candidate")

CIK_SOURCE_SEC_DATE_SCOPED = "sec_date_scoped_display_names"
CIK_SOURCE_LEGACY_URL = "legacy_historical_override_url_derived"
CIK_SOURCE_CURRENT_MATCH = "sec_current_ticker_match"
CIK_SOURCE_NONE = "no_cik_available"

CIK_TIER = {
    CIK_SOURCE_SEC_DATE_SCOPED: "A",
    CIK_SOURCE_LEGACY_URL: "B",
    CIK_SOURCE_CURRENT_MATCH: "C",
}

REQUIRED_IDENTITY_COLUMNS = (
    "symbol_era_id",
    "source_classification",
    "identity_tier",
    "identity_method",
    "identity_entity_id",
    "identity_source_url",
)
REQUIRED_SEC_COLUMNS = ("symbol_era_id", "sec_cik", "sec_current_confidence")


def reconcile_cik(era_identity: pl.DataFrame, sec_ticker_cik: pl.DataFrame) -> pl.DataFrame:
    """One row per era: `symbol_era_id, resolved_cik, cik_source, cik_tier`. Both CIK
    representations (unpadded `identity_entity_id`, zero-padded `sec_cik`) are
    normalized to unpadded strings before comparison."""
    require_columns(era_identity, REQUIRED_IDENTITY_COLUMNS)
    require_columns(sec_ticker_cik, REQUIRED_SEC_COLUMNS)
    # Cast defensively: a caller-built frame with an all-null column (e.g. an empty or
    # single-row test fixture) infers Null dtype, not String, and the string ops below
    # would fail on it even though real parquet input never has this problem.
    joined = (
        era_identity.select(list(REQUIRED_IDENTITY_COLUMNS))
        .cast(dict.fromkeys(REQUIRED_IDENTITY_COLUMNS[1:], pl.String))
        .join(
            sec_ticker_cik.select(list(REQUIRED_SEC_COLUMNS)).cast(
                dict.fromkeys(REQUIRED_SEC_COLUMNS[1:], pl.String)
            ),
            on="symbol_era_id",
            how="left",
        )
    )
    joined = joined.with_columns(_legacy_url_cik_expr().alias("_legacy_cik"))
    resolved = joined.with_columns(_resolved_cik_expr().alias("resolved_cik")).with_columns(
        _cik_source_expr().alias("cik_source")
    )
    resolved = resolved.with_columns(
        pl.col("cik_source")
        .replace_strict(CIK_TIER, default=None, return_dtype=pl.String)
        .alias("cik_tier")
    )
    return resolved.select("symbol_era_id", "resolved_cik", "cik_source", "cik_tier")


def distinct_ciks(reconciled: pl.DataFrame) -> list[str]:
    """Sorted, deduped list of every resolved CIK — the fetch list for `sec_sic_client`,
    which is much smaller than the era count since many eras share an issuer."""
    return sorted(reconciled.filter(pl.col("resolved_cik").is_not_null())["resolved_cik"].unique())


def require_columns(frame: pl.DataFrame, columns: tuple[str, ...]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"frame missing required columns: {missing}")


def _legacy_url_cik_expr() -> pl.Expr:
    is_legacy = (pl.col("identity_method") == "legacy_historical_override").fill_null(False)
    parsed = pl.col("identity_source_url").map_elements(
        parse_cik_from_archive_url, return_dtype=pl.String
    )
    return pl.when(is_legacy).then(parsed).otherwise(None)


def _tier_a_expr() -> pl.Expr:
    return (
        (pl.col("identity_tier") == "verified").fill_null(False)
        & (pl.col("identity_method") == CIK_SOURCE_SEC_DATE_SCOPED).fill_null(False)
        & pl.col("identity_entity_id").str.contains(r"^\d+$").fill_null(False)
    )


def _tier_b_expr() -> pl.Expr:
    return (
        (pl.col("identity_tier") == "verified").fill_null(False)
        & (pl.col("identity_method") == "legacy_historical_override").fill_null(False)
        & pl.col("_legacy_cik").is_not_null()
        & (pl.col("_legacy_cik") != "")
    )


def _tier_c_expr() -> pl.Expr:
    return (
        pl.col("source_classification").is_in(list(ACTIVE_CLASSES)).fill_null(False)
        & (pl.col("sec_current_confidence") == "sec_current_match").fill_null(False)
        & pl.col("sec_cik").is_not_null()
    )


def _resolved_cik_expr() -> pl.Expr:
    return (
        pl.when(_tier_a_expr())
        .then(pl.col("identity_entity_id").str.strip_chars_start("0"))
        .when(_tier_b_expr())
        .then(pl.col("_legacy_cik"))
        .when(_tier_c_expr())
        .then(pl.col("sec_cik").str.strip_chars_start("0"))
        .otherwise(None)
    )


def _cik_source_expr() -> pl.Expr:
    return (
        pl.when(_tier_a_expr())
        .then(pl.lit(CIK_SOURCE_SEC_DATE_SCOPED))
        .when(_tier_b_expr())
        .then(pl.lit(CIK_SOURCE_LEGACY_URL))
        .when(_tier_c_expr())
        .then(pl.lit(CIK_SOURCE_CURRENT_MATCH))
        .otherwise(pl.lit(CIK_SOURCE_NONE))
    )
