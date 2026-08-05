"""Reconcile the unreconciled CIK sources in this repo into one best-CIK-per-era
table, confidence-tiered, with strict scoping so a current-listing ticker match is never
used as a stand-in for historical identity on a dead ticker.

The sources, highest confidence first:

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
- **Tier D** — `utils.sec_name_cik_lookup`: an unambiguous exact-normalized-name match
  between an era's `identity_issuer` (from a `corroborated`/`openfigi_asserted` identity
  fact — Tiers A-C never touch these, since their `identity_entity_id` is a Bloomberg
  FIGI, not a CIK) and SEC's current company-name list. Unlike Tier C this applies to
  *any* class, dead-ticker ones included: a company keeps roughly the same name even
  after its old ticker gets reused by someone else, so a name match doesn't carry the
  same "wrong company" risk a current-ticker match does. Ambiguous names (two distinct
  CIKs normalizing the same) are excluded upstream in `sec_name_cik_lookup`, so this
  tier never guesses between candidates.

An era with none of Tiers A-D resolves to no CIK rather than a further guess. [CA][IV][KBT]
"""

from __future__ import annotations

import polars as pl

from utils.sec_identity_evidence import parse_cik_from_archive_url

ACTIVE_CLASSES = ("stable_candidate", "ipo_or_new_listing_candidate")

CIK_SOURCE_SEC_DATE_SCOPED = "sec_date_scoped_display_names"
CIK_SOURCE_LEGACY_URL = "legacy_historical_override_url_derived"
CIK_SOURCE_CURRENT_MATCH = "sec_current_ticker_match"
CIK_SOURCE_NAME_MATCHED = "sec_name_matched"
CIK_SOURCE_NONE = "no_cik_available"

CIK_TIER = {
    CIK_SOURCE_SEC_DATE_SCOPED: "A",
    CIK_SOURCE_LEGACY_URL: "B",
    CIK_SOURCE_CURRENT_MATCH: "C",
    CIK_SOURCE_NAME_MATCHED: "D",
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


def reconcile_cik(
    era_identity: pl.DataFrame,
    sec_ticker_cik: pl.DataFrame,
    name_matches: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """One row per era: `symbol_era_id, resolved_cik, cik_source, cik_tier`. Both CIK
    representations (unpadded `identity_entity_id`, zero-padded `sec_cik`) are
    normalized to unpadded strings before comparison. `name_matches` is optional
    (`symbol_era_id`, `name_matched_cik`) — see `utils.sec_name_cik_lookup.match_by_name`;
    omitting it just means Tier D never fires."""
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
        .join(_name_matches_or_empty(name_matches), on="symbol_era_id", how="left")
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


def _name_matches_or_empty(name_matches: pl.DataFrame | None) -> pl.DataFrame:
    if name_matches is None or not name_matches.height:
        return pl.DataFrame(schema={"symbol_era_id": pl.String, "name_matched_cik": pl.String})
    require_columns(name_matches, ("symbol_era_id", "name_matched_cik"))
    return name_matches.select("symbol_era_id", "name_matched_cik").cast(
        {"name_matched_cik": pl.String}
    )


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


def _tier_d_expr() -> pl.Expr:
    return pl.col("name_matched_cik").is_not_null()


def _resolved_cik_expr() -> pl.Expr:
    return (
        pl.when(_tier_a_expr())
        .then(pl.col("identity_entity_id").str.strip_chars_start("0"))
        .when(_tier_b_expr())
        .then(pl.col("_legacy_cik"))
        .when(_tier_c_expr())
        .then(pl.col("sec_cik").str.strip_chars_start("0"))
        .when(_tier_d_expr())
        .then(pl.col("name_matched_cik"))
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
        .when(_tier_d_expr())
        .then(pl.lit(CIK_SOURCE_NAME_MATCHED))
        .otherwise(pl.lit(CIK_SOURCE_NONE))
    )
