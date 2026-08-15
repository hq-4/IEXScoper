"""Optional side-inputs for `utils/build_era_sector_enriched.py`. Both degrade
gracefully to "no extra coverage" rather than failing when their source file doesn't
exist yet, since neither changes the correctness of what the orchestrator already
computes — they only add more CIK/instrument-class coverage on top. [CA][CDiP]
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.sec_name_cik_lookup import build_name_cik_index, match_by_name


def load_name_matches(
    sec_company_tickers_path: Path, era_identity: pl.DataFrame
) -> pl.DataFrame | None:
    """Tier D (name-matched CIKs, see `utils.sector_cik_reconcile`) is skipped, not an
    error, when the SEC ticker/name table isn't available."""
    if not sec_company_tickers_path.exists():
        return None
    sec_tickers = pl.read_parquet(sec_company_tickers_path)
    name_index = build_name_cik_index(sec_tickers)
    return match_by_name(era_identity, name_index)


def load_stable_classes(path: Path) -> pl.DataFrame:
    """The OpenFIGI classification for `stable_candidate`/`ipo_or_new_listing_candidate`
    eras (see `utils/build_openfigi_stable_universe.py`). Missing just means
    `instrument_class` falls back to whatever `identity_instrument` already has."""
    schema = {"symbol_era_id": pl.String, "stable_openfigi_class": pl.String}
    if not path.exists():
        return pl.DataFrame(schema=schema)
    return pl.read_parquet(path).select(
        "symbol_era_id", pl.col("openfigi_class").cast(pl.String).alias("stable_openfigi_class")
    )


EDGAR_METADATA_COLUMNS = (
    "identity_disproven",
    "blank_sic_lead_cik",
    "blank_sic_lead_name",
    "blank_sic_lead_high_confidence",
)
EDGAR_METADATA_DEFAULTS = {
    "identity_disproven": False,
    "blank_sic_lead_cik": None,
    "blank_sic_lead_name": None,
    "blank_sic_lead_high_confidence": False,
}


def load_edgar_matches(path: Path) -> pl.DataFrame | None:
    """Tier E (EDGAR company-search-matched CIKs, see `utils.sector_cik_reconcile`) is
    skipped, not an error, when `utils/build_edgar_company_search_matches.py` hasn't
    been run yet. Carries `EDGAR_METADATA_COLUMNS` (Phase 18/19) alongside the
    CIK-matching columns purely as informational metadata for `apply_identity_disproven`/
    `apply_blank_sic_lead` below — `reconcile_cik` re-selects only the columns it needs,
    so these extra columns never reach or affect CIK resolution. Degrades to each
    column's default when reading an older matches file built before it existed, rather
    than failing."""
    if not path.exists():
        return None
    frame = pl.read_parquet(path)
    missing = [column for column in EDGAR_METADATA_COLUMNS if column not in frame.columns]
    if missing:
        frame = frame.with_columns(
            pl.lit(EDGAR_METADATA_DEFAULTS[column]).alias(column) for column in missing
        )
    return frame.select("identity_issuer", "matched_cik", *EDGAR_METADATA_COLUMNS)


def apply_identity_disproven(
    era_identity: pl.DataFrame, edgar_matches: pl.DataFrame | None
) -> pl.DataFrame:
    """Backfills `identity_disproven` (Phase 18) onto every era sharing a Tier-E-searched
    `identity_issuer`, `False` when no EDGAR search ever ran for that name (never
    searched means never disproven, not "assumed fine"). Purely informational — doesn't
    touch `resolved_cik`/`cik_source`, so it can't change what gets a CIK, only how a
    still-unresolved name is presented to a human researcher."""
    if edgar_matches is None or not edgar_matches.height:
        return era_identity.with_columns(pl.lit(False).alias("identity_disproven"))
    lookup = edgar_matches.select("identity_issuer", "identity_disproven").unique(
        subset="identity_issuer", keep="first"
    )
    joined = era_identity.join(lookup, on="identity_issuer", how="left")
    return joined.with_columns(pl.col("identity_disproven").fill_null(False))


def apply_blank_sic_lead(
    era_identity: pl.DataFrame, edgar_matches: pl.DataFrame | None
) -> pl.DataFrame:
    """Backfills the Phase 19 `blank_sic_lead_*` research-lead fields onto every era
    sharing a Tier-E-searched `identity_issuer` — `None`/`False` when no EDGAR search
    ever ran, or it ran but found no lead. Purely informational, same "never touches
    `resolved_cik`/`cik_source`" contract as `apply_identity_disproven`."""
    lead_columns = ("blank_sic_lead_cik", "blank_sic_lead_name", "blank_sic_lead_high_confidence")
    if edgar_matches is None or not edgar_matches.height:
        return era_identity.with_columns(
            pl.lit(None, dtype=pl.String).alias("blank_sic_lead_cik"),
            pl.lit(None, dtype=pl.String).alias("blank_sic_lead_name"),
            pl.lit(False).alias("blank_sic_lead_high_confidence"),
        )
    lookup = edgar_matches.select("identity_issuer", *lead_columns).unique(
        subset="identity_issuer", keep="first"
    )
    joined = era_identity.join(lookup, on="identity_issuer", how="left")
    return joined.with_columns(pl.col("blank_sic_lead_high_confidence").fill_null(False))


def load_iex_fallback_names(path: Path) -> pl.DataFrame | None:
    """`iex_latest_issuer` from `utils/build_iex_entity_enrichment.py`'s output — a real
    issuer name IEX's own entity snapshots captured, already sitting unused for eras
    where the identity pillar came back empty. This mostly rescues names OpenFIGI's
    ticker-keyed lookup structurally cannot find: once a ticker is renamed away (e.g.
    `BK` -> `BNY`), querying the *old* ticker string returns zero FIGI matches, the same
    current-listing bias Tier C already has, just one layer upstream. Missing file just
    means this fallback contributes nothing."""
    if not path.exists():
        return None
    return (
        pl.read_parquet(path)
        .select("symbol_era_id", "iex_latest_issuer")
        .filter(pl.col("iex_latest_issuer").is_not_null())
        .rename({"iex_latest_issuer": "iex_fallback_issuer"})
    )


def apply_iex_fallback_issuer(
    era_identity: pl.DataFrame, iex_fallback: pl.DataFrame | None
) -> pl.DataFrame:
    """Backfills `identity_issuer` from the IEX fallback only where the identity pillar
    left it null — never overwrites a real OpenFIGI/SEC-asserted name. Adds
    `identity_issuer_from_iex_fallback` so the backfilled rows stay distinguishable from
    the identity pillar's own assertions in the output."""
    if iex_fallback is None or not iex_fallback.height:
        return era_identity.with_columns(
            pl.lit(False).alias("identity_issuer_from_iex_fallback")
        )
    joined = era_identity.join(iex_fallback, on="symbol_era_id", how="left")
    backfilled = pl.col("identity_issuer").is_null() & pl.col("iex_fallback_issuer").is_not_null()
    return joined.with_columns(
        pl.coalesce(["identity_issuer", "iex_fallback_issuer"]).alias("identity_issuer"),
        backfilled.fill_null(False).alias("identity_issuer_from_iex_fallback"),
    ).drop("iex_fallback_issuer")
