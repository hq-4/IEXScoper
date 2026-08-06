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


def load_edgar_matches(path: Path) -> pl.DataFrame | None:
    """Tier E (EDGAR company-search-matched CIKs, see `utils.sector_cik_reconcile`) is
    skipped, not an error, when `utils/build_edgar_company_search_matches.py` hasn't
    been run yet."""
    if not path.exists():
        return None
    return pl.read_parquet(path).select("identity_issuer", "matched_cik")
