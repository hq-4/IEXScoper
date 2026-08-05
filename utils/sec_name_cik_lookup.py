"""SEC company-name -> CIK lookup, built entirely from data already fetched by
`utils/enrich_symbol_eras_sec.py` (`reports/sec-ticker-cik/sec_company_tickers_exchange.parquet`)
— zero new network calls. Matches by normalized company name rather than ticker, which
sidesteps the ticker-reuse ambiguity that makes ticker-based CIK matching unsafe for
dead-ticker eras: a company keeps roughly the same name even after its old ticker gets
reused by someone else, so a name match doesn't carry the same "wrong company" risk a
current-ticker match does. Only unambiguous exact-normalized-name matches are ever
returned; a name that normalizes to more than one distinct CIK is treated as no match
rather than guessed — aggressive normalization is safe here precisely because ambiguity
falls back to "no match," never a wrong pick. [CA][IV][KBT]
"""

from __future__ import annotations

import re

import polars as pl

LEGAL_SUFFIXES = frozenset(
    {
        "INCORPORATED",
        "INCORPORATION",
        "CORPORATION",
        "COMPANY",
        "LIMITED",
        "HOLDINGS",
        "HOLDING",
        "GROUP",
        "TRUST",
        "PARTNERS",
        "PARTNERSHIP",
        "INC",
        "CORP",
        "CO",
        "LTD",
        "LLC",
        "LP",
        "LLP",
        "PLC",
        "SA",
        "NV",
        "AG",
    }
)
NON_ALNUM = re.compile(r"[^A-Z0-9 ]+")


def normalize_name(value: str | None) -> str:
    """Uppercase, strip punctuation, and drop trailing legal-entity suffix tokens
    (repeatedly, so "XYZ HOLDINGS INC" -> "XYZ"). Blank/None -> ""."""
    text = NON_ALNUM.sub(" ", str(value or "").upper())
    tokens = text.split()
    while tokens and tokens[-1] in LEGAL_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def build_name_cik_index(sec_tickers: pl.DataFrame) -> pl.DataFrame:
    """One row per unambiguous normalized name: `normalized_name`, `cik` (unpadded).
    Names that normalize identically for two or more distinct CIKs are dropped
    entirely — a real, if rare, case (e.g. two different shell companies both named
    something that strips down to the same root)."""
    distinct = sec_tickers.select("sec_cik", "sec_name").unique()
    normalized = distinct.with_columns(
        pl.col("sec_name")
        .map_elements(normalize_name, return_dtype=pl.String)
        .alias("normalized_name"),
        pl.col("sec_cik").str.strip_chars_start("0").alias("cik"),
    ).filter(pl.col("normalized_name") != "")
    ambiguous = (
        normalized.group_by("normalized_name")
        .agg(pl.col("cik").n_unique().alias("cik_count"))
        .filter(pl.col("cik_count") > 1)
        .select("normalized_name")
    )
    return normalized.join(ambiguous, on="normalized_name", how="anti").select(
        "normalized_name", "cik"
    )


def match_by_name(era_identity: pl.DataFrame, name_index: pl.DataFrame) -> pl.DataFrame:
    """era_identity needs `symbol_era_id`, `identity_issuer`. Returns one row per era:
    `symbol_era_id`, `name_matched_cik` (unpadded, null when no unambiguous match)."""
    require_columns(era_identity, ("symbol_era_id", "identity_issuer"))
    normalized = era_identity.select("symbol_era_id", "identity_issuer").with_columns(
        pl.col("identity_issuer")
        .map_elements(normalize_name, return_dtype=pl.String)
        .alias("normalized_name")
    )
    joined = normalized.join(name_index, on="normalized_name", how="left")
    return joined.select("symbol_era_id", pl.col("cik").alias("name_matched_cik"))


def require_columns(frame: pl.DataFrame, columns: tuple[str, ...]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"frame missing required columns: {missing}")
