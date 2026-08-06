"""SEC company-name -> CIK lookup, built entirely from data already fetched by
`utils/enrich_symbol_eras_sec.py` (`reports/sec-ticker-cik/sec_company_tickers_exchange.parquet`)
— zero new network calls. Matches by normalized company name rather than ticker, which
sidesteps the ticker-reuse ambiguity that makes ticker-based CIK matching unsafe for
dead-ticker eras: a company keeps roughly the same name even after its old ticker gets
reused by someone else, so a name match doesn't carry the same "wrong company" risk a
current-ticker match does. Only unambiguous exact-normalized-name matches are ever
returned; a name that normalizes to more than one distinct CIK is treated as no match
rather than guessed — aggressive normalization is safe here precisely because ambiguity
falls back to "no match," never a wrong pick.

`match_by_name` tries two exact matches against the same unambiguous index, not a fuzzy
one: the plain normalized `identity_issuer`, and — if that misses — the same name with
trailing Bloomberg/OpenFIGI security-descriptor tokens stripped first (`-CW23`, `-ADR`,
`W/I`, `-CLASS A`, …; ticker-level metadata OpenFIGI folds into its `name` field, not
part of the real legal name). Measured on the live worklist: this alone recovers 180
additional exact matches (299 era rows) with zero new ambiguity risk. A broader
token-subset/fuzzy matcher was evaluated and rejected — on real data it matched names
like "1895 Bancorp of Wisconsin" to an unrelated company simply named "Bancorp" (a
single generic token satisfying a naive subset check), which is exactly the kind of
wrong-company risk this module exists to avoid. [CA][IV][KBT]
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

# SEC appends a trailing "/XX" state-of-incorporation tag to disambiguate identically
# named registrants (e.g. "CORE SCIENTIFIC, INC./TX"). Left alone, NON_ALNUM turns the
# slash into a space and the two-letter code becomes a trailing token that blocks the
# legal-suffix strip loop below from ever reaching "INC" — stripped here, before that
# conversion, so it never gets the chance to.
JURISDICTION_SUFFIX = re.compile(r"/[A-Z]{2}$", re.IGNORECASE)

# Bloomberg/OpenFIGI appends these to a security's `name` field to distinguish share
# classes, warrants, ADRs, and when-issued lines — they're ticker/security metadata,
# never part of the issuer's actual legal name, so stripping them before matching is
# safe (unlike guessing at genuine name variation).
DESCRIPTOR_PATTERNS = (
    re.compile(r"-CW\d+$", re.IGNORECASE),
    re.compile(r"-SPON ADR$", re.IGNORECASE),
    re.compile(r"-ADR$", re.IGNORECASE),
    re.compile(r"[-\s]*W/I$", re.IGNORECASE),
    # Matches both "-CL A" and the "- CL A" spacing variant seen on real worklist rows
    # (e.g. "ROYALTY PHARMA PLC- CL A").
    re.compile(r"-\s*CL\s[A-Z]$", re.IGNORECASE),
    re.compile(r"-CLASS [A-Z]$", re.IGNORECASE),
    re.compile(r"-NEW$", re.IGNORECASE),
    re.compile(r"-WI$", re.IGNORECASE),
    re.compile(r"-WTS?$", re.IGNORECASE),
    re.compile(r"-RTS$", re.IGNORECASE),
    re.compile(r"-UNITS?$", re.IGNORECASE),
    # A bare trailing "-A"/"-B" share-class letter with no "CL"/"CLASS" word attached —
    # e.g. "EVERPURE INC-A", "C3.AI INC-A". Requires exactly one letter after the
    # hyphen so it can't accidentally eat a real two-letter word ending like "-CO".
    re.compile(r"-[A-Z]$", re.IGNORECASE),
)


def normalize_name(value: str | None) -> str:
    """Uppercase, strip a trailing SEC jurisdiction tag and punctuation, and drop
    trailing legal-entity suffix tokens (repeatedly, so "XYZ HOLDINGS INC" -> "XYZ").
    Blank/None -> ""."""
    text = JURISDICTION_SUFFIX.sub("", str(value or "").upper())
    text = NON_ALNUM.sub(" ", text)
    tokens = text.split()
    while tokens and tokens[-1] in LEGAL_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def strip_security_descriptors(value: str | None) -> str:
    """Remove trailing OpenFIGI/Bloomberg security-descriptor suffixes (see
    `DESCRIPTOR_PATTERNS`) before normalization, e.g. "ABB LTD-SPON ADR" -> "ABB LTD"."""
    text = str(value or "")
    for pattern in DESCRIPTOR_PATTERNS:
        text = pattern.sub("", text)
    return text


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
    `symbol_era_id`, `name_matched_cik` (unpadded, null when neither the plain nor the
    descriptor-stripped normalized name has an unambiguous match)."""
    require_columns(era_identity, ("symbol_era_id", "identity_issuer"))
    base = era_identity.select("symbol_era_id", "identity_issuer")
    plain_cik = _match_one_pass(base, name_index, "identity_issuer").rename(
        {"name_matched_cik": "cik_plain"}
    )
    stripped = base.with_columns(
        pl.col("identity_issuer")
        .map_elements(strip_security_descriptors, return_dtype=pl.String)
        .alias("stripped_issuer")
    )
    stripped_cik = _match_one_pass(stripped, name_index, "stripped_issuer").rename(
        {"name_matched_cik": "cik_stripped"}
    )
    return (
        plain_cik.join(stripped_cik, on="symbol_era_id", how="left")
        .with_columns(pl.coalesce(["cik_plain", "cik_stripped"]).alias("name_matched_cik"))
        .select("symbol_era_id", "name_matched_cik")
    )


def _match_one_pass(
    frame: pl.DataFrame, name_index: pl.DataFrame, name_column: str
) -> pl.DataFrame:
    normalized = frame.select("symbol_era_id", name_column).with_columns(
        pl.col(name_column)
        .map_elements(normalize_name, return_dtype=pl.String)
        .alias("normalized_name")
    )
    joined = normalized.join(name_index, on="normalized_name", how="left")
    return joined.select("symbol_era_id", pl.col("cik").alias("name_matched_cik"))


def require_columns(frame: pl.DataFrame, columns: tuple[str, ...]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"frame missing required columns: {missing}")
