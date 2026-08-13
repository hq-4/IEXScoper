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
wrong-company risk this module exists to avoid.

A third pass, still exact at the token level rather than fuzzy, handles two remaining
structural gaps found by cross-checking the live worklist's still-unresolved
googleable-issuer names against SEC's current listings for the same ticker: (1) OpenFIGI
truncates its `name` field to a hard 28-character ceiling (`"ALPHA METALLURGICAL
RESOURCE"` for Alpha Metallurgical Resources, even eating into the base name to fit a
share-class suffix), and (2) un-expanded Bloomberg abbreviations survive normalization
(`HLDGS` vs `HOLDINGS`, `INTL` vs `INTERNATIONAL`) because they aren't legal suffixes
`normalize_name` knows to drop. Both collapse to the same rule: one normalized name is a
word-boundary prefix of the other. This carries real over-match risk if applied loosely
(a single short word like "Bancorp" would prefix-match hundreds of companies), so
`_prefix_match_name` requires the *shorter* side to have at least `MIN_PREFIX_TOKENS`
tokens and requires exactly one distinct CIK across every candidate that satisfies the
prefix relation — ambiguous still means no match, never a guess. Measured on the live
worklist: 220 additional unique-name matches (~21M trade rows) among names whose ticker
was independently confirmed to be currently SEC-listed; the ticker-reuse cases (a
different company now trading the same symbol, e.g. `IAC`, `USEG`) correctly stayed
unmatched since a reused ticker's name shares no real prefix relation with the old one.
[CA][IV][KBT]
"""

from __future__ import annotations

import re
from collections import defaultdict

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

# Floor for the prefix-match fallback: the shorter of the two normalized names must have
# at least this many tokens, so a single generic word (e.g. "Bancorp") can't prefix-match
# an unrelated company — the same over-match risk the rejected fuzzy matcher hit on real
# data.
MIN_PREFIX_TOKENS = 2

# OpenFIGI's 28-character name truncation sometimes lands mid-word ("RESOURCE" for
# "RESOURCES") rather than on a token boundary. A same-position final-token truncation is
# allowed, but only past this minimum length, so a short/generic partial token (e.g. a
# lone "R") can't over-match.
MIN_PARTIAL_TOKEN_CHARS = 3

# Guards the one real false-positive risk a mid-word partial match introduces: SPAC
# sequel numbering ("XYZ Acquisition Corp II" vs "...Corp III") is a genuine different
# company, not a truncation of the same name — and "II" is a literal string prefix of
# "III". Any leftover characters made up entirely of Roman-numeral letters blocks the
# partial match rather than accepting it.
ROMAN_NUMERAL_CHARS = frozenset("IVXLCDM")

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
    `symbol_era_id`, `name_matched_cik` (unpadded, null when none of the plain exact,
    descriptor-stripped exact, or descriptor-stripped prefix passes has an unambiguous
    match). Priority order: plain exact > stripped exact > stripped prefix — a cheaper,
    more certain match always wins over a broader one."""
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
    prefix_cik = _match_prefix_pass(stripped, name_index, "stripped_issuer").rename(
        {"name_matched_cik": "cik_prefix"}
    )
    return (
        plain_cik.join(stripped_cik, on="symbol_era_id", how="left")
        .join(prefix_cik, on="symbol_era_id", how="left")
        .with_columns(
            pl.coalesce(["cik_plain", "cik_stripped", "cik_prefix"]).alias("name_matched_cik")
        )
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


def _build_prefix_buckets(
    name_index: pl.DataFrame,
) -> dict[str, list[tuple[tuple[str, ...], str]]]:
    """Group the unambiguous name index by first token, so prefix matching only compares
    names that could plausibly share a prefix relation instead of a full cross product
    over every index row."""
    buckets: dict[str, list[tuple[tuple[str, ...], str]]] = defaultdict(list)
    for normalized_name, cik in zip(
        name_index["normalized_name"].to_list(), name_index["cik"].to_list(), strict=True
    ):
        tokens = tuple(normalized_name.split())
        if tokens:
            buckets[tokens[0]].append((tokens, cik))
    return buckets


def _is_prefix_relation(query_tokens: tuple[str, ...], candidate_tokens: tuple[str, ...]) -> bool:
    """True when one token tuple is a prefix of the other: either a clean whole-token
    prefix (extra trailing tokens on the longer side, e.g. "HERTZ GLOBAL" + "HLDGS"), or
    — only when both sides have the *same* token count and every token but the last
    matches exactly — a same-position final-token truncation past `MIN_PARTIAL_TOKEN_CHARS`
    that isn't a Roman-numeral sequel suffix. The shorter side must clear
    `MIN_PREFIX_TOKENS` either way."""
    shorter, longer = (
        (query_tokens, candidate_tokens)
        if len(query_tokens) <= len(candidate_tokens)
        else (candidate_tokens, query_tokens)
    )
    if len(shorter) < MIN_PREFIX_TOKENS or shorter[:-1] != longer[: len(shorter) - 1]:
        return False
    shorter_last, longer_last = shorter[-1], longer[len(shorter) - 1]
    if shorter_last == longer_last:
        return True
    if len(shorter) != len(longer):
        # Token-count mismatch already means "extra trailing tokens" is the only allowed
        # shape; the token at shorter's last position must match exactly, not partially.
        return False
    partial, full = (
        (shorter_last, longer_last)
        if len(shorter_last) <= len(longer_last)
        else (longer_last, shorter_last)
    )
    if len(partial) < MIN_PARTIAL_TOKEN_CHARS or not full.startswith(partial):
        return False
    remainder = full[len(partial) :]
    return any(ch not in ROMAN_NUMERAL_CHARS for ch in remainder)


def _prefix_match_name(
    normalized_query: str, buckets: dict[str, list[tuple[tuple[str, ...], str]]]
) -> str | None:
    """One normalized name -> one CIK, only when exactly one distinct CIK in the index
    has a name satisfying `_is_prefix_relation` with the query (covers both OpenFIGI's
    28-character name truncation and un-stripped abbreviations like `HLDGS` left over
    after `normalize_name`). More than one distinct CIK satisfying the relation is
    genuine ambiguity, not a match."""
    query_tokens = tuple(normalized_query.split())
    if not query_tokens:
        return None
    matched_ciks: set[str] = set()
    for tokens, cik in buckets.get(query_tokens[0], ()):
        if _is_prefix_relation(query_tokens, tokens):
            matched_ciks.add(cik)
            if len(matched_ciks) > 1:
                return None
    if len(matched_ciks) == 1:
        return next(iter(matched_ciks))
    return None


def _match_prefix_pass(
    frame: pl.DataFrame, name_index: pl.DataFrame, name_column: str
) -> pl.DataFrame:
    """Same shape as `_match_one_pass` (`symbol_era_id` -> `name_matched_cik`), but via
    `_prefix_match_name` instead of an exact-equality join. Resolved once per distinct
    normalized name rather than per row, since the era population can repeat the same
    issuer name many times."""
    buckets = _build_prefix_buckets(name_index)
    normalized = frame.select("symbol_era_id", name_column).with_columns(
        pl.col(name_column)
        .map_elements(normalize_name, return_dtype=pl.String)
        .alias("normalized_name")
    )
    lookup = {
        name: _prefix_match_name(name, buckets)
        for name in normalized["normalized_name"].unique().to_list()
        if name
    }
    return normalized.with_columns(
        pl.col("normalized_name")
        .map_elements(lambda name: lookup.get(name), return_dtype=pl.String)
        .alias("name_matched_cik")
    ).select("symbol_era_id", "name_matched_cik")


def require_columns(frame: pl.DataFrame, columns: tuple[str, ...]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"frame missing required columns: {missing}")
