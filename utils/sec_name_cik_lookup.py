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

Two more narrow spacing gaps surfaced by tracing live Tier E rejections through
`edgar_company_search_match`'s cached responses (zero new network calls to find or
confirm either): `DESCRIPTOR_PATTERNS`' `-CLASS [A-Z]$` pattern required the hyphen to
sit directly against "CLASS" with no space, so `"SWEETGREEN INC - CLASS A"` and `"FIRST
DATA CORP- CLASS A"` never got their descriptor stripped even though the equivalent
abbreviated `-CL A` pattern already tolerated that spacing (see the `ROYALTY PHARMA PLC-
CL A` case above) — both patterns now also consume a space *before* the hyphen (e.g.
`"UCP INC - CL A"`), which the original `-CL A` fix left as a trailing-space artifact on
the stripped name. Separately, `JURISDICTION_SUFFIX`
only matched a tight `/XX` with no space, but SEC's own submissions payload returns
`"Alight Inc. / DE"` (a space before the state code) — `strip_security_descriptors`'s own
`Core Scientific, Inc./tx` precedent had no space, so this variant was never covered.
Replayed the full still-unresolved Tier E population (1,904 names) against the cached
search/validation responses already on disk with both fixes applied — zero new network
requests — and 15 names flip to a validated match (`SWEETGREEN`, `ALIGHT`, `ALTERYX`,
`FIRST DATA CORP`, `MCAFEE`, among others), 23 eras / 6.19M trade rows.

Phase 24 (see `docs/TASK_LIST.md` for full detail, kept brief here per the sibling
`edgar_company_search_match.py`'s own Phase 23 file-size cleanup): three more real
ADR-suffix variants `DESCRIPTOR_PATTERNS` didn't cover — `"SPONS ADR"` (a different
abbreviation than the existing `"SPON ADR"`), the fully spelled-out `"SPONSORED ADR"`,
and a space between the hyphen and `"ADR"`. Found tracing the worklist's top ADR-shaped
rows (`SONY CORP-SPONSORED ADR`, `SIBANYE GOLD LTD-SPONS ADR`,
`BRASKEM SA-CLASS A- ADR`); the space-tolerant fix is ordered before the `CLASS`
patterns so a compound `-CLASS A- ADR` suffix strips its ADR half first.

Phase 26: `NON_ALNUM` already turns a literal `"&"` into a dropped space (so "ECOLOGY &
ENVIRONMENT INC" normalizes with no trace of the ampersand at all), but the spelled-out
word `"AND"` survived as a real token — an asymmetry that broke an otherwise-exact match
whenever OpenFIGI's `identity_issuer` spells out `"AND"` and SEC's registered name uses
`"&"` (or vice versa), e.g. `"PETCO HEALTH AND WELLNESS CO"` vs SEC's `"Petco Health &
Wellness Company, Inc."`. `JOINER_WORDS` closes the gap by dropping `"AND"` the same way
`"&"` already disappears, anywhere in the token stream (not just trailing, since the word
can sit mid-name). Checked before shipping: replaying the entire SEC current-listings
index under old vs. new normalization produced zero new ambiguous-name collisions (13
either way); replaying the still-unresolved Tier E population found 4 names newly resolve
to a single validated candidate (`ECOLOGY AND ENVIRON`, `PETCO HEALTH AND WELLNESS CO`,
`VILLAGE BANK AND TRUST FINAN`, `YANGTZE RIVER PORT AND LOGIS`), each spot-checked against
SEC's live submissions payload.

Phase 28: `JURISDICTION_SUFFIX` only stripped a tag anchored at the true end of the
string (`"/DE"`), but SEC's own registrant names sometimes wrap it in a *second* trailing
slash (`"TRC COMPANIES INC /DE/"`) — 253 names in the current-listings index alone carry
this shape, every one previously left with the tag surviving as a stray trailing token no
real OpenFIGI name would ever replicate (so effectively unreachable, found while tracing
Phase 27's `TRC COS INC` gap). Added an optional trailing `/?`. Collision check: 3 new
ambiguous groups, each genuinely different real companies sharing an identical base name
post-strip (`CITIZENS`, `FIRST BANCORP`, `INDEPENDENT BANK`) — none were reachable matches
before either, so nothing regresses; see the constant's own comment for detail.
Cache-only quantification against the still-unresolved population: 24 names newly resolve
(`AETNA INC`, `CYPRESS SEMICONDUCTOR CORP`, `LINEAR TECHNOLOGY CORP`, `PLANTRONICS INC`,
`STILLWATER MINING CO`, `TRC COS INC`, `WEINGARTEN REALTY INVESTORS`, among others), zero
network calls, zero cache misses; SIC codes spot-checked against cached data, all correct
for real, well-known companies.

Phase 29: `HORIZON THERAPEUTICS PLC` traced a gap in `LEGAL_SUFFIXES` itself: OpenFIGI's
`"PLC"` abbreviation pops as a trailing legal suffix, but SEC's own registered name for
the same UK/Irish-incorporated entity sometimes spells it out as `"Public Ltd Co"` (SEC's
own three-word expansion, not just `"PLC"`) — `"PUBLIC"` isn't itself a recognized legal
suffix, so the pop loop stops there, one token short of where the abbreviated side lands.
Added `"PUBLIC"` to `LEGAL_SUFFIXES`. Checked first: every current-listings name ending in
`"PUBLIC"` does so as part of this exact `"PUBLIC LTD CO"` pattern (5 real names,
`PROTHENA CORP PUBLIC LTD CO`, `CRH PUBLIC LTD CO`, `VODAFONE GROUP PUBLIC LTD CO`, among
others) — never a genuine distinguishing final word on its own. Collision check: zero new
ambiguous-name collisions (16 either way). Cache-only Tier E quantification: 2 names newly
resolve (`HORIZON THERAPEUTICS PLC`, `KALERA PLC`), zero network calls, zero cache misses;
both spot-checked against cached SIC data, correct.

Phase 31: `TUSIMPLE HOLDINGS INC - A` (Phase 30's residual open case) traced to the bare
trailing share-class-letter descriptor pattern (`-[A-Z]$`) requiring the hyphen directly
against the letter, with no tolerance for surrounding whitespace — unlike the `-CL A`/
`-CLASS A` patterns, which already got this same spacing fix in Phase 12. Left un-stripped,
the trailing `" - A"` blocked the legal-suffix pop loop from ever reaching `"INC"`/
`"HOLDINGS"` underneath it. Widened to tolerate whitespace on both sides of the hyphen,
same as the `-CL A`/`-CLASS A` fix. Cache-only quantification: 17 of
23 names carrying this shape newly resolve (`TUSIMPLE HOLDINGS INC - A`, `SWITCH INC - A`,
`ATRECA INC - A`, `COWEN INC - A`, `TRIBUNE MEDIA CO - A`, `TERRAFORM POWER INC - A`,
among others), zero network calls, zero cache misses; spot-checked against cached SIC
data, all correct. No collision-risk replay needed here (unlike `JURISDICTION_SUFFIX`/
`LEGAL_SUFFIXES`): `DESCRIPTOR_PATTERNS` only ever strips OpenFIGI's query-side name, not
SEC's own registered names, so it cannot create a new ambiguity within the SEC index
itself.

Phase 32: `DOTTED_ABBREVIATION` fuses SEC's period-per-letter abbreviations (`"U.S."`,
`"S.A."`, `"N.V."`, `"L.P."`) before general punctuation-stripping would otherwise split
them into stray single-letter tokens — see the constant's own comment for the full
collision-risk story, which surfaced a genuine pre-existing Tier D correctness bug (four
distinct real Navios Maritime registrants silently sharing one CIK) this fix retroactively
exposed and corrected, not just the usual "zero new collisions" result.

Phase 33: the same jurisdiction-tag convention `JURISDICTION_SUFFIX` already handles
sometimes uses a backslash instead of a forward slash in SEC's own submissions data
(e.g. `"AGILITI, INC."` followed by a trailing backslash-state tag) — confirmed
directly against `data.sec.gov`, not a rendering artifact. Widened to accept
either slash direction. Zero occurrences in the current-listings index (only reachable
via the Phase 30 ticker fallback), so the collision check found nothing new either way.
Cache-only quantification: 6 rows across 4 distinct real companies newly resolve
(`AGILITI INC`, `DIAMOND EAGLE ACQUISITION CO` -> DraftKings Holdings' real 2020 SPAC
merger, `LANDEC CORP` -> renamed Lifecore Biomedical, `PROTAGENIC THERAPEUTIC`), zero
network calls, zero cache misses, all spot-checked correct.

Phase 34: `POSSESSIVE_APOSTROPHE` fuses a possessive-contraction apostrophe (`CONN'S`
-> `CONNS`) before general punctuation-stripping would otherwise split it into a stray
one-letter `"S"` token — SEC's own registered names drop the apostrophe entirely rather
than replacing it with a space. Same collision-risk-free, quantify-first discipline as
every prior shared-`normalize_name` change (zero new collisions across the current-
listings index; 4 names newly resolve — `ART'S-WAY MANUFACTURING CO`, `CONN'S INC`,
`FLANIGAN'S ENTERPRISES INC`, `RUTH'S HOSPITALITY GROUP INC` — zero network calls, zero
cache misses, all spot-checked correct). [CA][IV][KBT]
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
        # Phase 29: SEC sometimes spells "PLC" out as "Public Ltd Co" rather than the
        # abbreviation — "LTD"/"CO" already pop, "PUBLIC" needs to too so both sides
        # land on the same base name. Never a genuine distinguishing final word on its
        # own in the current-listings index (see module docstring).
        "PUBLIC",
    }
)
NON_ALNUM = re.compile(r"[^A-Z0-9 ]+")

# "&" already vanishes entirely under NON_ALNUM (turned into a dropped space, not a
# word); JOINER_WORDS makes the spelled-out equivalent behave the same way so
# "PETCO HEALTH AND WELLNESS CO" normalizes identically to SEC's own
# "Petco Health & Wellness Company, Inc." Filtered anywhere in the token stream, not
# just trailing, since the word can sit mid-name.
JOINER_WORDS = frozenset({"AND"})

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
# slash into a space and the trailing word becomes a token that blocks the legal-suffix
# strip loop below from ever reaching "INC" — stripped here, before that conversion, so
# it never gets the chance to. Tolerates the "/ XX" spacing variant too (e.g. SEC's own
# submissions payload returns "Alight Inc. / DE", not "/DE"). Phase 24: widened from
# exactly 2 letters to any letters — SEC uses the identical trailing-slash convention for
# a "/THE" sorting artifact (`"EASTERN CO/THE"` = "The Eastern Company", so it alphabetizes
# under "E"), successor tags (`"/NEW"`), and full country/state names
# (`"BITFARMS LTD/CANADA"`, `"PEOPLES FINANCIAL CORP/MISS"`), not just 2-letter state
# codes — none of these are ever part of a registrant's actual distinguishing name.
# Checked before widening: zero new ambiguous-name collisions anywhere in the full SEC
# current-listings index (13 collisions either way — the widening doesn't create any).
#
# Phase 28: SEC's own registrant names sometimes wrap the tag in a *second* trailing
# slash too (`"TRC COMPANIES INC /DE/"`, not just `"TRC COMPANIES INC /DE"`) — 253 names
# in the current-listings index alone carry this shape, and the un-widened pattern's `$`
# anchor left every one of them un-stripped, with the tag surviving as a real trailing
# token that no OpenFIGI-side name would ever also carry (so these entries were
# effectively unreachable, not just occasionally wrong). Added an optional trailing `/?`.
# Collision check: 3 new ambiguous-name groups appear (`CITIZENS`, `FIRST BANCORP`,
# `INDEPENDENT BANK`) — each is genuinely different real companies sharing an identical
# base legal name once their state tag strips correctly (three distinct real "First
# Bancorp" bank holding companies in PR/NC/ME, for instance). None of these were
# reachable matches before this change either (their old normalized form kept a stray
# state-code token no real issuer name would replicate), so nothing that previously
# worked regresses — they're now explicitly ambiguous/dropped instead of silently
# unmatchable, the same safe "ambiguous means no match" posture `build_name_cik_index`
# already applies everywhere else.
#
# Phase 33: SEC's own submissions data occasionally uses a backslash instead of a
# forward slash for this exact tag (`"AGILITI, INC. \DE"`, `"LANDEC CORP \CA\"`,
# `"Protagenic Therapeutics, Inc.\new"`) — a real, if uncommon, alternate data-entry
# convention confirmed directly against `data.sec.gov`, not a rendering artifact of any
# one endpoint. Widened to accept either slash direction, tolerating the same optional
# second trailing mark and spacing already handled for the forward-slash form. Zero
# occurrences of this shape anywhere in the current-listings index (these are all
# names EDGAR search alone can't find — recoverable only via the Phase 30 ticker
# fallback), so the usual full-index collision check found nothing to report either way
# (22 ambiguous groups, unchanged).
JURISDICTION_SUFFIX = re.compile(r"[/\\]\s*[A-Z]+\s*[/\\]?$", re.IGNORECASE)

# SEC frequently punctuates a compact abbreviation with a period after every letter
# ("U.S. Silica", "Cosan S.A.", "Sono Group N.V.", "TXO Partners, L.P.") — left alone,
# NON_ALNUM below would convert each internal period into a token-splitting space
# ("S.A." -> "S A", two stray single-letter tokens), which breaks two things at once:
# an exact match against OpenFIGI's already-unpunctuated form ("US SILICA" vs "U S
# SILICA"), and the legal-suffix pop loop's whole-token check ("SA"/"NV"/"LP" in
# LEGAL_SUFFIXES never matches a split "S A"/"N V"/"L P"). Collapsed to the fused form
# ("S.A." -> "SA") before general punctuation-stripping — narrow by construction (only
# matches letters immediately fused by periods with no space between them, so a genuine
# multi-word phrase is never touched). 182 names in the current-listings index alone
# carry this shape.
DOTTED_ABBREVIATION = re.compile(r"\b(?:[A-Z]\.){2,}", re.IGNORECASE)

# Phase 34: SEC's own registered names drop a possessive apostrophe entirely
# ("CONN'S INC" -> "CONNS INC", "FLANIGAN'S ENTERPRISES INC" -> "FLANIGANS ENTERPRISES
# INC") rather than replacing it with a space — left to `NON_ALNUM` below, the
# apostrophe becomes a token-splitting space instead, producing a stray one-letter "S"
# token ("CONN" + "S") that never matches SEC's fused "CONNS". Deletes just the
# apostrophe when it's immediately followed by a bare "S" (the possessive-contraction
# shape), narrow by construction — never touches an apostrophe anywhere else in a name
# (a mid-word apostrophe not followed by "S" still becomes a space as before).
POSSESSIVE_APOSTROPHE = re.compile(r"'(?=S(?:[^A-Za-z0-9]|$))", re.IGNORECASE)

# Bloomberg/OpenFIGI appends these to a security's `name` field to distinguish share
# classes, warrants, ADRs, and when-issued lines — they're ticker/security metadata,
# never part of the issuer's actual legal name, so stripping them before matching is
# safe (unlike guessing at genuine name variation).
DESCRIPTOR_PATTERNS = (
    re.compile(r"-CW\d+$", re.IGNORECASE),
    re.compile(r"-SPON ADR$", re.IGNORECASE),
    # "SPONS ADR" (e.g. "SIBANYE GOLD LTD-SPONS ADR") is a differently-abbreviated
    # sibling of "-SPON ADR" above, not covered by it — real gap found while tracing the
    # worklist's top ADR-shaped rows (Phase 24).
    re.compile(r"-SPONS\s?ADR$", re.IGNORECASE),
    # The fully spelled-out form (e.g. "SONY CORP-SPONSORED ADR"), same real gap.
    re.compile(r"[-\s]*SPONSORED\s+ADR$", re.IGNORECASE),
    # Tolerates a space between the hyphen and "ADR" (e.g. "BRASKEM SA-CLASS A- ADR"),
    # not just the tight "-ADR" the original pattern required — same spacing-tolerance
    # precedent as the `-CL A`/`-CLASS A` fixes below. Ordered before the CLASS patterns
    # so a compound "-CLASS A- ADR" suffix strips its ADR half first, letting the CLASS
    # pattern then match what's left.
    re.compile(r"[-\s]+ADR$", re.IGNORECASE),
    re.compile(r"[-\s]*W/I$", re.IGNORECASE),
    # Matches "-CL A", the "- CL A" spacing variant (e.g. "ROYALTY PHARMA PLC- CL A"),
    # and the " -CL A"/"" - CL A" variant with a space *before* the hyphen too (e.g.
    # "UCP INC - CL A") — the leading `\s*` consumes that space so it doesn't survive as
    # a trailing artifact on the stripped name.
    re.compile(r"\s*-\s*CL\s[A-Z]$", re.IGNORECASE),
    # Same spacing tolerance for the unabbreviated "CLASS" word ("SWEETGREEN INC -
    # CLASS A", "FIRST DATA CORP- CLASS A") — the "-CL A" fix above only covered the
    # abbreviated form.
    re.compile(r"\s*-\s*CLASS\s[A-Z]$", re.IGNORECASE),
    re.compile(r"-NEW$", re.IGNORECASE),
    re.compile(r"-WI$", re.IGNORECASE),
    re.compile(r"-WTS?$", re.IGNORECASE),
    re.compile(r"-RTS$", re.IGNORECASE),
    re.compile(r"-UNITS?$", re.IGNORECASE),
    # A bare trailing "-A"/"-B" share-class letter with no "CL"/"CLASS" word attached —
    # e.g. "EVERPURE INC-A", "C3.AI INC-A". Requires exactly one letter after the
    # hyphen so it can't accidentally eat a real two-letter word ending like "-CO".
    # Phase 31: tolerates whitespace around the hyphen too ("TUSIMPLE HOLDINGS INC - A",
    # "SWITCH INC - A"), the same spacing-tolerance precedent as the `-CL A`/`-CLASS A`
    # patterns above — the tight form left this real, common share-class shape
    # un-stripped, which then blocked the legal-suffix pop loop below from ever
    # reaching "INC"/"HOLDINGS".
    re.compile(r"\s*-\s*[A-Z]$", re.IGNORECASE),
)


def normalize_name(value: str | None) -> str:
    """Uppercase, strip a trailing SEC jurisdiction tag, fuse a dotted abbreviation
    ("S.A." -> "SA") and a possessive apostrophe ("CONN'S" -> "CONNS") before general
    punctuation-stripping would otherwise split them, drop joiner words ("AND", to
    match "&" already vanishing under punctuation-stripping), and drop trailing
    legal-entity suffix tokens (repeatedly, so "XYZ HOLDINGS INC" -> "XYZ").
    Blank/None -> ""."""
    text = JURISDICTION_SUFFIX.sub("", str(value or "").upper())
    text = DOTTED_ABBREVIATION.sub(lambda match: match.group(0).replace(".", ""), text)
    text = POSSESSIVE_APOSTROPHE.sub("", text)
    text = NON_ALNUM.sub(" ", text)
    tokens = [token for token in text.split() if token not in JOINER_WORDS]
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
