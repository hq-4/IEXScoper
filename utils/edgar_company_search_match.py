"""Match an issuer name to a CIK via EDGAR's company-name browse search
(`utils.sec_company_search_client`).

EDGAR's classic `browse-edgar` company search does **literal prefix matching** against
the exact registered name string — confirmed live: `"CORE SCIENTIFIC INC"` (an
OpenFIGI/IEX-style name) returns zero hits against the registrant's actual `"Core
Scientific, Inc./tx"`, but the shorter `"Core Scientific"` matches immediately, because
prefix matching only needs the query to literally start the target string, and the
comma/suffix/jurisdiction-tag divergence breaks that for the full name but not the
truncated one. So this tries the full (descriptor-stripped) name first, then
progressively drops trailing words down to a 2-word floor — 1-word queries are too
generic to be worth the ambiguity/request cost — stopping at the first query that
yields a validated match.

A truncated query is more permissive, so it usually returns *more* candidates, not
fewer — every candidate a query returns is validated against the real registrant name
(the same `sec_sic_client.fetch_sic` call already used for SIC, often a free cache hit),
never a bare "only one search hit" trusted on its own. If more than one candidate
validates, that's genuine ambiguity (two different real companies both matching the
query name), not a bug to work around. A query returning an implausibly large candidate
count (self-evidently too generic to be useful) is reported ambiguous without validating
each one individually, and truncation stops there — a shorter query would only be worse.

A validated name match still isn't automatically trusted: a live run surfaced a genuine
SEC name *collision* — searching for the real, Kafka-company "Confluent, Inc." (CIK
1699838, SIC 7372) also turned up an unrelated same-named shell, "CONFLUENT INC" (CIK
1171179, blank SIC), that normalized-name matching alone can't tell apart from the real
one. Every confirmed-correct match found while building this had a real SIC on record;
the collision didn't — so a candidate with no SIC is never accepted, even if its name
matches exactly. This is free (the SIC is already fetched during validation), and safe
by the same logic as everything else here: better genuinely unresolved than
confidently wrong. [CA][IV][REH][KBT]
"""

from __future__ import annotations

from typing import Any

from utils.resolution_v2_network import CachedPrimaryClient, PrimarySourceError
from utils.sec_company_search_client import search_company_ciks
from utils.sec_name_cik_lookup import normalize_name, strip_security_descriptors
from utils.sec_sic_client import STATUS_FETCH_ERROR as SIC_STATUS_FETCH_ERROR
from utils.sec_sic_client import fetch_sic

STATUS_MATCHED = "matched"
STATUS_NO_CANDIDATES = "no_candidates"
STATUS_AMBIGUOUS = "ambiguous_candidates"
STATUS_NO_VALIDATED_MATCH = "no_validated_match"
STATUS_FETCH_ERROR = "fetch_error"

MAX_CANDIDATES_TO_VALIDATE = 8
MIN_QUERY_WORDS = 2


def match_issuer_name(
    client: CachedPrimaryClient, issuer_name: str, *, max_age_days: int = 90
) -> dict[str, Any]:
    """One issuer name -> one result row (always the same set of keys, regardless of
    outcome — see `_result`). Never raises; every outcome is a structured status so a
    batch run over thousands of names continues past a transient SEC 5xx/timeout on one
    name instead of aborting and losing every result already collected — nothing is
    cached on a `PrimarySourceError`, so a `fetch_error` name is retried, not
    permanently skipped, on the next run."""
    saw_any_candidates = False
    for query in _search_query_variants(issuer_name):
        try:
            candidates = search_company_ciks(client, query, max_age_days=max_age_days)
        except PrimarySourceError:
            return _result(issuer_name, STATUS_FETCH_ERROR)
        if not candidates:
            continue
        saw_any_candidates = True
        if len(candidates) > MAX_CANDIDATES_TO_VALIDATE:
            # Further truncation only broadens the match set further — stop here.
            return _result(issuer_name, STATUS_AMBIGUOUS, candidate_count=len(candidates))
        validated = _validate_candidates(client, issuer_name, candidates, max_age_days)
        if validated is None:
            return _result(issuer_name, STATUS_FETCH_ERROR)
        if len(validated) == 1:
            cik, sic_result = validated[0]
            return _result(
                issuer_name,
                STATUS_MATCHED,
                matched_cik=cik,
                candidate_name=sic_result.get("entity_name"),
                sic=sic_result.get("sic"),
                sic_description=sic_result.get("sic_description"),
            )
        if len(validated) > 1:
            return _result(issuer_name, STATUS_AMBIGUOUS, candidate_count=len(validated))
        # Zero candidates validated at this query — try a shorter one.
    status = STATUS_NO_VALIDATED_MATCH if saw_any_candidates else STATUS_NO_CANDIDATES
    return _result(issuer_name, status)


def _search_query_variants(name: str) -> list[str]:
    """Most-specific query first, then progressively fewer trailing words down to a
    `MIN_QUERY_WORDS` floor. Deduped and whitespace-normalized so a name with no
    descriptor suffix to strip doesn't waste a redundant identical search."""
    variants: list[str] = []
    seen: set[str] = set()

    def add(candidate: str) -> None:
        normalized = " ".join(candidate.split())
        key = normalized.casefold()
        if normalized and key not in seen:
            seen.add(key)
            variants.append(normalized)

    add(name)
    stripped = strip_security_descriptors(name)
    add(stripped)
    words = stripped.split()
    while len(words) > MIN_QUERY_WORDS:
        words = words[:-1]
        add(" ".join(words))
    return variants


def _validate_candidates(
    client: CachedPrimaryClient, issuer_name: str, candidates: list[str], max_age_days: int
) -> list[tuple[str, dict[str, Any]]] | None:
    """Every candidate's real registrant name (reusing `fetch_sic` — often a free cache
    hit) is checked against the query name; stops early once 2 validate, since that
    already proves ambiguity without checking the rest. Returns `None` (not an empty
    list) on a fetch error, so the caller reports `fetch_error` instead of a false
    negative."""
    validated: list[tuple[str, dict[str, Any]]] = []
    for cik in candidates:
        sic_result = fetch_sic(client, cik, max_age_days=max_age_days)
        if sic_result.get("fetch_status") == SIC_STATUS_FETCH_ERROR:
            return None
        if not sic_result.get("sic"):
            # A blank SIC means SEC has no record of this registrant ever actually
            # filing/operating — real evidence, not a hunch: a live search for a
            # legitimate operating company ("CONFLUENT INC-CLASS A") turned up an
            # unrelated same-name shell (CIK 1171179, blank SIC) alongside the real one
            # (CIK 1699838, SIC 7372) that a plain normalized-name match alone couldn't
            # tell apart. Every confirmed-correct match checked while building this had
            # a real SIC on record; this genuine collision didn't.
            continue
        if _names_match(issuer_name, sic_result.get("entity_name")):
            validated.append((cik, sic_result))
            if len(validated) >= 2:
                break
    return validated


def _names_match(issuer_name: str, entity_name: str | None) -> bool:
    if not entity_name:
        return False
    target = normalize_name(entity_name)
    if not target:
        return False
    if normalize_name(issuer_name) == target:
        return True
    return normalize_name(strip_security_descriptors(issuer_name)) == target


def _result(
    issuer_name: str,
    status: str,
    *,
    matched_cik: str | None = None,
    candidate_count: int | None = None,
    candidate_name: str | None = None,
    sic: str | None = None,
    sic_description: str | None = None,
) -> dict[str, Any]:
    return {
        "identity_issuer": issuer_name,
        "match_status": status,
        "matched_cik": matched_cik,
        "candidate_count": candidate_count,
        "candidate_name": candidate_name,
        "sic": sic,
        "sic_description": sic_description,
    }
