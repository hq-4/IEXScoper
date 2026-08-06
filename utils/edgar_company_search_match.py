"""Match an issuer name to a CIK via EDGAR's company-name browse search
(`utils.sec_company_search_client`), accepting only an unambiguous single candidate
whose actual registrant name — fetched from the *same* `submissions.json` call already
used for SIC (`utils.sec_sic_client.fetch_sic`) — exactly matches the query name after
normalization, with the same descriptor-stripping fallback
`utils.sec_name_cik_lookup` already uses. One search request plus (at most, and often
a free cache hit) one validation/SIC request per unique issuer name — never per era
row. [CA][IV][REH][KBT]
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
STATUS_NAME_MISMATCH = "name_mismatch"
STATUS_FETCH_ERROR = "fetch_error"


def match_issuer_name(
    client: CachedPrimaryClient, issuer_name: str, *, max_age_days: int = 90
) -> dict[str, Any]:
    """One issuer name -> one result row (always the same set of keys, regardless of
    outcome — see `_result`). Never raises; every outcome (no candidates, ambiguous,
    name mismatch, matched, fetch error) is a structured status so a batch run over
    thousands of names continues past a transient SEC 5xx/timeout on one name instead of
    aborting and losing every result already collected — nothing is cached on a
    `PrimarySourceError` (the failure happens before `EvidenceRegistry.put`), so a
    `fetch_error` name is retried, not permanently skipped, on the next run."""
    try:
        candidates = search_company_ciks(client, issuer_name, max_age_days=max_age_days)
    except PrimarySourceError:
        return _result(issuer_name, STATUS_FETCH_ERROR)
    if not candidates:
        return _result(issuer_name, STATUS_NO_CANDIDATES)
    if len(candidates) > 1:
        return _result(issuer_name, STATUS_AMBIGUOUS, candidate_count=len(candidates))
    cik = candidates[0]
    sic_result = fetch_sic(client, cik, max_age_days=max_age_days)
    if sic_result.get("fetch_status") == SIC_STATUS_FETCH_ERROR:
        # fetch_sic already absorbed its own PrimarySourceError into this status; treat
        # it the same as a search-side fetch error, not a genuine name mismatch, so a
        # rerun retries validation instead of leaving this permanently unresolved.
        return _result(issuer_name, STATUS_FETCH_ERROR)
    entity_name = sic_result.get("entity_name")
    if not _names_match(issuer_name, entity_name):
        return _result(issuer_name, STATUS_NAME_MISMATCH, candidate_name=entity_name)
    return _result(
        issuer_name,
        STATUS_MATCHED,
        matched_cik=cik,
        candidate_name=entity_name,
        sic=sic_result.get("sic"),
        sic_description=sic_result.get("sic_description"),
    )


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
