"""Search SEC's classic EDGAR company browse index by name.

Unlike `sec_company_tickers_exchange.json` (current listings only),
`www.sec.gov/cgi-bin/browse-edgar?action=getcompany` includes every registrant SEC has
ever assigned a CIK to, active or not — the one source in this repo that can find a CIK
for a company that's genuinely gone (deregistered/merged/dissolved) rather than just
absent from a current-listing snapshot. Cached/rate-limited/retried through the same
`CachedPrimaryClient` as the rest of the SEC-lane tooling, via `get_json`'s
`parse_response`/`is_negative` hooks (this endpoint returns an Atom/XML feed, not
JSON — including a known SEC-side bug where `<entry title="...">` renders as a raw
`ARRAY(0x...)` placeholder instead of a real name, so this parser deliberately never
reads `title` and only extracts `<cik>` tags).

Phase 30: `lookup_cik_by_ticker` uses the *same* endpoint's `CIK` param, which SEC also
accepts a ticker symbol for — resolved via SEC's own persistent ticker registry, not
`browse-edgar`'s name-search index, so it finds a registrant even after it renamed away
from its former name entirely (`SEAS` -> "United Parks & Resorts Inc.", formerly
"SeaWorld Entertainment, Inc.") or dropped out of the name-search index after going
private (`HOLX` -> "Hologic Inc", unreachable by any name-based query at all — see
`edgar_company_search_match`'s Phase 30 entry for the full investigation). Safe despite
ticker reuse being a real risk elsewhere in this codebase (`sec_name_cik_lookup`'s
module docstring): this function only ever hands the caller *one candidate CIK to
validate*, the identical acceptance gate every other candidate source already goes
through — a stale/reused ticker pointing at an unrelated company gets rejected on name
mismatch downstream, not trusted here. [CA][IV][REH][KBT]
"""

from __future__ import annotations

import re
from datetime import timedelta

import requests

from utils.resolution_v2_network import CachedPrimaryClient
from utils.resolution_v2_registry import CachePolicy

SEC_COMPANY_SEARCH_SOURCE = "sec_company_search"
SEC_COMPANY_SEARCH_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
DEFAULT_MAX_AGE_DAYS = 90
CIK_TAG_PATTERN = re.compile(r"<cik>(\d+)</cik>")


def search_company_ciks(
    client: CachedPrimaryClient, name: str, *, max_age_days: int = DEFAULT_MAX_AGE_DAYS
) -> list[str]:
    """Distinct candidate CIKs (unpadded) for an EDGAR company-name browse search, or
    an empty list when nothing matches. More than one candidate means the name is
    ambiguous at the SEC level (a substring/prefix match hit multiple registrants) —
    this function makes no judgment call about which to pick; that's the caller's job
    (see `utils.build_edgar_company_search_matches`, which only ever accepts a single
    unambiguous candidate, then validates it against the registrant's actual name)."""
    if not name or not name.strip():
        return []
    params = {
        "action": "getcompany",
        "company": name,
        "type": "",
        "dateb": "",
        "owner": "include",
        "count": "100",
        "output": "atom",
    }
    payload, _ = client.get_json(
        SEC_COMPANY_SEARCH_SOURCE,
        SEC_COMPANY_SEARCH_URL,
        params,
        CachePolicy(max_age=timedelta(days=max_age_days)),
        parse_response=_parse_ciks,
        is_negative=lambda payload: not payload["ciks"],
    )
    return payload["ciks"]


def lookup_cik_by_ticker(
    client: CachedPrimaryClient, ticker: str, *, max_age_days: int = DEFAULT_MAX_AGE_DAYS
) -> str | None:
    """One CIK (unpadded) SEC's ticker registry resolves this symbol to, or `None` if
    the symbol was never registered at all. Live-confirmed (Phase 30) this always
    returns at most one `<cik>` tag regardless of `count`, since SEC's ticker-to-CIK
    mapping is inherently 1:1 — unlike `search_company_ciks`'s name search, which can
    legitimately return many."""
    if not ticker or not ticker.strip():
        return None
    params = {
        "action": "getcompany",
        "CIK": ticker.strip(),
        "type": "",
        "dateb": "",
        "owner": "include",
        "count": "1",
        "output": "atom",
    }
    payload, _ = client.get_json(
        SEC_COMPANY_SEARCH_SOURCE,
        SEC_COMPANY_SEARCH_URL,
        params,
        CachePolicy(max_age=timedelta(days=max_age_days)),
        parse_response=_parse_ciks,
        is_negative=lambda payload: not payload["ciks"],
    )
    ciks = payload["ciks"]
    return ciks[0] if ciks else None


def _parse_ciks(response: requests.Response) -> dict[str, list[str]]:
    ciks = sorted({str(int(match)) for match in CIK_TAG_PATTERN.findall(response.text)})
    return {"ciks": ciks}
