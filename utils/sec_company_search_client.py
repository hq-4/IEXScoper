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
reads `title` and only extracts `<cik>` tags). [CA][IV][REH][KBT]
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


def _parse_ciks(response: requests.Response) -> dict[str, list[str]]:
    ciks = sorted({str(int(match)) for match in CIK_TAG_PATTERN.findall(response.text)})
    return {"ciks": ciks}
