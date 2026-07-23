from __future__ import annotations

import csv
import io
import json
import re
from datetime import date
from typing import Any
from urllib.parse import urlparse

from utils.sec_identity_evidence import parse_day, parse_display_name

OFFICIAL_SOURCES = {"finra_daily_list", "nasdaqtrader", "nyse_delisting", "sec_filing"}
TAG_PATTERN = re.compile(
    r"<(?:dei:)?(?P<tag>TradingSymbol|EntityRegistrantName|EntityCentralIndexKey|SecurityExchangeName|SecurityTitle)[^>]*>(?P<value>.*?)</[^>]+>",
    re.IGNORECASE | re.DOTALL,
)
IX_PATTERN = re.compile(
    r"<ix:(?:nonNumeric|nonFraction)[^>]+name=[\"']dei:(?P<tag>TradingSymbol|EntityRegistrantName|EntityCentralIndexKey|SecurityExchangeName|SecurityTitle)[\"'][^>]*>(?P<value>.*?)</ix:[^>]+>",
    re.IGNORECASE | re.DOTALL,
)


class FinraDailyListAdapter:
    source = "finra_daily_list"

    def parse(self, payload: Any) -> list[dict[str, Any]]:
        return [_official_row(row, self.source) for row in _records(payload)]


class NasdaqTraderAdapter:
    source = "nasdaqtrader"

    def parse(self, payload: Any) -> list[dict[str, Any]]:
        return [_official_row(row, self.source) for row in _records(payload)]


class NyseDelistingAdapter:
    source = "nyse_delisting"

    def parse(self, payload: Any) -> list[dict[str, Any]]:
        return [_official_row(row, self.source) for row in _records(payload)]


class IssuerSourceAdapter:
    def parse(self, payload: Any, *, url: str, issuer_domains: set[str]) -> list[dict[str, Any]]:
        host = (urlparse(url).hostname or "").lower()
        allowed = (
            host == "sec.gov"
            or host.endswith(".sec.gov")
            or any(host == domain or host.endswith(f".{domain}") for domain in issuer_domains)
        )
        if not allowed:
            return []
        return [
            _official_row(row, "issuer_primary") | {"source_url": url} for row in _records(payload)
        ]


def parse_inline_xbrl(text: str) -> dict[str, Any]:
    facts: dict[str, list[str]] = {}
    for match in [*TAG_PATTERN.finditer(text), *IX_PATTERN.finditer(text)]:
        value = re.sub(r"<[^>]+>", " ", match.group("value"))
        cleaned = " ".join(value.split()).strip()
        if cleaned:
            facts.setdefault(match.group("tag"), []).append(cleaned)
    return {
        "tickers": _unique(facts.get("TradingSymbol", []), upper=True),
        "registrant_names": _unique(facts.get("EntityRegistrantName", [])),
        "ciks": _unique([_clean_cik(item) for item in facts.get("EntityCentralIndexKey", [])]),
        "exchanges": _unique(facts.get("SecurityExchangeName", [])),
        "security_titles": _unique(facts.get("SecurityTitle", [])),
    }


def entity_selector_candidates(payload: dict[str, Any], symbol: str) -> list[dict[str, Any]]:
    hits = payload.get("hits", {}).get("hits", [])
    candidates: dict[str, dict[str, Any]] = {}
    for hit in hits if isinstance(hits, list) else []:
        source = hit.get("_source", {}) if isinstance(hit, dict) else {}
        names = source.get("display_names") or source.get("displayNames") or []
        names = [names] if isinstance(names, str) else names
        for name in names if isinstance(names, list) else []:
            parsed = parse_display_name(str(name))
            if symbol.upper() in parsed.tickers and parsed.cik:
                candidates[parsed.cik] = {
                    "cik": parsed.cik,
                    "issuer": parsed.issuer_name,
                    "source": "sec_entity_selector_discovery",
                    "verification_state": "discovery_only",
                }
    return list(candidates.values())


def verify_identity(
    symbol: str, boundary: date | str, candidates: list[dict[str, Any]]
) -> dict[str, Any]:
    target = boundary if isinstance(boundary, date) else parse_day(boundary)
    exact = [row for row in candidates if _exact_candidate(row, symbol, target)]
    entity_ids = {_clean_cik(row.get("cik") or row.get("entity_id")) for row in exact}
    entity_ids.discard("")
    if len(entity_ids) != 1:
        return {
            "verification_state": "collision_hold" if len(entity_ids) > 1 else "unresolved",
            "candidate_entity_count": len(entity_ids),
        }
    chosen = max(exact, key=_candidate_sort_key)
    return {
        "verification_state": "verified",
        "candidate_entity_count": 1,
        "entity_id": next(iter(entity_ids)),
        "issuer": chosen.get("issuer", ""),
        "instrument": chosen.get("security_title", ""),
        "source": chosen.get("source", ""),
        "source_url": chosen.get("source_url", ""),
    }


def _exact_candidate(row: dict[str, Any], symbol: str, target: date | None) -> bool:
    tickers = row.get("tickers") or [row.get("symbol")]
    exact_symbol = symbol.upper() in {str(value).upper() for value in tickers if value}
    source = str(row.get("source") or "")
    exact_source = source in OFFICIAL_SOURCES or source == "issuer_primary"
    start = parse_day(row.get("effective_date") or row.get("valid_from"))
    end = parse_day(row.get("end_date") or row.get("valid_through"))
    in_interval = bool(target and (not start or start <= target) and (not end or target <= end))
    exact_filing = source == "sec_filing" and bool(
        row.get("dei_exact") or row.get("display_name_exact")
    )
    official_record = source != "sec_filing" and bool(
        row.get("issuer") and row.get("security_title") and (start or end)
    )
    return exact_symbol and exact_source and in_interval and (exact_filing or official_record)


def _official_row(row: dict[str, Any], source: str) -> dict[str, Any]:
    lowered = {str(key).lower().replace(" ", "_"): value for key, value in row.items()}
    return {
        "symbol": _first(lowered, "symbol", "issue_symbol", "new_symbol", "ticker"),
        "old_symbol": _first(lowered, "old_symbol", "current_symbol", "old_ticker"),
        "issuer": _first(lowered, "issuer", "company_name", "security_name", "name"),
        "security_title": _first(lowered, "security_title", "issue_name", "security_name", "name"),
        "cik": _clean_cik(_first(lowered, "cik", "issuer_cik")),
        "effective_date": _first(lowered, "effective_date", "action_date", "date"),
        "end_date": _first(lowered, "end_date", "deletion_date", "delisting_date"),
        "action": _first(lowered, "action", "event_type", "reason"),
        "source_url": _first(lowered, "source_url", "url", "link"),
        "source": source,
    }


def _records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "rows", "results", "items"):
            if isinstance(payload.get(key), list):
                return [row for row in payload[key] if isinstance(row, dict)]
        return [payload]
    text = str(payload or "").strip()
    try:
        return _records(json.loads(text))
    except (json.JSONDecodeError, TypeError):
        return list(csv.DictReader(io.StringIO(text))) if text else []


def _candidate_sort_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("effective_date") or row.get("filing_date") or ""),
        str(row.get("source") or ""),
        str(row.get("issuer") or ""),
    )


def _first(row: dict[str, Any], *keys: str) -> str:
    return str(next((row[key] for key in keys if row.get(key) not in {None, ""}), "")).strip()


def _unique(values: list[str], *, upper: bool = False) -> list[str]:
    cleaned = [(value.upper() if upper else value).strip() for value in values]
    return list(dict.fromkeys(value for value in cleaned if value))


def _clean_cik(value: Any) -> str:
    return re.sub(r"\D", "", str(value or "")).lstrip("0")
