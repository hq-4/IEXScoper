from __future__ import annotations

from datetime import date
from typing import Any

from utils.sec_identity_evidence import parse_day


def select_stratified_filings(
    filings: list[dict[str, Any]], boundary: date
) -> list[dict[str, Any]]:
    unique = _deduplicate(filings)
    operating = _closest(unique, boundary, {"8-K", "6-K"}, 4)
    regulatory = _closest(unique, boundary, {"25", "25-NSE", "15", "15-12B", "15-12G", "15-15D"}, 4)
    deals = _closest(
        unique, boundary, {"425", "DEFM14A", "S-4", "S-4/A", "SC 13E3", "SC 13E3/A"}, 2
    )
    selected = operating + regulatory + deals
    accessions = {str(item.get("accession_no") or "") for item in selected}
    exhibits = [item for item in unique if _is_completion_exhibit(item, accessions)]
    return _deduplicate(selected + exhibits)


def _closest(
    rows: list[dict[str, Any]], boundary: date, forms: set[str], limit: int
) -> list[dict[str, Any]]:
    matching = [row for row in rows if str(row.get("form") or "").upper() in forms]
    return sorted(matching, key=lambda row: _distance(row, boundary))[:limit]


def _distance(row: dict[str, Any], boundary: date) -> int:
    filed = parse_day(row.get("filing_date"))
    return abs((filed - boundary).days) if filed else 100_000


def _deduplicate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("accession_no") or ""),
            str(row.get("document_url") or row.get("role") or ""),
        )
        unique.setdefault(key, row)
    return list(unique.values())


def _is_completion_exhibit(row: dict[str, Any], accessions: set[str]) -> bool:
    role = str(row.get("role") or row.get("document_type") or "").upper()
    return str(row.get("accession_no") or "") in accessions and role.startswith("EX-99.1")
