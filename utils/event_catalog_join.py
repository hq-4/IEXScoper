from __future__ import annotations

from typing import Any

from utils.event_catalog_sources import normalize_ticker, parse_loose_date, parse_yyyymmdd

DATE_WINDOW_DAYS = 45


def match_eras_to_catalog(
    eras: list[dict[str, Any]], catalog: list[dict[str, Any]], window_days: int = DATE_WINDOW_DAYS
) -> list[dict[str, Any]]:
    by_ticker: dict[str, list[dict[str, Any]]] = {}
    for row in catalog:
        by_ticker.setdefault(row["ticker"], []).append(row)
    matches: list[dict[str, Any]] = []
    for era in eras:
        for row in by_ticker.get(normalize_ticker(era["symbol"]), []):
            for basis in _date_bases(era, row, window_days):
                matches.append(_match_row(era, row, basis))
    return matches


def _date_bases(era: dict[str, Any], row: dict[str, Any], window_days: int) -> list[str]:
    last_day = parse_yyyymmdd(era.get("last_day"))
    first_day = parse_yyyymmdd(era.get("first_day"))
    event = parse_loose_date(str(row.get("event_date") or ""))
    inception = parse_loose_date(str(row.get("inception_date") or ""))
    bases: list[str] = []
    if event and last_day and abs((event - last_day).days) <= window_days:
        bases.append("delist_window")
    if inception and first_day and abs((inception - first_day).days) <= window_days:
        bases.append("inception_window")
    return bases


def _match_row(era: dict[str, Any], row: dict[str, Any], basis: str) -> dict[str, Any]:
    return {
        "symbol_era_id": era["symbol_era_id"],
        "symbol": era["symbol"],
        "openfigi_class": era.get("openfigi_class"),
        "gap_status": era.get("gap_status"),
        "first_day": era.get("first_day"),
        "last_day": era.get("last_day"),
        "source": row["source"],
        "catalog_ticker": row["ticker"],
        "catalog_name": row.get("name"),
        "catalog_issuer": row.get("issuer"),
        "catalog_event_date": row.get("event_date"),
        "catalog_inception_date": row.get("inception_date"),
        "match_basis": basis,
    }


def summarize_source(
    source: str,
    status: str,
    catalog: list[dict[str, Any]],
    parse_failures: int,
    matches: list[dict[str, Any]],
    unresolved_eras: list[dict[str, Any]],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    matched_ids = {m["symbol_era_id"] for m in matches}
    summary: dict[str, Any] = {
        "status": status,
        "catalog_size": len(catalog),
        "parse_failures": parse_failures,
        "eras_matched": len(matched_ids),
        "match_rows": len(matches),
        "by_openfigi_class": _class_breakdown(unresolved_eras, matched_ids),
        "fund_etf_hit_rate": _fund_hit_rate(unresolved_eras, matched_ids),
    }
    if extra:
        summary.update(extra)
    return summary


def combined_summary(
    matches: list[dict[str, Any]], unresolved_eras: list[dict[str, Any]]
) -> dict[str, Any]:
    matched_ids = {m["symbol_era_id"] for m in matches}
    total = len(unresolved_eras)
    return {
        "unresolved_eras": total,
        "eras_matched_unique": len(matched_ids),
        "coverage_share": round(len(matched_ids) / total, 4) if total else 0.0,
        "by_openfigi_class": _class_breakdown(unresolved_eras, matched_ids),
        "fund_etf_hit_rate": _fund_hit_rate(unresolved_eras, matched_ids),
        "by_source": _source_breakdown(matches),
    }


def _class_breakdown(
    unresolved_eras: list[dict[str, Any]], matched_ids: set[str]
) -> dict[str, Any]:
    breakdown: dict[str, dict[str, int]] = {}
    for era in unresolved_eras:
        bucket = breakdown.setdefault(
            str(era.get("openfigi_class") or "unknown"), {"total": 0, "matched": 0}
        )
        bucket["total"] += 1
        if era["symbol_era_id"] in matched_ids:
            bucket["matched"] += 1
    return dict(sorted(breakdown.items()))


def _fund_hit_rate(unresolved_eras: list[dict[str, Any]], matched_ids: set[str]) -> float | None:
    funds = [e for e in unresolved_eras if e.get("openfigi_class") == "fund_etf"]
    if not funds:
        return None
    hit = sum(1 for e in funds if e["symbol_era_id"] in matched_ids)
    return round(hit / len(funds), 4)


def _source_breakdown(matches: list[dict[str, Any]]) -> dict[str, int]:
    per_source: dict[str, set[str]] = {}
    for match in matches:
        per_source.setdefault(match["source"], set()).add(match["symbol_era_id"])
    return {source: len(ids) for source, ids in sorted(per_source.items())}
