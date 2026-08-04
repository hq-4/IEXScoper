from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

import requests

from utils.sec_identity_sources import SecEvidenceClient, SecTransportError, document_url

NASDAQ_DELISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqdelisted.txt"
OTHER_DELISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherdelisted.txt"
WIKI_DEFUNCT_ETF_URL = "https://en.wikipedia.org/wiki/List_of_defunct_exchange-traded_funds"
EFTS_FORMS = "25,25-NSE"
EFTS_PAGE_SIZE = 100
FETCH_TIMEOUT_SECONDS = 20.0
GENERIC_USER_AGENT = "IEXScoper event-catalog probe (research)"
RETRYABLE = {429, 500, 502, 503, 504}


def fetch_text_cached(url: str, cache_path: Path, user_agent: str) -> dict[str, Any]:
    if cache_path.exists():
        return {"status": "ok_cached", "text": cache_path.read_text(encoding="utf-8")}
    result = _fetch_text(url, user_agent)
    if result["status"] == "ok":
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(result["text"], encoding="utf-8")
    return result


def _fetch_text(url: str, user_agent: str) -> dict[str, Any]:
    try:
        response = requests.get(
            url,
            headers={"User-Agent": user_agent},
            timeout=FETCH_TIMEOUT_SECONDS,
            allow_redirects=False,
        )
    except requests.RequestException as exc:
        return {"status": "unreachable", "error": str(exc), "text": ""}
    if response.status_code in {301, 302, 303, 307, 308}:
        location = response.headers.get("Location", "")
        status = "unreachable_404" if "404" in location else f"redirect:{location}"
        return {"status": status, "text": ""}
    if response.status_code == 404:
        return {"status": "unreachable_404", "text": ""}
    if response.status_code != 200:
        return {"status": f"http_{response.status_code}", "text": ""}
    if "Trader.aspx" in response.text or "<html" in response.text[:200].lower():
        return {"status": "format_changed_html", "text": ""}
    return {"status": "ok", "text": response.text}


def enumerate_form25_hits(
    client: SecEvidenceClient,
    cache_path: Path,
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    cached = _load_jsonl(cache_path)
    hits, seen = cached, {h["adsh"] for h in cached}
    start_year = int(start_date[:4])
    for year in _missing_years(cached, start_year, int(end_date[:4])):
        year_hits = _year_hits(client, year, start_date, end_date)
        for hit in year_hits:
            if hit["adsh"] not in seen:
                seen.add(hit["adsh"])
                hits.append(hit)
        _append_jsonl(cache_path, year_hits)
    return sorted(hits, key=lambda h: h.get("file_date", ""), reverse=True)


def _missing_years(cached: list[dict[str, Any]], start_year: int, end_year: int) -> list[int]:
    done = {int(h["file_date"][:4]) for h in cached if h.get("file_date")}
    return [year for year in range(start_year, end_year + 1) if year not in done]


def _year_hits(
    client: SecEvidenceClient, year: int, start_date: str, end_date: str
) -> list[dict[str, Any]]:
    lower = max(start_date, f"{year}-01-01")
    upper = min(end_date, f"{year}-12-31")
    hits: list[dict[str, Any]] = []
    offset = 0
    while True:
        params = {
            "forms": EFTS_FORMS,
            "startdt": lower,
            "enddt": upper,
            "from": str(offset),
            "size": str(EFTS_PAGE_SIZE),
        }
        payload = _search_with_fallback(client, params)
        batch = payload.get("hits", {}).get("hits", [])
        hits.extend(_flatten_hit(hit) for hit in batch)
        if len(batch) < EFTS_PAGE_SIZE:
            return hits
        offset += EFTS_PAGE_SIZE


def _search_with_fallback(client: SecEvidenceClient, params: dict[str, str]) -> dict[str, Any]:
    try:
        return client.search(params)
    except (SecTransportError, ValueError):
        fallback = {"forms": EFTS_FORMS, "from": params["from"], "size": params["size"]}
        payload = client.search(fallback)
        batch = payload.get("hits", {}).get("hits", [])
        kept = [h for h in batch if params["startdt"] <= _hit_date(h) <= params["enddt"]]
        return {"hits": {"hits": kept}}


def _hit_date(hit: dict[str, Any]) -> str:
    return str(hit.get("_source", {}).get("file_date") or "")


def _flatten_hit(hit: dict[str, Any]) -> dict[str, Any]:
    item = hit.get("_source", {})
    names = item.get("display_names") or []
    accession = str(item.get("adsh") or "")
    return {
        "adsh": accession,
        "form": str(item.get("form") or ""),
        "file_date": str(item.get("file_date") or ""),
        "display_names": [str(n) for n in names] if isinstance(names, list) else [str(names)],
        "document_url": document_url(hit, item, accession, ""),
    }


def fetch_form25_documents(
    hits: list[dict[str, Any]],
    client: SecEvidenceClient,
    docs_dir: Path,
    cap: int,
    sleep_seconds: float,
) -> dict[str, Any]:
    docs_dir.mkdir(parents=True, exist_ok=True)
    fetched = failed = 0
    for hit in hits[:cap]:
        path = docs_dir / f"{_safe_name(hit)}.txt"
        if path.exists():
            continue
        try:
            text = client.document_text(hit["document_url"])
        except (SecTransportError, ValueError):
            failed += 1
            continue
        path.write_text(text, encoding="utf-8")
        fetched += 1
        time.sleep(max(sleep_seconds, 0.13))
    return {
        "fetched": fetched,
        "fetch_failures": failed,
        "cache_hits": _doc_count(hits, docs_dir, cap),
    }


def _doc_count(hits: list[dict[str, Any]], docs_dir: Path, cap: int) -> int:
    return sum(1 for hit in hits[:cap] if (docs_dir / f"{_safe_name(hit)}.txt").exists())


def _safe_name(hit: dict[str, Any]) -> str:
    raw = f"{hit['adsh']}_{hit['document_url'].rsplit('/', 1)[-1]}"
    return re.sub(r"[^A-Za-z0-9_.\-]", "_", raw)[:180]


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
