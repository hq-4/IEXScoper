from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.event_catalog_fetch import (
    GENERIC_USER_AGENT,
    NASDAQ_DELISTED_URL,
    OTHER_DELISTED_URL,
    WIKI_DEFUNCT_ETF_URL,
    enumerate_form25_hits,
    fetch_form25_documents,
    fetch_text_cached,
)
from utils.event_catalog_join import combined_summary, match_eras_to_catalog, summarize_source
from utils.event_catalog_sources import (
    DISPLAY_TICKER_RE,
    parse_form25,
    parse_nasdaq_delisted,
    parse_wiki_defunct_etfs,
    ticker_from_display_names,
)
from utils.sec_identity_sources import SecEvidenceClient

DEFAULT_ERA_CLASSES = Path("reports/openfigi-identity/era_classes.parquet")
DEFAULT_IDENTITY_FACTS = Path("data/resolution/identity_facts.jsonl")
DEFAULT_LEDGER = Path("data/manual_overrides/ticker_era_resolution_ledger.csv")
DEFAULT_CACHE_ROOT = Path("data/event_catalog/cache")
DEFAULT_OUTPUT_ROOT = Path("reports/event-catalog-probe")
FORM25_START = "2016-01-01"
FORM25_END = "2026-06-30"
FORM25_DOC_CAP = 4000
FORM25_DOC_SLEEP = 0.15
ALL_SOURCES = ("nasdaq_delisted", "sec_form25", "wiki_defunct_etf")


def main() -> int:
    load_dotenv()
    args = parse_args()
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    setup_logging(str(output_root / "event_catalog_probe.jsonl"))
    logger = get_logger(__name__)
    cache_root = Path(args.cache_root)
    unresolved = load_unresolved_eras(
        Path(args.era_classes), Path(args.identity_facts), Path(args.ledger)
    )
    logger.info(
        "Event catalog probe start",
        extra={
            "event": "event_catalog_probe_start",
            "detail": {"unresolved_eras": len(unresolved), "sources": list(args.sources)},
        },
    )
    catalogs: dict[str, list[dict[str, Any]]] = {}
    statuses: dict[str, str] = {}
    failures: dict[str, int] = {}
    extras: dict[str, dict[str, Any]] = {}
    user_agent = os.getenv(args.user_agent_env, "")
    for source in args.sources:
        catalogs[source], statuses[source], failures[source], extras[source] = run_source(
            source, cache_root, user_agent, args
        )
    report = build_report(unresolved, catalogs, statuses, failures, extras)
    write_report(output_root, report, catalogs)
    logger.info(
        "Event catalog probe complete",
        extra={"event": "event_catalog_probe_complete", "detail": report["combined"]},
    )
    return 0


def run_source(
    source: str, cache_root: Path, user_agent: str, args: argparse.Namespace
) -> tuple[list[dict[str, Any]], str, int, dict[str, Any]]:
    if source == "nasdaq_delisted":
        return run_nasdaq(cache_root, user_agent)
    if source == "wiki_defunct_etf":
        return run_wiki(cache_root, user_agent)
    if source == "sec_form25":
        name_map = build_name_symbol_map(Path(args.symbol_figi_map))
        return run_form25(cache_root, user_agent, args.form25_doc_cap, name_map)
    raise ValueError(f"unknown source {source}")


def run_nasdaq(
    cache_root: Path, user_agent: str
) -> tuple[list[dict[str, Any]], str, int, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    failures = 0
    per_file: dict[str, str] = {}
    for name, url in (
        ("nasdaqdelisted", NASDAQ_DELISTED_URL),
        ("otherdelisted", OTHER_DELISTED_URL),
    ):
        result = fetch_text_cached(
            url, cache_root / f"{name}.txt", user_agent or GENERIC_USER_AGENT
        )
        per_file[name] = result["status"]
        if result["status"].startswith("ok"):
            parsed, failed = parse_nasdaq_delisted(result["text"], "nasdaq_delisted")
            rows.extend(parsed)
            failures += failed
    status = "ok" if rows else ";".join(f"{k}={v}" for k, v in per_file.items())
    return rows, status, failures, {"per_file_status": per_file}


def run_wiki(
    cache_root: Path, user_agent: str
) -> tuple[list[dict[str, Any]], str, int, dict[str, Any]]:
    result = fetch_text_cached(
        WIKI_DEFUNCT_ETF_URL,
        cache_root / "wiki_defunct_etfs.html",
        user_agent or GENERIC_USER_AGENT,
    )
    if not result["status"].startswith("ok"):
        return [], result["status"], 0, {}
    rows, failures = parse_wiki_defunct_etfs(result["text"])
    return rows, "ok", failures, {}


NAME_SUFFIXES = frozenset(
    "INC CORP CORPORATION CO COMPANY LTD LLC LP LLP PLC HOLDINGS HOLDING GROUP NV SE AG".split()
)


def normalize_issuer_name(name: Any) -> str:
    text = re.sub(r"/(THE|A|AN)$", "", str(name or "").upper())
    words = re.sub(r"[^A-Z0-9 ]", " ", text).split()
    if words and words[0] == "THE":
        words.pop(0)
    while words and words[-1] in NAME_SUFFIXES:
        words.pop()
    return " ".join(words)


def subject_name_from_display(names: list[str]) -> str | None:
    for name in names or []:
        text = str(name)
        if "(CIK" in text and not DISPLAY_TICKER_RE.search(text):
            cleaned = re.sub(r"\s*\(CIK[^)]*\)", "", text).strip()
            if len(cleaned) > 2:
                return cleaned
    return None


def build_name_symbol_map(figi_map_path: Path) -> dict[str, str]:
    if not figi_map_path.exists():
        return {}
    frame = pl.read_parquet(figi_map_path).select(["symbol", "name"]).drop_nulls()
    by_name: dict[str, set[str]] = {}
    for symbol, name in frame.iter_rows():
        key = normalize_issuer_name(name)
        if key:
            by_name.setdefault(key, set()).add(str(symbol))
    return {key: next(iter(symbols)) for key, symbols in by_name.items() if len(symbols) == 1}


def run_form25(
    cache_root: Path, user_agent: str, doc_cap: int, name_map: dict[str, str] | None = None
) -> tuple[list[dict[str, Any]], str, int, dict[str, Any]]:
    if not user_agent.strip():
        return [], "skipped_no_sec_user_agent", 0, {}
    client = SecEvidenceClient(user_agent, sleep_seconds=FORM25_DOC_SLEEP)
    hits = enumerate_form25_hits(
        client, cache_root / "form25_index.jsonl", FORM25_START, FORM25_END
    )
    fetch_stats = fetch_form25_documents(
        hits, client, cache_root / "form25_docs", doc_cap, FORM25_DOC_SLEEP
    )
    rows, failures = parse_form25_docs(hits[:doc_cap], cache_root / "form25_docs", name_map)
    extra = form25_extra(hits, fetch_stats, doc_cap)
    extra["ticker_sources"] = _count_by(rows, "ticker_source")
    return rows, "ok", failures, extra


def parse_form25_docs(
    hits: list[dict[str, Any]], docs_dir: Path, name_map: dict[str, str] | None = None
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    failures = 0
    for hit in hits:
        row = form25_row(hit, docs_dir, name_map)
        if row is None:
            failures += 1
        else:
            rows.append(row)
    return rows, failures


def form25_row(
    hit: dict[str, Any], docs_dir: Path, name_map: dict[str, str] | None = None
) -> dict[str, Any] | None:
    ticker = ticker_from_display_names(hit.get("display_names") or [])
    ticker_source = "display_names" if ticker else None
    issuer, security_name, event_date = None, None, hit.get("file_date")
    doc_path = _doc_path(hit, docs_dir)
    if doc_path is not None:
        parsed = parse_form25(doc_path.read_text(encoding="utf-8", errors="replace"))
        issuer = parsed["issuer"]
        security_name = parsed.get("security_name")
        event_date = parsed["effective_date"] or hit.get("file_date")
    if not ticker and name_map:
        for candidate, source in (
            (security_name, "security_name_bind"),
            (issuer, "issuer_name_bind"),
        ):
            if candidate:
                ticker = name_map.get(normalize_issuer_name(candidate))
                if ticker:
                    ticker_source = source
                    break
    if not ticker and not issuer:
        issuer = subject_name_from_display(hit.get("display_names") or [])
    if not ticker:
        return None
    return {
        "ticker": ticker,
        "name": security_name or issuer or (hit.get("display_names") or [""])[0],
        "issuer": issuer,
        "event_date": event_date,
        "inception_date": None,
        "source": "sec_form25",
        "form": hit.get("form"),
        "adsh": hit.get("adsh"),
        "file_date": hit.get("file_date"),
        "ticker_source": ticker_source,
    }


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key))
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _doc_path(hit: dict[str, Any], docs_dir: Path) -> Path | None:
    from utils.event_catalog_fetch import _safe_name

    path = docs_dir / f"{_safe_name(hit)}.txt"
    return path if path.exists() else None


def form25_extra(
    hits: list[dict[str, Any]], fetch_stats: dict[str, Any], doc_cap: int
) -> dict[str, Any]:
    covered = hits[:doc_cap]
    dates = sorted(h["file_date"] for h in covered if h.get("file_date"))
    return {
        "efts_hits_total": len(hits),
        "covered_date_window": [dates[0], dates[-1]] if dates else None,
        **fetch_stats,
    }


def build_report(
    unresolved: list[dict[str, Any]],
    catalogs: dict[str, list[dict[str, Any]]],
    statuses: dict[str, str],
    failures: dict[str, int],
    extras: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    from datetime import datetime

    sources: dict[str, Any] = {}
    all_matches: list[dict[str, Any]] = []
    for source, catalog in catalogs.items():
        matches = match_eras_to_catalog(unresolved, catalog)
        all_matches.extend(matches)
        sources[source] = summarize_source(
            source, statuses[source], catalog, failures[source], matches, unresolved, extras[source]
        )
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "sources": sources,
        "combined": combined_summary(all_matches, unresolved),
        "_matches": all_matches,
    }


def write_report(
    output_root: Path, report: dict[str, Any], catalogs: dict[str, list[dict[str, Any]]]
) -> None:
    matches = report.pop("_matches")
    (output_root / "summary.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    for source, catalog in catalogs.items():
        if catalog:
            pl.DataFrame(catalog).write_parquet(output_root / f"catalog_{source}.parquet")
    if matches:
        pl.DataFrame(matches).write_parquet(output_root / "matched_eras.parquet")


def load_unresolved_eras(
    era_classes_path: Path, identity_facts_path: Path, ledger_path: Path
) -> list[dict[str, Any]]:
    eras = pl.read_parquet(era_classes_path).to_dicts()
    resolved = _resolved_era_ids(identity_facts_path, ledger_path)
    return [era for era in eras if era["symbol_era_id"] not in resolved]


def _resolved_era_ids(identity_facts_path: Path, ledger_path: Path) -> set[str]:
    ids: set[str] = set()
    for line in identity_facts_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            era_id = json.loads(line).get("symbol_era_id")
            if era_id:
                ids.add(str(era_id))
    ledger = pl.read_csv(ledger_path)
    ids.update(str(v) for v in ledger["symbol_era_id"].drop_nulls().to_list())
    return ids


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", nargs="+", default=list(ALL_SOURCES), choices=ALL_SOURCES)
    parser.add_argument("--era-classes", default=str(DEFAULT_ERA_CLASSES))
    parser.add_argument("--identity-facts", default=str(DEFAULT_IDENTITY_FACTS))
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--cache-root", default=str(DEFAULT_CACHE_ROOT))
    parser.add_argument(
        "--symbol-figi-map", default="reports/openfigi-identity/symbol_figi_map.parquet"
    )
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--user-agent-env", default="SEC_USER_AGENT")
    parser.add_argument("--form25-doc-cap", type=int, default=FORM25_DOC_CAP)
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
