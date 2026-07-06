from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Any

import polars as pl
import requests

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.edgar_full_text_client import request_with_retries, search_params
from utils.edgar_full_text_outputs import build_summary, write_outputs
from utils.edgar_full_text_schema import (
    DEFAULT_ENDPOINT,
    DEFAULT_EVENT_TERMS,
    DEFAULT_FORMS,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_RETRIES,
    DEFAULT_SIZE,
    DEFAULT_SLEEP_SECONDS,
    DEFAULT_TEMPLATE_PATH,
    DEFAULT_TIMEOUT_SECONDS,
    LEAD_SCHEMA,
)
from utils.search_edgar_full_text_types import EdgarFullTextConfig


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root)
    setup_logging(str(output_root / "edgar_full_text_search.jsonl"))
    try:
        config = EdgarFullTextConfig(
            template_path=Path(args.template_path),
            output_root=output_root,
            endpoint=args.endpoint,
            symbols=parse_csv_arg(args.symbols),
            user_agent=resolve_user_agent(args.user_agent),
            forms=parse_csv_arg(args.forms) or DEFAULT_FORMS,
            event_terms=parse_csv_arg(args.event_terms) or DEFAULT_EVENT_TERMS,
            size=args.size,
            max_symbols=args.max_symbols,
            timeout_seconds=args.timeout_seconds,
            sleep_seconds=args.sleep_seconds,
            retries=args.retries,
        )
        result = search_edgar_full_text(config)
    except Exception as exc:
        get_logger(__name__).exception(
            "EDGAR full text search failed",
            extra={"event": "edgar_full_text_search_failed", "detail": {"error": repr(exc)}},
        )
        return 1
    get_logger(__name__).info(
        "EDGAR full text search complete",
        extra={"event": "edgar_full_text_search_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template-path", default=str(DEFAULT_TEMPLATE_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--symbols")
    parser.add_argument("--user-agent")
    parser.add_argument("--forms", default=",".join(DEFAULT_FORMS))
    parser.add_argument("--event-terms", default=",".join(DEFAULT_EVENT_TERMS))
    parser.add_argument("--size", type=int, default=DEFAULT_SIZE)
    parser.add_argument("--max-symbols", type=int)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    return parser.parse_args()


def parse_csv_arg(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def resolve_user_agent(value: str | None) -> str:
    user_agent = value or os.getenv("SEC_USER_AGENT")
    if not user_agent:
        raise ValueError("provide --user-agent or set SEC_USER_AGENT before hitting EDGAR")
    return user_agent


def search_edgar_full_text(config: EdgarFullTextConfig) -> dict[str, Any]:
    validate_config(config)
    config.output_root.mkdir(parents=True, exist_ok=True)
    targets = load_targets(config)
    rows = []
    raw_payloads = []
    for target in targets:
        query = query_for_symbol(target["symbol"], config.event_terms)
        try:
            payload = request_search(config, target, query)
        except Exception as exc:
            error = repr(exc)
            detail = {"query": query, "error": error}
            extra = {
                "event": "edgar_full_text_symbol_failed",
                "symbol": target["symbol"],
                "detail": detail,
            }
            if isinstance(exc, requests.RequestException):
                get_logger(__name__).warning("EDGAR full text symbol request failed", extra=extra)
            else:
                get_logger(__name__).exception("EDGAR full text symbol search failed", extra=extra)
            rows.append(error_result_row(target, query, error))
            time.sleep(config.sleep_seconds)
            continue
        raw_payloads.append({"symbol": target["symbol"], "query": query, "payload": payload})
        rows.extend(rows_for_hits(target, query, payload))
        time.sleep(config.sleep_seconds)
    leads = pl.DataFrame(rows, schema=LEAD_SCHEMA, orient="row")
    summary = build_summary(config, targets, leads)
    write_outputs(config.output_root, leads, raw_payloads, summary)
    return {"summary": summary, "rows": leads.to_dicts()}


def validate_config(config: EdgarFullTextConfig) -> None:
    if not config.template_path.exists() and not config.symbols:
        raise FileNotFoundError(f"resolution template does not exist: {config.template_path}")
    if config.size <= 0:
        raise ValueError("--size must be positive")
    if config.max_symbols is not None and config.max_symbols <= 0:
        raise ValueError("--max-symbols must be positive")
    if config.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be positive")
    if config.sleep_seconds < 0:
        raise ValueError("--sleep-seconds cannot be negative")
    if config.retries <= 0:
        raise ValueError("--retries must be positive")


def load_targets(config: EdgarFullTextConfig) -> list[dict[str, Any]]:
    if config.symbols:
        targets = [
            {"symbol": symbol.upper(), "first_day": None, "last_day": None}
            for symbol in config.symbols
        ]
    else:
        frame = pl.read_csv(config.template_path, infer_schema_length=0)
        required = ["symbol", "symbol_era_id", "first_day", "last_day", "priority_rank"]
        missing = [column for column in required if column not in frame.columns]
        if missing:
            raise ValueError(f"resolution template missing required columns: {missing}")
        targets = frame.select(required).to_dicts()
    return targets[: config.max_symbols] if config.max_symbols else targets


def query_for_symbol(symbol: str, event_terms: tuple[str, ...]) -> str:
    return " OR ".join(event_terms)


def request_search(
    config: EdgarFullTextConfig, target: dict[str, Any], query: str
) -> dict[str, Any]:
    params = search_params(config, target, query, include_forms=True)
    try:
        response = request_with_retries(config, params)
    except Exception as exc:
        fallback = search_params(config, target, query, include_forms=False)
        get_logger(__name__).info(
            "EDGAR full text retrying without forms",
            extra={
                "event": "edgar_full_text_without_forms",
                "symbol": target["symbol"],
                "detail": {"error": repr(exc), "params": fallback},
            },
        )
        response = request_with_retries(config, fallback)
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"SEC full-text response must be a JSON object for {target['symbol']}")
    return payload


def rows_for_hits(
    target: dict[str, Any], query: str, payload: dict[str, Any]
) -> list[dict[str, Any]]:
    hits = extract_hits(payload)
    if not hits:
        return [empty_result_row(target, query, total_hits(payload))]
    rows = []
    for rank, hit in enumerate(hits, start=1):
        source = hit.get("_source", hit) if isinstance(hit, dict) else {}
        rows.append(result_row(target, query, rank, source, total_hits(payload)))
    return rows


def extract_hits(payload: dict[str, Any]) -> list[dict[str, Any]]:
    hits = payload.get("hits")
    if isinstance(hits, dict) and isinstance(hits.get("hits"), list):
        return [hit for hit in hits["hits"] if isinstance(hit, dict)]
    if isinstance(hits, list):
        return [hit for hit in hits if isinstance(hit, dict)]
    return []


def total_hits(payload: dict[str, Any]) -> int:
    hits = payload.get("hits")
    if isinstance(hits, dict):
        total = hits.get("total")
        if isinstance(total, dict):
            return int(total.get("value") or 0)
        if isinstance(total, int):
            return total
    total = payload.get("total")
    return int(total) if isinstance(total, int) else 0


def empty_result_row(target: dict[str, Any], query: str, total: int) -> dict[str, Any]:
    row = base_row(target, query, total)
    row["search_status"] = "no_hits"
    return row


def error_result_row(target: dict[str, Any], query: str, error: str) -> dict[str, Any]:
    row = base_row(target, query, 0)
    row["search_status"] = "search_error"
    row["document_url"] = error
    return row


def result_row(
    target: dict[str, Any], query: str, rank: int, source: dict[str, Any], total: int
) -> dict[str, Any]:
    row = base_row(target, query, total)
    row.update(
        {
            "search_status": "hit",
            "hit_rank": rank,
            "cik": first_value(source, ["cik", "ciks"]),
            "entity": first_value(source, ["entity", "company", "display_names", "companyName"]),
            "form": first_value(source, ["form", "root_form"]),
            "filed_at": first_value(source, ["file_date", "filedAt", "filing_date"]),
            "accession_no": first_value(source, ["adsh", "accession_no", "accessionNumber"]),
            "document_url": first_value(source, ["documentUrl", "file_url", "url"]),
        }
    )
    return row


def base_row(target: dict[str, Any], query: str, total: int) -> dict[str, Any]:
    return {
        "priority_rank": target.get("priority_rank"),
        "symbol": str(target["symbol"]).upper(),
        "symbol_era_id": target.get("symbol_era_id"),
        "first_day": target.get("first_day"),
        "last_day": target.get("last_day"),
        "query": query,
        "search_status": "not_searched",
        "total_hits": total,
        "hit_rank": None,
        "cik": None,
        "entity": None,
        "form": None,
        "filed_at": None,
        "accession_no": None,
        "document_url": None,
    }


def first_value(source: dict[str, Any], keys: list[str]) -> str | None:
    for key in keys:
        value = source.get(key)
        if isinstance(value, list):
            value = value[0] if value else None
        if value not in (None, ""):
            return str(value)
    return None


if __name__ == "__main__":
    raise SystemExit(main())
