from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import requests

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.sec_candidate_verifier_schema import (
    COMPLETION_TERMS,
    DEFAULT_CANDIDATES_PATH,
    DEFAULT_LOG_PATH,
    DEFAULT_OUTPUT_PATH,
    DEFAULT_SUMMARY_PATH,
    DELISTING_TERMS,
    EVENT_TERMS,
    REQUIRED_COLUMNS,
    VerifyConfig,
)


def main() -> int:
    args = parse_args()
    setup_logging(str(DEFAULT_LOG_PATH))
    try:
        config = VerifyConfig(
            candidates_path=Path(args.candidates_path),
            output_path=Path(args.output_path),
            summary_path=Path(args.summary_path),
            user_agent=resolve_user_agent(args.user_agent),
            timeout_seconds=args.timeout_seconds,
            sleep_seconds=args.sleep_seconds,
            max_rows=args.max_rows,
        )
        result = verify_sec_override_candidates(config)
    except Exception as exc:
        get_logger(__name__).exception(
            "SEC override candidate verifier failed",
            extra={"event": "sec_candidate_verifier_failed", "detail": {"error": repr(exc)}},
        )
        return 1
    get_logger(__name__).info(
        "SEC override candidate verifier complete",
        extra={"event": "sec_candidate_verifier_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates-path", default=str(DEFAULT_CANDIDATES_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--user-agent")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--max-rows", type=int)
    return parser.parse_args()


def verify_sec_override_candidates(config: VerifyConfig) -> dict[str, Any]:
    validate_config(config)
    candidates = pl.read_csv(config.candidates_path, infer_schema_length=0)
    missing = [column for column in REQUIRED_COLUMNS if column not in candidates.columns]
    if missing:
        raise ValueError(f"candidate file missing required columns: {missing}")
    rows = candidates.head(config.max_rows) if config.max_rows else candidates
    verified = [verify_row(row, config) for row in rows.to_dicts()]
    output = pl.DataFrame(verified)
    summary = build_summary(config, output)
    write_outputs(config, output, summary)
    return {"summary": summary, "rows": output.to_dicts()}


def validate_config(config: VerifyConfig) -> None:
    if not config.candidates_path.exists():
        raise FileNotFoundError(f"candidate file does not exist: {config.candidates_path}")
    if config.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be positive")
    if config.sleep_seconds < 0:
        raise ValueError("--sleep-seconds cannot be negative")
    if config.max_rows is not None and config.max_rows <= 0:
        raise ValueError("--max-rows must be positive")


def resolve_user_agent(value: str | None) -> str:
    user_agent = value or os.getenv("SEC_USER_AGENT")
    if not user_agent:
        raise ValueError("provide --user-agent or set SEC_USER_AGENT before fetching SEC filings")
    return user_agent


def verify_row(row: dict[str, Any], config: VerifyConfig) -> dict[str, Any]:
    try:
        document_url = resolve_document_url(row["primary_source_url"], config)
        text = fetch_text(document_url, config)
        result = score_text(row, text, document_url, None)
    except Exception as exc:
        result = score_text(row, "", None, repr(exc))
    time.sleep(config.sleep_seconds)
    return {**row, **result}


def resolve_document_url(archive_url: str, config: VerifyConfig) -> str:
    base = archive_url.rstrip("/")
    payload = fetch_json(f"{base}/index.json", config)
    items = payload.get("directory", {}).get("item", [])
    if not isinstance(items, list):
        raise ValueError(f"SEC archive index did not include item list: {base}")
    names = [str(item.get("name")) for item in items if isinstance(item, dict)]
    html_names = [name for name in names if name.lower().endswith((".htm", ".html"))]
    primary_names = [name for name in html_names if is_primary_filing_document(name)]
    if not primary_names:
        raise ValueError(f"SEC archive index did not include filing HTML: {base}")
    return f"{base}/{primary_names[0]}"


def is_primary_filing_document(name: str) -> bool:
    lowered = name.lower()
    skipped_fragments = (
        "index",
        "filingsummary",
        "exhibit",
        "ex-",
        "ex_",
        "dex",
        "ex23",
        "xex",
        "exfilingfees",
    )
    return not any(fragment in lowered for fragment in skipped_fragments)


def fetch_json(url: str, config: VerifyConfig) -> dict[str, Any]:
    response = requests.get(url, headers=headers(config), timeout=config.timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"SEC JSON response must be an object: {url}")
    return payload


def fetch_text(url: str, config: VerifyConfig) -> str:
    response = requests.get(url, headers=headers(config), timeout=config.timeout_seconds)
    response.raise_for_status()
    return html_to_text(response.text)


def headers(config: VerifyConfig) -> dict[str, str]:
    return {"User-Agent": config.user_agent, "Accept-Encoding": "gzip, deflate"}


def score_text(
    row: dict[str, Any], text: str, document_url: str | None, error: str | None
) -> dict[str, Any]:
    if error:
        return verifier_result("fetch_error", 0, (), document_url, error)
    normalized = normalize_text(text)
    flags = evidence_flags(row, normalized)
    score = evidence_score(flags)
    return verifier_result(bucket_for_score(score, flags), score, flags, document_url, None)


def evidence_flags(row: dict[str, Any], text: str) -> tuple[str, ...]:
    flags = []
    if issuer_matches(row["proposed_historical_issuer_name"], text):
        flags.append("issuer_name_match")
    if symbol_matches(row["symbol"], text):
        flags.append("symbol_match")
    if has_any(text, EVENT_TERMS):
        flags.append("event_language")
    if has_any(text, COMPLETION_TERMS):
        flags.append("completion_language")
    if has_any(text, DELISTING_TERMS):
        flags.append("delisting_language")
    if str(row.get("form") or "").upper().strip() in {"SC 13E3", "SC 13E3/A"}:
        flags.append("going_private_form")
    return tuple(flags)


def evidence_score(flags: tuple[str, ...]) -> int:
    weights = {
        "issuer_name_match": 20,
        "symbol_match": 10,
        "event_language": 25,
        "completion_language": 35,
        "delisting_language": 25,
        "going_private_form": 15,
    }
    return sum(weights[flag] for flag in flags)


def bucket_for_score(score: int, flags: tuple[str, ...]) -> str:
    has_identity = "issuer_name_match" in flags or "symbol_match" in flags
    has_event = "event_language" in flags or "completion_language" in flags
    if score >= 70 and has_identity and has_event:
        return "strong_review_candidate"
    if score >= 45 and has_event:
        return "moderate_review_candidate"
    return "weak_review_candidate"


def verifier_result(
    bucket: str,
    score: int,
    flags: tuple[str, ...],
    document_url: str | None,
    error: str | None,
) -> dict[str, Any]:
    return {
        "verifier_bucket": bucket,
        "verifier_score": score,
        "verifier_flags": "|".join(flags),
        "verifier_document_url": document_url or "",
        "verifier_error": error or "",
    }


def issuer_matches(issuer: Any, text: str) -> bool:
    tokens = [token for token in normalize_text(issuer).split() if len(token) >= 4]
    if not tokens:
        return False
    required = min(2, len(tokens))
    return sum(1 for token in tokens if token in text) >= required


def symbol_matches(symbol: Any, text: str) -> bool:
    symbol_text = normalize_text(symbol)
    return len(symbol_text) > 1 and bool(re.search(rf"\b{re.escape(symbol_text)}\b", text))


def has_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(normalize_text(term) in text for term in terms)


def normalize_text(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()


def html_to_text(value: str) -> str:
    without_scripts = re.sub(r"(?is)<(script|style).*?</\1>", " ", value)
    without_tags = re.sub(r"(?s)<[^>]+>", " ", without_scripts)
    return normalize_text(without_tags)


def build_summary(config: VerifyConfig, output: pl.DataFrame) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "SEC filing text verifier for dead-ticker override candidates",
        "candidates_path": str(config.candidates_path),
        "output_path": str(config.output_path),
        "row_count": output.height,
        "bucket_counts": count_by(output, "verifier_bucket"),
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(config: VerifyConfig, output: pl.DataFrame, summary: dict[str, Any]) -> None:
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    config.summary_path.parent.mkdir(parents=True, exist_ok=True)
    output.write_csv(config.output_path)
    config.summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
