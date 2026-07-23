from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.edgar_full_text_triage_schema import (
    DEAL_FORMS,
    DEFAULT_LEADS_PATH,
    DEFAULT_SUMMARY_PATH,
    DEFAULT_TRIAGE_CSV,
    DEFAULT_TRIAGE_PARQUET,
    EVENT_WORDS,
    EXIT_FORMS,
    REVIEW_FORMS,
    TRIAGE_SCHEMA,
    TriageConfig,
)


def main() -> int:
    args = parse_args()
    setup_logging(str(Path(args.summary_path).with_suffix(".jsonl")))
    try:
        result = triage_edgar_full_text_leads(
            TriageConfig(
                leads_path=Path(args.leads_path),
                output_csv=Path(args.output_csv),
                output_parquet=Path(args.output_parquet),
                summary_path=Path(args.summary_path),
                top_n=args.top_n,
            )
        )
    except Exception as exc:
        get_logger(__name__).exception(
            "EDGAR full text triage failed",
            extra={"event": "edgar_full_text_triage_failed", "detail": {"error": repr(exc)}},
        )
        return 1
    get_logger(__name__).info(
        "EDGAR full text triage complete",
        extra={"event": "edgar_full_text_triage_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leads-path", default=str(DEFAULT_LEADS_PATH))
    parser.add_argument("--output-csv", default=str(DEFAULT_TRIAGE_CSV))
    parser.add_argument("--output-parquet", default=str(DEFAULT_TRIAGE_PARQUET))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--top-n", type=int, default=5)
    return parser.parse_args()


def triage_edgar_full_text_leads(config: TriageConfig) -> dict[str, Any]:
    validate_config(config)
    leads = read_leads(config.leads_path)
    rows = rank_rows(leads.to_dicts(), config.top_n)
    triage = pl.DataFrame(rows, schema=TRIAGE_SCHEMA, orient="row")
    summary = build_summary(config, leads, triage)
    write_outputs(config, triage, summary)
    return {"summary": summary, "rows": triage.to_dicts()}


def validate_config(config: TriageConfig) -> None:
    if not config.leads_path.exists():
        raise FileNotFoundError(f"EDGAR full-text leads file does not exist: {config.leads_path}")
    if config.top_n <= 0:
        raise ValueError("--top-n must be positive")


def read_leads(path: Path) -> pl.DataFrame:
    if path.suffix == ".parquet":
        return pl.read_parquet(path)
    return pl.read_csv(path, infer_schema_length=0)


def rank_rows(rows: list[dict[str, Any]], top_n: int) -> list[dict[str, Any]]:
    scored = [triage_row(row) for row in rows if row.get("search_status") == "hit"]
    ordered = sorted(scored, key=sort_key)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in ordered:
        grouped.setdefault(str(row["symbol_era_id"] or row["symbol"]), []).append(row)
    return top_ranked_rows(grouped, top_n)


def top_ranked_rows(grouped: dict[str, list[dict[str, Any]]], top_n: int) -> list[dict[str, Any]]:
    rows = []
    for group_rows in grouped.values():
        for rank, row in enumerate(group_rows[:top_n], start=1):
            row["triage_rank"] = rank
            rows.append(row)
    return sorted(rows, key=lambda row: (priority_sort(row.get("priority_rank")), row["triage_rank"]))


def triage_row(row: dict[str, Any]) -> dict[str, Any]:
    form_strength, form_score = score_form(row.get("form"))
    date_relation, date_score = score_date(row.get("filed_at"), row.get("first_day"), row.get("last_day"))
    entity_match, entity_score = score_entity(row.get("symbol"), row.get("entity"), row.get("query"))
    rank_score = max(0, 11 - int(row.get("hit_rank") or 99))
    score = form_score + date_score + entity_score + rank_score
    bucket = confidence_bucket(form_strength, date_relation, entity_match)
    return output_row(row, score, bucket, form_strength, date_relation, entity_match)


def score_form(value: Any) -> tuple[str, int]:
    form = normalize_form(value)
    if form in DEAL_FORMS:
        return "deal_form", 60
    if form in EXIT_FORMS:
        return "exchange_exit_form", 55
    if form in REVIEW_FORMS:
        return "review_form", 25
    return "weak_form", 5


def score_date(filed_at: Any, first_day: Any, last_day: Any) -> tuple[str, int]:
    filed = parse_date(filed_at)
    first = parse_date(first_day)
    last = parse_date(last_day)
    if not filed or not first or not last:
        return "unknown_date", 0
    if first <= filed <= last:
        return "inside_symbol_era", 30
    if 0 <= (filed - last).days <= 730:
        return "near_after_symbol_era", 20
    if 0 <= (first - filed).days <= 365:
        return "near_before_symbol_era", 10
    return "outside_symbol_era", -15


def score_entity(symbol: Any, entity: Any, query: Any) -> tuple[str, int]:
    entity_text = normalize_text(entity)
    symbol_text = normalize_text(symbol)
    if not entity_text or not symbol_text:
        return "unknown_entity", 0
    if len(symbol_text) > 1 and has_token(entity_text, symbol_text):
        return "symbol_token_in_entity", 20
    if query_alias_matches(entity_text, query):
        return "query_alias_in_entity", 15
    if len(symbol_text) <= 2:
        return "short_symbol_no_entity_match", -15
    return "no_entity_match", 0


def confidence_bucket(form_strength: str, date_relation: str, entity_match: str) -> str:
    if entity_match == "short_symbol_no_entity_match":
        return "manual_review_lead"
    strong_form = form_strength in {"deal_form", "exchange_exit_form"}
    useful_date = date_relation in {"inside_symbol_era", "near_after_symbol_era"}
    useful_entity = entity_match in {"symbol_token_in_entity", "query_alias_in_entity"}
    if strong_form and useful_date and useful_entity:
        return "high_confidence_lead"
    if strong_form and (useful_date or useful_entity):
        return "medium_confidence_lead"
    if form_strength == "review_form" and useful_date and useful_entity:
        return "medium_confidence_lead"
    return "manual_review_lead"


def output_row(
    row: dict[str, Any],
    score: int,
    bucket: str,
    form_strength: str,
    date_relation: str,
    entity_match: str,
) -> dict[str, Any]:
    result = {column: row.get(column) for column in TRIAGE_SCHEMA if column not in generated_columns()}
    result.update(
        {
            "triage_rank": 0,
            "triage_bucket": bucket,
            "triage_score": score,
            "triage_reason": f"{form_strength}; {date_relation}; {entity_match}",
            "form_strength": form_strength,
            "date_relation": date_relation,
            "entity_match": entity_match,
        }
    )
    return result


def generated_columns() -> set[str]:
    return {
        "triage_rank",
        "triage_bucket",
        "triage_score",
        "triage_reason",
        "form_strength",
        "date_relation",
        "entity_match",
    }


def sort_key(row: dict[str, Any]) -> tuple[int, int, int]:
    return (
        priority_sort(row.get("priority_rank")),
        -int(row["triage_score"]),
        int(row.get("hit_rank") or 99),
    )


def priority_sort(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 999_999


def normalize_form(value: Any) -> str:
    return str(value or "").upper().strip()


def normalize_text(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()


def has_token(haystack: str, needle: str) -> bool:
    return bool(re.search(rf"(^| )({re.escape(needle)})( |$)", haystack))


def query_alias_matches(entity_text: str, query: Any) -> bool:
    for token in alias_tokens(query):
        if token not in EVENT_WORDS and len(token) >= 3 and token in entity_text:
            return True
    return False


def alias_tokens(query: Any) -> list[str]:
    text = normalize_text(query)
    return [token for token in text.split() if token not in {"AND", "OR"}]


def parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def build_summary(config: TriageConfig, leads: pl.DataFrame, triage: pl.DataFrame) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "Local triage ranking for SEC EDGAR full-text leads",
        "leads_path": str(config.leads_path),
        "top_n": config.top_n,
        "input_rows": leads.height,
        "triage_rows": triage.height,
        "symbols_ranked": triage.select("symbol").n_unique() if triage.height else 0,
        "bucket_counts": count_by(triage, "triage_bucket"),
        "form_strength_counts": count_by(triage, "form_strength"),
        "limitations": [
            "Triage ranking is heuristic and does not verify historical ticker identity.",
            "Short ticker symbols still require manual issuer, CIK, filing date, and event review.",
        ],
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    if frame.height == 0:
        return {}
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(config: TriageConfig, triage: pl.DataFrame, summary: dict[str, Any]) -> None:
    config.output_csv.parent.mkdir(parents=True, exist_ok=True)
    config.output_parquet.parent.mkdir(parents=True, exist_ok=True)
    config.summary_path.parent.mkdir(parents=True, exist_ok=True)
    triage.write_csv(config.output_csv)
    triage.write_parquet(config.output_parquet, compression="zstd")
    config.summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
