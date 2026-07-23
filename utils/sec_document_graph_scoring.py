from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

DEFAULT_SEC_DOC_TOP_N = 5


def load_sec_evidence(path: Path | None) -> pl.DataFrame:
    if path is None or not path.exists():
        return pl.DataFrame()
    return pl.read_csv(path, infer_schema_length=0)


def build_sec_doc_graph(evidence: pl.DataFrame, top_n: int) -> pl.DataFrame:
    if evidence.is_empty() or "symbol_era_id" not in evidence.columns:
        return empty_doc_graph()
    rows = [score_sec_doc(row) for row in evidence.to_dicts()]
    scored = pl.DataFrame(rows).sort(["symbol_era_id", "sec_doc_score"], descending=[False, True])
    return (
        scored.group_by("symbol_era_id", maintain_order=True)
        .head(top_n)
        .group_by("symbol_era_id", maintain_order=True)
        .agg(doc_graph_aggs())
    )


def empty_doc_graph() -> pl.DataFrame:
    schema = {
        "symbol_era_id": pl.String,
        "sec_doc_graph_bucket": pl.String,
        "sec_doc_graph_score": pl.Int64,
        "sec_doc_graph_doc_count": pl.Int64,
        "sec_doc_graph_reasons": pl.String,
        "sec_doc_graph_top_docs": pl.String,
    }
    return pl.DataFrame(schema=schema)


def doc_graph_aggs() -> list[pl.Expr]:
    return [
        pl.col("sec_doc_bucket").first().alias("sec_doc_graph_bucket"),
        pl.col("sec_doc_score").max().alias("sec_doc_graph_score"),
        pl.len().alias("sec_doc_graph_doc_count"),
        pl.col("sec_doc_reason").unique().str.join(" | ").alias("sec_doc_graph_reasons"),
        pl.col("sec_doc_ref").str.join(" | ").alias("sec_doc_graph_top_docs"),
    ]


def score_sec_doc(row: dict[str, Any]) -> dict[str, Any]:
    flags = evidence_flags(row)
    score = sec_doc_score(flags, row)
    return {
        "symbol_era_id": row.get("symbol_era_id"),
        "sec_doc_bucket": sec_doc_bucket(flags, score),
        "sec_doc_score": score,
        "sec_doc_reason": ",".join(flags) or "no_combined_evidence",
        "sec_doc_ref": sec_doc_ref(row, score),
    }


def evidence_flags(row: dict[str, Any]) -> list[str]:
    flags = split_flags(row.get("verifier_flags"))
    reason = str(row.get("triage_reason") or "")
    if "symbol_token_in_entity" in reason or "query_alias_in_entity" in reason:
        flags.append("issuer_or_symbol_context")
    if terminal_date_proximity(row):
        flags.append("terminal_date_proximity")
    return sorted(set(flags))


def split_flags(value: Any) -> list[str]:
    return [part for part in str(value or "").split("|") if part]


def terminal_date_proximity(row: dict[str, Any]) -> bool:
    filed = parse_date(row.get("filed_at"))
    last = parse_date(row.get("last_day"))
    return bool(filed and last and abs((filed - last).days) <= 730)


def sec_doc_score(flags: list[str], row: dict[str, Any]) -> int:
    weights = {
        "issuer_name_match": 25,
        "symbol_match": 15,
        "issuer_or_symbol_context": 10,
        "event_language": 25,
        "completion_language": 30,
        "delisting_language": 25,
        "terminal_date_proximity": 20,
        "going_private_form": 15,
    }
    triage_score = int_or_zero(row.get("triage_score"))
    return sum(weights.get(flag, 0) for flag in flags) + min(triage_score // 10, 15)


def sec_doc_bucket(flags: list[str], score: int) -> str:
    flag_set = set(flags)
    identity = {"issuer_name_match", "symbol_match", "issuer_or_symbol_context"}
    event = {"event_language", "completion_language", "delisting_language"}
    has_identity = bool(identity & flag_set)
    has_event = bool(event & flag_set)
    if score >= 80 and has_identity and has_event:
        return "auto_verified_candidate"
    if has_identity and has_event:
        return "issuer_event_review"
    if has_event:
        return "event_only_review"
    return "manual_review"


def sec_doc_ref(row: dict[str, Any], score: int) -> str:
    form = row.get("form") or ""
    filed = row.get("filed_at") or ""
    url = row.get("verifier_document_url") or row.get("primary_source_url") or ""
    return f"score={score};form={form};filed_at={filed};url={url}"


def parse_date(value: Any) -> date | None:
    text = re.sub(r"[^0-9-]", "", str(value or ""))
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def int_or_zero(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
