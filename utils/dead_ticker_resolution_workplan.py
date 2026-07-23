from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT
from utils.instrument_research_routing import (
    ROUTE_MANUAL_SYNTAX_REVIEW,
    ROUTE_OPERATING_COMPANY_SEC_EVENT,
    ROUTE_PREFERRED_REDEMPTION_OR_DELISTING,
    ROUTE_SECURITY_ACTION,
    ROUTE_SHARE_CLASS_CORPORATE_ACTION,
)
from utils.dead_ticker_workplan_ledger import build_low_materiality_ledger_candidates
from utils.dead_ticker_workplan_outputs import build_summary, write_outputs
from utils.dead_ticker_workplan_automation import attach_automation_status
from utils.sec_document_graph_scoring import (
    DEFAULT_SEC_DOC_TOP_N,
    build_sec_doc_graph,
    load_sec_evidence,
)

DEFAULT_LANES_PATH = DEFAULT_OUTPUT_ROOT / "resolution-lanes" / "resolution_lanes.csv"
DEFAULT_WORKPLAN_ROOT = DEFAULT_OUTPUT_ROOT / "resolution-workplan"
DEFAULT_SEC_EVIDENCE_PATH = DEFAULT_OUTPUT_ROOT / "sec_override_candidates_verified_triage.csv"
DEFAULT_AUTOMATION_PATH = DEFAULT_WORKPLAN_ROOT / "identity_resolution_automation.csv"
HIGH_IMPACT_RANK_LIMIT = 2_000
HIGH_IMPACT_TRADE_ROWS = 100_000
LOW_MATERIALITY_TRADE_ROWS = 99
LOW_MATERIALITY_OBSERVED_DAYS = 5
OPERATING_LIFECYCLE_LANES = {
    "operating_partial_window",
    "operating_sparse_intermit",
    "operating_intermit_material",
}
DERIVATIVE_ROUTES = {
    ROUTE_PREFERRED_REDEMPTION_OR_DELISTING,
    ROUTE_SECURITY_ACTION,
    ROUTE_SHARE_CLASS_CORPORATE_ACTION,
}
CURRENT_SEC_CONFIDENCE = {"sec_current_match", "sec_multiple_current_matches"}
CURRENT_IEX_CONFIDENCE = {
    "iex_snapshot_overlap",
    "iex_snapshot_changed_during_window",
    "iex_snapshot_removed_before_latest",
    "iex_current_symbol_only",
}


@dataclass(frozen=True)
class ResolutionWorkplanConfig:
    lanes_path: Path
    output_root: Path
    sec_evidence_path: Path | None = DEFAULT_SEC_EVIDENCE_PATH
    sec_doc_top_n: int = DEFAULT_SEC_DOC_TOP_N
    automation_path: Path | None = DEFAULT_AUTOMATION_PATH


def build_resolution_workplan(config: ResolutionWorkplanConfig) -> dict[str, Any]:
    validate_config(config)
    lanes = read_lanes(config.lanes_path)
    workplan = add_workplan_columns(add_impact_columns(lanes))
    doc_graph = build_sec_doc_graph(
        load_sec_evidence(config.sec_evidence_path), config.sec_doc_top_n
    )
    workplan = attach_automation_status(
        attach_doc_graph(workplan, doc_graph), config.automation_path
    )
    low_candidates = build_low_materiality_ledger_candidates(workplan)
    summary = build_summary(config, workplan, low_candidates, doc_graph)
    write_outputs(config.output_root, workplan, low_candidates, doc_graph, summary)
    return {"summary": summary, "rows": workplan.to_dicts()}


def validate_config(config: ResolutionWorkplanConfig) -> None:
    if not config.lanes_path.exists():
        raise FileNotFoundError(f"resolution lanes CSV does not exist: {config.lanes_path}")
    if config.sec_doc_top_n <= 0:
        raise ValueError("--sec-doc-top-n must be positive")


def read_lanes(path: Path) -> pl.DataFrame:
    lanes = pl.read_csv(path, infer_schema_length=0)
    missing = [column for column in required_lane_columns() if column not in lanes.columns]
    if missing:
        raise ValueError(f"resolution lanes missing required columns: {missing}")
    return lanes.with_columns(
        pl.col("trade_rows").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("observed_days").cast(pl.Int64, strict=False).fill_null(0),
    )


def required_lane_columns() -> list[str]:
    return [
        "symbol",
        "symbol_era_id",
        "source_classification",
        "research_route",
        "instrument_type",
        "resolution_lane",
        "first_day",
        "last_day",
        "observed_days",
        "trade_rows",
        "sec_current_confidence",
        "iex_entity_confidence",
    ]


def add_impact_columns(lanes: pl.DataFrame) -> pl.DataFrame:
    total = int(lanes["trade_rows"].sum() or 0)
    return (
        lanes.sort(["trade_rows", "symbol_era_id"], descending=[True, False])
        .with_row_index("impact_rank", offset=1)
        .with_columns(pl.col("trade_rows").cum_sum().alias("cumulative_trade_rows"))
        .with_columns(cumulative_pct_expr(total).alias("cumulative_trade_rows_pct"))
    )


def cumulative_pct_expr(total_trade_rows: int) -> pl.Expr:
    if total_trade_rows <= 0:
        return pl.lit(0.0)
    return (pl.col("cumulative_trade_rows") / total_trade_rows * 100).round(6)


def add_workplan_columns(frame: pl.DataFrame) -> pl.DataFrame:
    return pl.concat(
        [frame, pl.DataFrame([workplan_row(row) for row in frame.to_dicts()])], how="horizontal"
    )


def workplan_row(row: dict[str, Any]) -> dict[str, str]:
    if is_high_impact_operating(row):
        return high_impact_bucket()
    if is_derivative_parent(row):
        return derivative_parent_bucket()
    if is_low_materiality(row):
        return low_materiality_bucket(row)
    if is_operating_lifecycle(row):
        return lifecycle_bucket()
    return manual_hold_bucket()


def high_impact_bucket() -> dict[str, str]:
    reason = "operating row in top impact set or >=100K trade rows"
    return bucket(
        "high_impact_operating",
        "SEC document graph issuer/event review",
        "search_edgar_full_text.py + verifier",
        reason,
    )


def derivative_parent_bucket() -> dict[str, str]:
    reason = "preferred, warrant, unit, right, or share-class research route"
    return bucket(
        "derivative_parent_resolution",
        "Resolve through parent/root security disposition",
        "build_parent_security_resolution_candidates.py",
        reason,
    )


def low_materiality_bucket(row: dict[str, Any]) -> dict[str, str]:
    tool = "import_ticker_era_resolution_ledger.py --dry-run"
    return bucket(
        "low_materiality_bulk_disposition",
        "Generate dry-run ledger disposition candidate",
        tool,
        low_materiality_reason(row),
    )


def lifecycle_bucket() -> dict[str, str]:
    action = "Search around first_day and last_day for lifecycle event"
    reason = "operating partial/intermittent row outside terminal-only path"
    return bucket("operating_lifecycle_search", action, "search_edgar_full_text.py", reason)


def manual_hold_bucket() -> dict[str, str]:
    reason = "no deterministic workplan routing rule matched"
    return bucket(
        "manual_review_hold", "Hold for manual syntax or evidence review", "manual review", reason
    )


def bucket(name: str, action: str, tool: str, reason: str) -> dict[str, str]:
    return {
        "workplan_bucket": name,
        "recommended_action": action,
        "recommended_tool": tool,
        "workplan_reason": reason,
    }


def is_high_impact_operating(row: dict[str, Any]) -> bool:
    return is_operating(row) and (
        int(row["impact_rank"]) <= HIGH_IMPACT_RANK_LIMIT
        or int(row["trade_rows"]) >= HIGH_IMPACT_TRADE_ROWS
    )


def is_derivative_parent(row: dict[str, Any]) -> bool:
    return str(row.get("research_route") or "") in DERIVATIVE_ROUTES


def is_low_materiality(row: dict[str, Any]) -> bool:
    return (
        int(row["trade_rows"]) <= LOW_MATERIALITY_TRADE_ROWS
        and int(row["observed_days"]) <= LOW_MATERIALITY_OBSERVED_DAYS
        and not has_current_sec_or_iex_evidence(row)
        and not is_high_impact_operating(row)
        and not is_derivative_parent(row)
        and not is_manual_syntax(row)
    )


def is_operating_lifecycle(row: dict[str, Any]) -> bool:
    return is_operating(row) and str(row.get("resolution_lane") or "") in OPERATING_LIFECYCLE_LANES


def is_operating(row: dict[str, Any]) -> bool:
    return str(row.get("research_route") or "") == ROUTE_OPERATING_COMPANY_SEC_EVENT


def is_manual_syntax(row: dict[str, Any]) -> bool:
    return (
        str(row.get("research_route") or "") == ROUTE_MANUAL_SYNTAX_REVIEW
        or str(row.get("resolution_lane") or "") == "manual_syntax"
    )


def has_current_sec_or_iex_evidence(row: dict[str, Any]) -> bool:
    return (
        str(row.get("sec_current_confidence") or "") in CURRENT_SEC_CONFIDENCE
        or str(row.get("iex_entity_confidence") or "") in CURRENT_IEX_CONFIDENCE
    )


def low_materiality_reason(row: dict[str, Any]) -> str:
    return (
        f"trade_rows={row['trade_rows']} <= {LOW_MATERIALITY_TRADE_ROWS}; "
        f"observed_days={row['observed_days']} <= {LOW_MATERIALITY_OBSERVED_DAYS}; "
        "no current SEC/IEX evidence"
    )


def attach_doc_graph(workplan: pl.DataFrame, doc_graph: pl.DataFrame) -> pl.DataFrame:
    if doc_graph.is_empty():
        return workplan.with_columns(empty_doc_graph_exprs())
    return workplan.join(doc_graph, on="symbol_era_id", how="left").with_columns(
        fill_doc_graph_exprs()
    )


def empty_doc_graph_exprs() -> list[pl.Expr]:
    return [
        pl.lit("not_scored").alias("sec_doc_graph_bucket"),
        pl.lit(0).alias("sec_doc_graph_score"),
        pl.lit(0).alias("sec_doc_graph_doc_count"),
        pl.lit("").alias("sec_doc_graph_reasons"),
        pl.lit("").alias("sec_doc_graph_top_docs"),
    ]


def fill_doc_graph_exprs() -> list[pl.Expr]:
    return [
        pl.col("sec_doc_graph_bucket").fill_null("not_scored"),
        pl.col("sec_doc_graph_score").fill_null(0),
        pl.col("sec_doc_graph_doc_count").fill_null(0),
        pl.col("sec_doc_graph_reasons").fill_null(""),
        pl.col("sec_doc_graph_top_docs").fill_null(""),
    ]
