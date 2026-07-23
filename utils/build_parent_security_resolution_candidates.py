from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.dead_ticker_review_schema import (
    DEFAULT_IEX_ERAS_PATH,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_SEC_ERAS_PATH,
)
from utils.instrument_research_routing import (
    ROUTE_PREFERRED_REDEMPTION_OR_DELISTING,
    ROUTE_SECURITY_ACTION,
    ROUTE_SHARE_CLASS_CORPORATE_ACTION,
)
from utils.resolution_ledger import (
    DEFAULT_RESOLUTION_OUTPUT_ROOT,
    RESOLUTION_COLUMNS,
)

DEFAULT_PRIORITY_QUEUE_PATH = DEFAULT_OUTPUT_ROOT / "unresolved_priority_queue.parquet"
DEFAULT_OUTPUT_PATH = (
    DEFAULT_RESOLUTION_OUTPUT_ROOT / "parent_security_resolution_candidates.csv"
)
CURRENT_SEC_CONFIDENCE = ("sec_current_match", "sec_multiple_current_matches")
CURRENT_IEX_CONFIDENCE = (
    "iex_snapshot_overlap",
    "iex_snapshot_changed_during_window",
    "iex_snapshot_removed_before_latest",
    "iex_current_symbol_only",
)
PARENT_ROUTES = (
    ROUTE_PREFERRED_REDEMPTION_OR_DELISTING,
    ROUTE_SECURITY_ACTION,
    ROUTE_SHARE_CLASS_CORPORATE_ACTION,
)


@dataclass(frozen=True)
class ParentSecurityResolutionConfig:
    priority_queue_path: Path
    sec_eras_path: Path
    iex_eras_path: Path
    output_path: Path
    summary_path: Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--priority-queue-path", default=str(DEFAULT_PRIORITY_QUEUE_PATH))
    parser.add_argument("--sec-eras-path", default=str(DEFAULT_SEC_ERAS_PATH))
    parser.add_argument("--iex-eras-path", default=str(DEFAULT_IEX_ERAS_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument(
        "--summary-path",
        default=str(DEFAULT_RESOLUTION_OUTPUT_ROOT / "parent_security_resolution_summary.json"),
    )
    args = parser.parse_args()
    config = ParentSecurityResolutionConfig(
        priority_queue_path=Path(args.priority_queue_path),
        sec_eras_path=Path(args.sec_eras_path),
        iex_eras_path=Path(args.iex_eras_path),
        output_path=Path(args.output_path),
        summary_path=Path(args.summary_path),
    )
    setup_logging(str(config.output_path.parent / "parent_security_resolution.jsonl"))
    result = build_parent_security_resolution_candidates(config)
    get_logger(__name__).info(
        "parent security resolution candidates complete",
        extra={"event": "parent_security_resolution_complete", "detail": result["summary"]},
    )
    return 0


def build_parent_security_resolution_candidates(
    config: ParentSecurityResolutionConfig,
) -> dict[str, Any]:
    validate_config(config)
    priority = pl.read_parquet(config.priority_queue_path)
    require_priority_columns(priority)
    current_roots = load_current_root_evidence(config.sec_eras_path, config.iex_eras_path)
    candidates = build_candidates(priority, current_roots)
    summary = build_summary(config, priority, candidates)
    write_outputs(config, candidates, summary)
    return {"summary": summary, "rows": candidates.to_dicts()}


def validate_config(config: ParentSecurityResolutionConfig) -> None:
    for path, label in [
        (config.priority_queue_path, "priority queue"),
        (config.sec_eras_path, "SEC enriched eras"),
        (config.iex_eras_path, "IEX enriched eras"),
    ]:
        if not path.exists():
            raise FileNotFoundError(f"{label} does not exist: {path}")


def require_priority_columns(frame: pl.DataFrame) -> None:
    required = [
        "symbol",
        "symbol_era_id",
        "research_route",
        "instrument_type",
        "first_day",
        "last_day",
    ]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"priority queue missing required columns: {missing}")


def load_current_root_evidence(sec_path: Path, iex_path: Path) -> pl.DataFrame:
    sec = pl.read_parquet(sec_path).select(
        ["symbol", "symbol_era_id", "sec_current_confidence", "sec_cik", "sec_name"]
    )
    iex = pl.read_parquet(iex_path).select(
        ["symbol_era_id", "iex_entity_confidence", "iex_latest_issuer"]
    )
    return (
        sec.join(iex, on="symbol_era_id", how="left")
        .filter(
            pl.col("sec_current_confidence").is_in(CURRENT_SEC_CONFIDENCE)
            | pl.col("iex_entity_confidence").is_in(CURRENT_IEX_CONFIDENCE)
        )
        .sort(["symbol", "sec_current_confidence", "iex_entity_confidence"])
        .unique("symbol", keep="first")
        .select(
            pl.col("symbol").alias("parent_root_symbol"),
            "sec_current_confidence",
            "sec_cik",
            "sec_name",
            "iex_entity_confidence",
            "iex_latest_issuer",
        )
    )


def build_candidates(priority: pl.DataFrame, current_roots: pl.DataFrame) -> pl.DataFrame:
    parent_rows = (
        priority.filter(pl.col("research_route").is_in(PARENT_ROUTES))
        .with_columns(parent_root_symbol_expr().alias("parent_root_symbol"))
        .filter(pl.col("parent_root_symbol").is_not_null() & (pl.col("parent_root_symbol") != ""))
        .join(current_roots, on="parent_root_symbol", how="inner")
    )
    if parent_rows.is_empty():
        return pl.DataFrame(schema={column: pl.String for column in RESOLUTION_COLUMNS})
    return (
        parent_rows.with_columns(
            pl.lit("terminal_disposition").alias("resolution_status"),
            pl.lit("terminal_parent_security_linked").alias("resolution_disposition"),
            pl.lit("local_parent_root_evidence").alias("evidence_tier"),
            issuer_name_expr().alias("historical_issuer_name"),
            event_type_expr().alias("event_type"),
            pl.col("last_day").alias("event_date"),
            pl.lit(None, dtype=pl.String).alias("successor"),
            pl.lit("local:parent-root-current-evidence").alias("primary_source_url"),
            pl.lit(None, dtype=pl.String).alias("secondary_source_url"),
            source_note_expr().alias("source_note"),
            pl.lit("parent_security_resolution_v1").alias("resolver"),
        )
        .select(RESOLUTION_COLUMNS)
        .sort(["research_route", "symbol"])
    )


def parent_root_symbol_expr() -> pl.Expr:
    symbol = pl.col("symbol")
    return (
        pl.when(symbol.str.contains(r"[-.]"))
        .then(symbol.str.replace(r"[-.].*$", ""))
        .when(symbol.str.ends_with("WS") | symbol.str.ends_with("WT") | symbol.str.ends_with("RT"))
        .then(symbol.str.slice(0, symbol.str.len_chars() - 2))
        .when(
            symbol.str.ends_with("W")
            | symbol.str.ends_with("U")
            | symbol.str.ends_with("+")
            | symbol.str.ends_with("=")
            | symbol.str.ends_with("^")
        )
        .then(symbol.str.slice(0, symbol.str.len_chars() - 1))
        .otherwise(None)
    )


def issuer_name_expr() -> pl.Expr:
    return pl.coalesce(
        [
            pl.col("sec_name"),
            pl.col("iex_latest_issuer"),
            pl.concat_str([pl.lit("Parent root "), pl.col("parent_root_symbol")]),
        ]
    )


def event_type_expr() -> pl.Expr:
    return (
        pl.when(pl.col("research_route") == ROUTE_PREFERRED_REDEMPTION_OR_DELISTING)
        .then(pl.lit("preferred_security_terminal_action"))
        .when(pl.col("research_route") == ROUTE_SHARE_CLASS_CORPORATE_ACTION)
        .then(pl.lit("share_class_terminal_action"))
        .otherwise(pl.lit("warrant_unit_right_terminal_action"))
    )


def source_note_expr() -> pl.Expr:
    return pl.concat_str(
        [
            pl.lit("Local terminal disposition: "),
            pl.col("symbol"),
            pl.lit(" linked to parent/root symbol "),
            pl.col("parent_root_symbol"),
            pl.lit(" with current SEC/IEX evidence; does not assert standalone issuer proof."),
        ]
    )


def build_summary(
    config: ParentSecurityResolutionConfig, priority: pl.DataFrame, candidates: pl.DataFrame
) -> dict[str, Any]:
    parent_scope = priority.filter(pl.col("research_route").is_in(PARENT_ROUTES))
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "local parent/root security terminal disposition candidates",
        "priority_queue_path": str(config.priority_queue_path),
        "candidate_count": candidates.height,
        "candidate_symbol_count": candidates["symbol"].n_unique() if candidates.height else 0,
        "parent_scope_count": parent_scope.height,
        "candidate_status_counts": count_by_if_present(candidates, "resolution_status"),
        "candidate_route_counts": count_by_if_present(candidates, "research_route"),
        "limitations": [
            "Parent/root linking is a workflow disposition, not issuer-level historical proof.",
            "Rows should be imported into the resolution ledger, not the historical identity override file.",
        ],
    }


def count_by_if_present(frame: pl.DataFrame, column: str) -> dict[str, int]:
    if frame.is_empty() or column not in frame.columns:
        return {}
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(
    config: ParentSecurityResolutionConfig, candidates: pl.DataFrame, summary: dict[str, Any]
) -> None:
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    config.summary_path.parent.mkdir(parents=True, exist_ok=True)
    candidates.write_csv(config.output_path)
    config.summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
