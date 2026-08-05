from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl


def build_summary(
    config: Any,
    workplan: pl.DataFrame,
    low_candidates: pl.DataFrame,
    doc_graph: pl.DataFrame,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "impact-weighted dead ticker resolution workplan",
        "lanes_path": str(config.lanes_path),
        "row_count": workplan.height,
        "total_trade_rows": int(workplan["trade_rows"].sum() or 0),
        "bucket_counts": count_by(workplan, "workplan_bucket"),
        "bucket_trade_rows": sum_by(workplan, "workplan_bucket", "trade_rows"),
        "low_materiality_candidate_count": low_candidates.height,
        "sec_doc_graph_rows": doc_graph.height,
        "sec_evidence_path": str(config.sec_evidence_path) if config.sec_evidence_path else None,
        "sec_doc_top_n": config.sec_doc_top_n,
        "limitations": limitations(),
    }


def limitations() -> list[str]:
    return [
        "Workplan buckets are workflow routing decisions, not historical issuer proof.",
        "Low-materiality ledger candidates are generated for dry-run review only.",
        "SEC document scoring uses local evidence files and does not fetch network data.",
    ]


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def sum_by(frame: pl.DataFrame, group_column: str, value_column: str) -> dict[str, int]:
    grouped = frame.group_by(group_column).agg(pl.col(value_column).sum()).sort(group_column)
    return {str(row[group_column]): int(row[value_column] or 0) for row in grouped.to_dicts()}


def write_outputs(
    root: Path,
    workplan: pl.DataFrame,
    low_candidates: pl.DataFrame,
    doc_graph: pl.DataFrame,
    summary: dict[str, Any],
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    workplan.write_csv(root / "workplan_all.csv")
    write_bucket_files(root, workplan)
    low_candidates.write_csv(root / "workplan_low_materiality_ledger_candidates.csv")
    doc_graph.write_csv(root / "workplan_sec_document_graph.csv")
    (root / "workplan_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def write_bucket_files(root: Path, workplan: pl.DataFrame) -> None:
    for bucket_name in sorted(workplan["workplan_bucket"].unique().to_list()):
        bucket_frame = workplan.filter(pl.col("workplan_bucket") == bucket_name)
        bucket_frame.sort("impact_rank").write_csv(root / f"workplan_{bucket_name}.csv")
