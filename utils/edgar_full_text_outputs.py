from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from utils.search_edgar_full_text_types import EdgarFullTextConfig


def prepare_log_path(output_root: Path, *, append_log: bool) -> Path:
    log_path = output_root / "edgar_full_text_search.jsonl"
    if not append_log and log_path.exists():
        log_path.unlink()
    return log_path


def build_summary(
    config: EdgarFullTextConfig, targets: list[dict[str, Any]], leads: pl.DataFrame
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "SEC EDGAR full-text lead search for dead ticker review",
        "endpoint": config.endpoint,
        "template_path": str(config.template_path),
        "alias_path": str(config.alias_path),
        "target_symbol_count": len(targets),
        "forms": list(config.forms),
        "event_terms": list(config.event_terms),
        "status_counts": count_by(leads, "search_status"),
        "symbols_with_hits": leads.filter(pl.col("search_status") == "hit")
        .select("symbol")
        .n_unique(),
        "limitations": [
            "EFTS hits are leads only and do not prove historical ticker identity.",
            "Short tickers can produce noisy full-text hits; verify issuer, CIK, filing date, and event.",
        ],
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(
    root: Path,
    leads: pl.DataFrame,
    raw_payloads: list[dict[str, Any]],
    summary: dict[str, Any],
) -> None:
    leads.write_csv(root / "edgar_full_text_leads.csv")
    leads.write_parquet(root / "edgar_full_text_leads.parquet", compression="zstd")
    (root / "edgar_full_text_raw.jsonl").write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in raw_payloads) + "\n",
        encoding="utf-8",
    )
    (root / "edgar_full_text_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
