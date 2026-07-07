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
    DEAD_REVIEW_CLASSES,
    DEFAULT_IEX_ERAS_PATH,
    DEFAULT_MANUAL_OVERRIDES_PATH,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_SEC_ERAS_PATH,
    REVIEW_COLUMNS,
)
from utils.instrument_classifier import (
    instrument_hint_expr,
    instrument_reason_expr,
    instrument_type_expr,
)


@dataclass(frozen=True)
class DeadTickerReviewConfig:
    sec_eras_path: Path
    iex_eras_path: Path
    manual_overrides_path: Path
    output_root: Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sec-eras-path", default=str(DEFAULT_SEC_ERAS_PATH))
    parser.add_argument("--iex-eras-path", default=str(DEFAULT_IEX_ERAS_PATH))
    parser.add_argument("--manual-overrides-path", default=str(DEFAULT_MANUAL_OVERRIDES_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    config = DeadTickerReviewConfig(
        sec_eras_path=Path(args.sec_eras_path),
        iex_eras_path=Path(args.iex_eras_path),
        manual_overrides_path=Path(args.manual_overrides_path),
        output_root=Path(args.output_root),
    )
    setup_logging(str(config.output_root / "dead_ticker_review.jsonl"))
    result = build_dead_ticker_review_queue(config)
    get_logger(__name__).info(
        "dead ticker review queue complete",
        extra={"event": "dead_ticker_review_complete", "detail": result["summary"]},
    )
    return 0


def build_dead_ticker_review_queue(config: DeadTickerReviewConfig) -> dict[str, Any]:
    validate_inputs(config)
    config.output_root.mkdir(parents=True, exist_ok=True)
    sec = pl.read_parquet(config.sec_eras_path)
    iex = pl.read_parquet(config.iex_eras_path).select(
        "symbol_era_id",
        "iex_entity_confidence",
        "iex_latest_issuer",
        "iex_product_hint",
        "iex_seen_in_latest",
    )
    overrides = load_manual_overrides(config.manual_overrides_path)
    queue = build_queue(
        sec.join(iex, on="symbol_era_id", how="left").join(
            overrides, on="symbol_era_id", how="left"
        )
    )
    summary = build_summary(config, queue)
    write_outputs(config.output_root, summary, queue)
    return {"summary": summary, "rows": queue.to_dicts()}


def validate_inputs(config: DeadTickerReviewConfig) -> None:
    for path, label in [
        (config.sec_eras_path, "SEC enriched symbol eras"),
        (config.iex_eras_path, "IEX enriched symbol eras"),
        (config.manual_overrides_path, "manual historical identity overrides"),
    ]:
        if not path.exists():
            raise FileNotFoundError(f"{label} does not exist: {path}")


def load_manual_overrides(path: Path) -> pl.DataFrame:
    required = [
        "symbol_era_id",
        "historical_identity_status",
        "historical_issuer_name",
        "historical_event_type",
        "historical_event_date",
        "historical_successor",
        "source_url",
        "source_note",
    ]
    frame = pl.read_csv(path, infer_schema_length=0)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"manual override file missing required columns: {missing}")
    return frame.select(required)


def build_queue(frame: pl.DataFrame) -> pl.DataFrame:
    require_columns(
        frame,
        [
            "symbol",
            "symbol_era_id",
            "source_classification",
            "first_day",
            "last_day",
            "trade_rows",
            "sec_current_confidence",
            "iex_entity_confidence",
        ],
    )
    return (
        frame.filter(pl.col("source_classification").is_in(DEAD_REVIEW_CLASSES))
        .with_columns(
            instrument_hint_expr().alias("instrument_hint"),
            instrument_type_expr().alias("instrument_type"),
            instrument_reason_expr().alias("instrument_reason"),
            evidence_status_expr().alias("identity_evidence_status"),
        )
        .with_columns(apply_manual_evidence_expr().alias("identity_evidence_status"))
        .with_columns(review_priority_expr().alias("review_priority"))
        .select(REVIEW_COLUMNS)
        .sort(["review_priority", "trade_rows", "symbol"], descending=[False, True, False])
    )


def require_columns(frame: pl.DataFrame, columns: list[str]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"review input missing required columns: {missing}")


def evidence_status_expr() -> pl.Expr:
    sec_match = pl.col("sec_current_confidence").is_in(
        ["sec_current_match", "sec_multiple_current_matches"]
    )
    iex_match = pl.col("iex_entity_confidence").is_in(
        [
            "iex_snapshot_overlap",
            "iex_snapshot_changed_during_window",
            "iex_snapshot_removed_before_latest",
            "iex_current_symbol_only",
        ]
    )
    return (
        pl.when(sec_match & iex_match)
        .then(pl.lit("current_sec_and_iex_evidence"))
        .when(sec_match)
        .then(pl.lit("current_sec_only_evidence"))
        .when(iex_match)
        .then(pl.lit("current_iex_only_evidence"))
        .otherwise(pl.lit("historical_identity_unresolved"))
    )


def review_priority_expr() -> pl.Expr:
    return (
        pl.when(pl.col("historical_identity_status").is_not_null())
        .then(0)
        .when(pl.col("identity_evidence_status") == "historical_identity_unresolved")
        .then(1)
        .when(pl.col("source_classification") == "intermittent_or_reused_candidate")
        .then(2)
        .when(pl.col("source_classification") == "delisted_or_acquired_candidate")
        .then(3)
        .otherwise(4)
    )


def build_summary(config: DeadTickerReviewConfig, queue: pl.DataFrame) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "dead and intermittent ticker-era identity review queue",
        "sec_eras_path": str(config.sec_eras_path),
        "iex_eras_path": str(config.iex_eras_path),
        "manual_overrides_path": str(config.manual_overrides_path),
        "review_era_count": queue.height,
        "unique_symbol_count": queue.select("symbol").n_unique(),
        "evidence_status_counts": count_by(queue, "identity_evidence_status"),
        "instrument_hint_counts": count_by(queue, "instrument_hint"),
        "instrument_type_counts": count_by(queue, "instrument_type"),
        "instrument_reason_counts": count_by(queue, "instrument_reason"),
        "review_priority_counts": count_by(queue, "review_priority"),
        "classification_counts": count_by(queue, "source_classification"),
        "limitations": [
            "This queue classifies ticker-era review targets; it does not prove issuer identity.",
            "Current SEC/IEX evidence can be stale or reused for historical eras.",
            "Instrument types are first-pass heuristics from ticker syntax and IEX hints only.",
            "Rows with historical_identity_unresolved need historical listing, filings, or manual review.",
        ],
    }


def apply_manual_evidence_expr() -> pl.Expr:
    return (
        pl.when(pl.col("historical_identity_status").is_not_null())
        .then(pl.lit("manual_verified_historical_identity"))
        .otherwise(pl.col("identity_evidence_status"))
    )


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(root: Path, summary: dict[str, Any], queue: pl.DataFrame) -> None:
    queue.write_parquet(root / "dead_ticker_review_queue.parquet", compression="zstd")
    queue.write_csv(root / "dead_ticker_review_queue.csv")
    write_instrument_audit(root, queue)
    (root / "dead_ticker_review_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_markdown(root / "dead_ticker_review_report.md", summary, queue)


def write_markdown(path: Path, summary: dict[str, Any], queue: pl.DataFrame) -> None:
    lines = [
        "# Dead Ticker Review Queue",
        "",
        "This report identifies non-stable ticker eras that still need historical identity evidence.",
        "",
        f"- Review eras: `{summary['review_era_count']}`",
        f"- Unique symbols: `{summary['unique_symbol_count']}`",
        "",
        "## Evidence Status",
        "",
    ]
    lines.extend(
        f"- `{key}`: `{value}`" for key, value in summary["evidence_status_counts"].items()
    )
    lines.extend(["", "## Instrument Hints", ""])
    lines.extend(
        f"- `{key}`: `{value}`" for key, value in summary["instrument_hint_counts"].items()
    )
    lines.extend(["", "## Instrument Types", ""])
    lines.extend(
        f"- `{key}`: `{value}`" for key, value in summary["instrument_type_counts"].items()
    )
    lines.extend(
        [
            "",
            "## Top Priority Sample",
            "",
            "| Symbol | Era | Class | Evidence | Hint | Type | Trades |",
        ]
    )
    lines.append("|---|---|---|---|---|---|---:|")
    for row in queue.head(25).to_dicts():
        lines.append(
            "| {symbol} | {symbol_era_id} | {source_classification} | "
            "{identity_evidence_status} | {instrument_hint} | {instrument_type} | "
            "{trade_rows} |".format(**row)
        )
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in summary["limitations"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_instrument_audit(root: Path, queue: pl.DataFrame) -> None:
    audit = queue.select(
        [
            "symbol",
            "symbol_era_id",
            "instrument_hint",
            "instrument_type",
            "instrument_reason",
            "trade_rows",
            "source_classification",
            "identity_evidence_status",
            "iex_product_hint",
            "iex_latest_issuer",
        ]
    ).sort(["instrument_type", "trade_rows", "symbol"], descending=[False, True, False])
    audit.write_csv(root / "instrument_heuristic_audit.csv")
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "first-pass local ticker-era instrument heuristic audit",
        "row_count": audit.height,
        "instrument_hint_counts": count_by(audit, "instrument_hint"),
        "instrument_type_counts": count_by(audit, "instrument_type"),
        "instrument_reason_counts": count_by(audit, "instrument_reason"),
        "top_examples_by_type": top_examples_by_type(audit),
        "limitations": [
            "This audit uses local ticker syntax and IEX hints only.",
            "Venue and feed symbol syntax differs; manual evidence is still required.",
            "The audit does not change downstream SEC resolver behavior.",
        ],
    }
    (root / "instrument_heuristic_audit_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def top_examples_by_type(audit: pl.DataFrame, limit: int = 10) -> dict[str, list[dict[str, Any]]]:
    examples: dict[str, list[dict[str, Any]]] = {}
    for instrument_type in sorted(audit["instrument_type"].unique().to_list()):
        examples[str(instrument_type)] = (
            audit.filter(pl.col("instrument_type") == instrument_type).head(limit).to_dicts()
        )
    return examples


if __name__ == "__main__":
    raise SystemExit(main())
