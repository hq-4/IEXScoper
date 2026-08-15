"""Ranked worklist of every era with no automatically-resolved CIK (and therefore no
automatic SIC/sector), for manual per-ticker research. Mirrors
`utils/build_dead_ticker_priority_queue.py`'s four-artifact shape. Ranked by
`trade_rows` descending so research time goes to the highest-impact tickers first;
`has_googleable_name` flags rows that at least carry an OpenFIGI-asserted issuer name
(easier to search for) versus the fully identity-less tail (just a ticker symbol).

Rows tagged `sic_coverage_status=fund_no_sic_needed` (a fund/ETF per the OpenFIGI
instrument classification) are excluded entirely — those aren't research targets, they
need "this is a fund," not a SIC industry lookup a human has to go find. [CA][CDiP]
"""

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

DEFAULT_ERAS_SECTOR_ENRICHED_PATH = Path("reports/era-identity/eras_sector_enriched.parquet")
DEFAULT_OUTPUT_ROOT = Path("reports/sector-research-worklist")
DEFAULT_TOP_N = 500

REQUIRED_COLUMNS = (
    "symbol",
    "symbol_era_id",
    "source_classification",
    "trade_rows",
    "first_day",
    "last_day",
    "identity_tier",
    "identity_issuer",
    "identity_instrument",
    "instrument_class",
    "cik_source",
    "resolved_cik",
    "sic_coverage_status",
    "identity_disproven",
)
WORKLIST_COLUMNS = [
    "priority_rank",
    "symbol",
    "symbol_era_id",
    "source_classification",
    "trade_rows",
    "first_day",
    "last_day",
    "identity_tier",
    "identity_issuer",
    "instrument_class",
    "has_googleable_name",
    "identity_disproven",
    "cik_source",
]
FUND_NO_SIC_NEEDED = "fund_no_sic_needed"
MANUAL_COLUMNS = ("manual_cik", "manual_sic", "manual_notes")


@dataclass(frozen=True)
class SectorWorklistConfig:
    eras_sector_enriched_path: Path = DEFAULT_ERAS_SECTOR_ENRICHED_PATH
    output_root: Path = DEFAULT_OUTPUT_ROOT
    top_n: int = DEFAULT_TOP_N


def main() -> int:
    args = parse_args()
    config = SectorWorklistConfig(
        eras_sector_enriched_path=Path(args.eras_sector_enriched_path),
        output_root=Path(args.output_root),
        top_n=args.top_n,
    )
    setup_logging(str(config.output_root / "sector_research_worklist.jsonl"))
    result = build_sector_worklist(config)
    get_logger(__name__).info(
        "sector research worklist complete",
        extra={"event": "sector_research_worklist_complete", "detail": result["summary"]},
    )
    return 0


def build_sector_worklist(config: SectorWorklistConfig) -> dict[str, Any]:
    validate_config(config)
    config.output_root.mkdir(parents=True, exist_ok=True)
    frame = load_eras_sector_enriched(config.eras_sector_enriched_path)
    worklist = prioritize_no_cik(frame)
    summary = build_summary(config, worklist, count_excluded_funds(frame))
    write_outputs(config.output_root, worklist, summary, config.top_n)
    return {"summary": summary, "rows": worklist.head(config.top_n).to_dicts()}


def validate_config(config: SectorWorklistConfig) -> None:
    if not config.eras_sector_enriched_path.exists():
        raise FileNotFoundError(
            f"era-sector enriched product does not exist: {config.eras_sector_enriched_path} "
            "(run utils/build_era_sector_enriched.py first)"
        )
    if config.top_n <= 0:
        raise ValueError("--top-n must be positive")


def load_eras_sector_enriched(path: Path) -> pl.DataFrame:
    frame = pl.read_parquet(path)
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    return frame


def prioritize_no_cik(frame: pl.DataFrame) -> pl.DataFrame:
    is_fund = (pl.col("sic_coverage_status") == FUND_NO_SIC_NEEDED).fill_null(False)
    return (
        frame.filter(pl.col("resolved_cik").is_null() & ~is_fund)
        .with_columns(pl.col("identity_issuer").is_not_null().alias("has_googleable_name"))
        .sort("trade_rows", descending=True)
        .with_row_index("priority_rank", offset=1)
        .select(WORKLIST_COLUMNS)
        .with_columns(pl.lit(None, dtype=pl.String).alias(column) for column in MANUAL_COLUMNS)
    )


def count_excluded_funds(frame: pl.DataFrame) -> int:
    """Eras excluded from the worklist because they're a fund/ETF, not because they're
    resolved — surfaced in the summary so the exclusion is visible, not silent."""
    is_fund = (pl.col("sic_coverage_status") == FUND_NO_SIC_NEEDED).fill_null(False)
    return frame.filter(pl.col("resolved_cik").is_null() & is_fund).height


def build_summary(
    config: SectorWorklistConfig, worklist: pl.DataFrame, excluded_fund_count: int
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "eras with no resolved CIK, ranked by trade_rows for manual sector research; "
        "funds/ETFs excluded (they don't need a SIC industry lookup)",
        "eras_sector_enriched_path": str(config.eras_sector_enriched_path),
        "worklist_era_count": worklist.height,
        "worklist_trade_rows": int(worklist["trade_rows"].sum() or 0),
        "excluded_fund_count": excluded_fund_count,
        "top_n": min(config.top_n, worklist.height),
        "has_googleable_name_count": int(worklist["has_googleable_name"].sum()),
        "identity_disproven_count": int(worklist["identity_disproven"].sum()),
        "classification_counts": count_by(worklist, "source_classification"),
        "top_classification_counts": count_by(worklist.head(config.top_n), "source_classification"),
        "sort_order": ["trade_rows descending"],
        "limitations": [
            "has_googleable_name only means an OpenFIGI-asserted issuer name exists — it is not "
            "proof of identity and may itself be wrong for a reused ticker.",
            "identity_disproven=true (Phase 18) is stronger than a plain unmatched name: SEC's own "
            "filing history proves the asserted issuer name can't be the operating entity for this "
            "era — don't research the name as printed; research the ticker/date range instead.",
            "manual_cik/manual_sic/manual_notes are blank by design, for the researcher's own "
            "findings; there is no re-import tool for this file yet.",
            f"{excluded_fund_count} no-CIK eras were excluded entirely as funds/ETFs "
            "(sic_coverage_status=fund_no_sic_needed) rather than left as research targets.",
        ],
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(root: Path, worklist: pl.DataFrame, summary: dict[str, Any], top_n: int) -> None:
    worklist.write_parquet(root / "sector_research_worklist.parquet", compression="zstd")
    worklist.write_csv(root / "sector_research_worklist.csv")
    worklist.head(top_n).write_csv(root / "sector_research_worklist_top.csv")
    (root / "sector_research_worklist_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_markdown(root / "sector_research_worklist_report.md", worklist.head(top_n), summary)


def write_markdown(path: Path, top_rows: pl.DataFrame, summary: dict[str, Any]) -> None:
    lines = [
        "# Sector Manual Research Worklist",
        "",
        "Eras with no automatically-resolved CIK (and therefore no automatic SIC/sector),",
        "ranked by trade volume for manual per-ticker research. Funds/ETFs are excluded —",
        "they don't need a SIC industry lookup.",
        "",
        f"- Worklist eras: `{summary['worklist_era_count']}`",
        f"- Excluded as funds/ETFs (not research targets): `{summary['excluded_fund_count']}`",
        f"- Worklist trade rows: `{summary['worklist_trade_rows']}`",
        f"- Rows with a googleable issuer name already asserted: `{summary['has_googleable_name_count']}`",
        f"- Rows with a disproven issuer name (Phase 18 — don't trust it as printed): "
        f"`{summary['identity_disproven_count']}`",
        f"- Top rows shown: `{summary['top_n']}`",
        "",
        "## Top Research Targets",
        "",
        "| Rank | Symbol | Era | Class | Issuer (if any) | Trade Rows | First | Last |",
        "|---:|---|---|---|---|---:|---|---|",
    ]
    for row in top_rows.to_dicts():
        issuer = row["identity_issuer"] or ""
        if row["identity_disproven"] and issuer:
            issuer = f"~~{issuer}~~ (disproven)"
        lines.append(
            "| {priority_rank} | {symbol} | {symbol_era_id} | {source_classification} | "
            "{identity_issuer} | {trade_rows} | {first_day} | {last_day} |".format(
                **{**row, "identity_issuer": issuer}
            )
        )
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in summary["limitations"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--eras-sector-enriched-path", default=str(DEFAULT_ERAS_SECTOR_ENRICHED_PATH)
    )
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
