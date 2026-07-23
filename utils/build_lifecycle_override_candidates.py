from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT

DEFAULT_TEMPLATE_PATH = DEFAULT_OUTPUT_ROOT / "sec-lifecycle-iterations" / "lifecycle_window.csv"
DEFAULT_TRIAGE_PATH = DEFAULT_OUTPUT_ROOT / "sec-lifecycle-iterations" / "edgar" / "edgar_full_text_triage.parquet"
DEFAULT_OUTPUT_PATH = DEFAULT_OUTPUT_ROOT / "sec-lifecycle-iterations" / "lifecycle_override_candidates.csv"
DEFAULT_LOG_PATH = DEFAULT_OUTPUT_ROOT / "sec-lifecycle-iterations" / "lifecycle_override_candidates.jsonl"
CANDIDATE_STATUS = "candidate_needs_review"
RESEARCH_COLUMNS = [
    "research_status",
    "proposed_historical_identity_status",
    "proposed_historical_issuer_name",
    "proposed_historical_event_type",
    "proposed_historical_event_date",
    "proposed_historical_successor",
    "primary_source_url",
    "secondary_source_url",
    "research_note",
]


@dataclass(frozen=True)
class LifecycleCandidateConfig:
    template_path: Path
    triage_path: Path
    output_path: Path
    min_bucket: str


def main() -> int:
    args = parse_args()
    setup_logging(str(DEFAULT_LOG_PATH))
    result = build_lifecycle_override_candidates(
        LifecycleCandidateConfig(
            template_path=Path(args.template_path),
            triage_path=Path(args.triage_path),
            output_path=Path(args.output_path),
            min_bucket=args.min_bucket,
        )
    )
    get_logger(__name__).info(
        "lifecycle override candidates complete",
        extra={"event": "lifecycle_override_candidates_complete", "detail": result},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template-path", default=str(DEFAULT_TEMPLATE_PATH))
    parser.add_argument("--triage-path", default=str(DEFAULT_TRIAGE_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument(
        "--min-bucket",
        choices=("high_confidence_lead", "medium_confidence_lead", "manual_review_lead"),
        default="manual_review_lead",
    )
    return parser.parse_args()


def build_lifecycle_override_candidates(config: LifecycleCandidateConfig) -> dict[str, Any]:
    validate_config(config)
    template = pl.read_csv(config.template_path, infer_schema_length=0)
    triage = read_triage(config.triage_path)
    candidates = candidate_rows(template, triage, config)
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    candidates.write_csv(config.output_path)
    return {"output_path": str(config.output_path), "candidate_count": candidates.height}


def validate_config(config: LifecycleCandidateConfig) -> None:
    if not config.template_path.exists():
        raise FileNotFoundError(f"lifecycle template does not exist: {config.template_path}")
    if not config.triage_path.exists():
        raise FileNotFoundError(f"SEC triage file does not exist: {config.triage_path}")


def read_triage(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path) if path.suffix == ".parquet" else pl.read_csv(path, infer_schema_length=0)


def candidate_rows(
    template: pl.DataFrame, triage: pl.DataFrame, config: LifecycleCandidateConfig
) -> pl.DataFrame:
    best = best_triage_rows(triage, config)
    if best.is_empty():
        return empty_candidates(template)
    joined = template.drop([c for c in RESEARCH_COLUMNS if c in template.columns]).join(
        best, on=["symbol", "symbol_era_id", "first_day", "last_day"], how="inner"
    )
    if joined.is_empty():
        return empty_candidates(template)
    return joined.with_columns(candidate_columns()).sort(sort_expr()).select(output_columns(joined))


def empty_candidates(template: pl.DataFrame) -> pl.DataFrame:
    columns = output_columns(template)
    return pl.DataFrame(schema={column: pl.String for column in columns})


def best_triage_rows(triage: pl.DataFrame, config: LifecycleCandidateConfig) -> pl.DataFrame:
    rows = triage.filter(
        (pl.col("triage_rank").cast(pl.Int64, strict=False) == 1)
        & pl.col("triage_bucket").is_in(allowed_buckets(config.min_bucket))
    )
    return rows.select(
        [
            "symbol",
            "symbol_era_id",
            "first_day",
            "last_day",
            "triage_rank",
            "triage_bucket",
            "triage_score",
            "triage_reason",
            "cik",
            "entity",
            "form",
            "filed_at",
            "accession_no",
        ]
    )


def allowed_buckets(min_bucket: str) -> list[str]:
    ordered = ["high_confidence_lead", "medium_confidence_lead", "manual_review_lead"]
    return ordered[: ordered.index(min_bucket) + 1]


def candidate_columns() -> list[pl.Expr]:
    return [
        pl.lit(CANDIDATE_STATUS).alias("research_status"),
        pl.lit("manual_verified_historical_identity").alias("proposed_historical_identity_status"),
        pl.col("entity").map_elements(clean_entity_name, return_dtype=pl.String).alias(
            "proposed_historical_issuer_name"
        ),
        pl.struct(["lifecycle_anchor", "form"]).map_elements(event_type, return_dtype=pl.String).alias(
            "proposed_historical_event_type"
        ),
        pl.col("filed_at").alias("proposed_historical_event_date"),
        pl.lit(None, dtype=pl.String).alias("proposed_historical_successor"),
        pl.struct(["cik", "accession_no"]).map_elements(sec_archive_url, return_dtype=pl.String).alias(
            "primary_source_url"
        ),
        pl.lit(None, dtype=pl.String).alias("secondary_source_url"),
        pl.struct(["triage_bucket", "triage_score", "triage_reason", "lifecycle_anchor"]).map_elements(
            research_note, return_dtype=pl.String
        ).alias("research_note"),
    ]


def output_columns(frame: pl.DataFrame) -> list[str]:
    sec_lead = ["triage_bucket", "triage_score", "triage_reason", "cik", "entity", "form", "filed_at", "accession_no"]
    carried = [c for c in frame.columns if c not in RESEARCH_COLUMNS and c not in sec_lead]
    return carried + RESEARCH_COLUMNS + sec_lead


def clean_entity_name(value: Any) -> str:
    return re.sub(r"(\s+\([^)]*\))+$", "", str(value or "").strip()).strip()


def event_type(row: dict[str, Any]) -> str:
    form = str(row.get("form") or "").upper().strip()
    if str(row.get("lifecycle_anchor")) == "first":
        return "trading_commencement_or_symbol_lifecycle_lead"
    if form in {"25-NSE", "15-12B"}:
        return "delisting_or_registration_termination_lead"
    if form in {"SC 13E3", "SC 13E3/A"}:
        return "going_private_lead"
    return "operating_lifecycle_terminal_lead"


def sec_archive_url(row: dict[str, Any]) -> str | None:
    cik = str(row.get("cik") or "").lstrip("0")
    accession = str(row.get("accession_no") or "").replace("-", "")
    if not cik or not accession:
        return None
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/"


def research_note(row: dict[str, Any]) -> str:
    return (
        f"SEC lifecycle {row['lifecycle_anchor']}-anchor triage {row['triage_bucket']} "
        f"score={row['triage_score']}; {row['triage_reason']}."
    )


def sort_expr() -> list[pl.Expr]:
    return [
        pl.col("priority_rank").cast(pl.Int64, strict=False),
        pl.col("lifecycle_anchor"),
        pl.col("triage_rank").cast(pl.Int64, strict=False),
    ]


if __name__ == "__main__":
    raise SystemExit(main())
