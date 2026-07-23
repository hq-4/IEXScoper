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

DEFAULT_TEMPLATE_PATH = DEFAULT_OUTPUT_ROOT / "manual_resolution_template.csv"
DEFAULT_TRIAGE_PATH = DEFAULT_OUTPUT_ROOT / "edgar-full-text" / "edgar_full_text_triage.parquet"
DEFAULT_OUTPUT_PATH = DEFAULT_OUTPUT_ROOT / "sec_override_candidates.csv"
DEFAULT_LOG_PATH = DEFAULT_OUTPUT_ROOT / "sec_override_candidates.jsonl"
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
class CandidateConfig:
    template_path: Path
    triage_path: Path
    output_path: Path
    min_bucket: str
    max_rows: int | None


def main() -> int:
    args = parse_args()
    config = CandidateConfig(
        template_path=Path(args.template_path),
        triage_path=Path(args.triage_path),
        output_path=Path(args.output_path),
        min_bucket=args.min_bucket,
        max_rows=args.max_rows,
    )
    setup_logging(str(DEFAULT_LOG_PATH))
    result = build_override_candidates(config)
    get_logger(__name__).info(
        "SEC override candidates complete",
        extra={"event": "sec_override_candidates_complete", "detail": result},
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
        default="high_confidence_lead",
    )
    parser.add_argument("--max-rows", type=int)
    return parser.parse_args()


def build_override_candidates(config: CandidateConfig) -> dict[str, Any]:
    validate_config(config)
    template = pl.read_csv(config.template_path, infer_schema_length=0)
    triage = read_triage(config.triage_path)
    candidates = candidate_rows(template, triage, config)
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    candidates.write_csv(config.output_path)
    return {
        "template_path": str(config.template_path),
        "triage_path": str(config.triage_path),
        "output_path": str(config.output_path),
        "candidate_count": candidates.height,
        "min_bucket": config.min_bucket,
    }


def validate_config(config: CandidateConfig) -> None:
    if not config.template_path.exists():
        raise FileNotFoundError(f"manual resolution template does not exist: {config.template_path}")
    if not config.triage_path.exists():
        raise FileNotFoundError(f"SEC triage file does not exist: {config.triage_path}")
    if config.max_rows is not None and config.max_rows <= 0:
        raise ValueError("--max-rows must be positive")


def read_triage(path: Path) -> pl.DataFrame:
    if path.suffix == ".parquet":
        return pl.read_parquet(path)
    return pl.read_csv(path, infer_schema_length=0)


def candidate_rows(
    template: pl.DataFrame, triage: pl.DataFrame, config: CandidateConfig
) -> pl.DataFrame:
    best = best_triage_rows(triage, config)
    joined = template.drop([column for column in RESEARCH_COLUMNS if column in template.columns]).join(
        best, on=["symbol", "symbol_era_id"], how="inner"
    )
    return (
        joined.with_columns(candidate_columns())
        .sort(pl.col("priority_rank").cast(pl.Int64, strict=False))
        .select(output_columns(joined.columns))
    )


def best_triage_rows(triage: pl.DataFrame, config: CandidateConfig) -> pl.DataFrame:
    rows = triage.filter(
        (pl.col("triage_rank").cast(pl.Int64, strict=False) == 1)
        & pl.col("triage_bucket").is_in(allowed_buckets(config))
    )
    if config.max_rows:
        rows = rows.head(config.max_rows)
    return rows.select(
        [
            "symbol",
            "symbol_era_id",
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


def allowed_buckets(config: CandidateConfig) -> list[str]:
    ordered = ["high_confidence_lead", "medium_confidence_lead", "manual_review_lead"]
    return ordered[: ordered.index(config.min_bucket) + 1]


def candidate_columns() -> list[pl.Expr]:
    return [
        pl.lit(CANDIDATE_STATUS).alias("research_status"),
        pl.lit("manual_verified_historical_identity").alias(
            "proposed_historical_identity_status"
        ),
        pl.col("entity").map_elements(clean_entity_name, return_dtype=pl.String).alias(
            "proposed_historical_issuer_name"
        ),
        pl.col("form").map_elements(event_type_for_form, return_dtype=pl.String).alias(
            "proposed_historical_event_type"
        ),
        pl.col("filed_at").alias("proposed_historical_event_date"),
        pl.lit(None, dtype=pl.String).alias("proposed_historical_successor"),
        pl.struct(["cik", "accession_no"]).map_elements(sec_archive_url, return_dtype=pl.String).alias(
            "primary_source_url"
        ),
        pl.lit(None, dtype=pl.String).alias("secondary_source_url"),
        pl.struct(["triage_bucket", "triage_score", "triage_reason", "form"]).map_elements(
            research_note, return_dtype=pl.String
        ).alias("research_note"),
    ]


def output_columns(source_columns: list[str]) -> list[str]:
    sec_lead = [
        "triage_bucket",
        "triage_score",
        "triage_reason",
        "cik",
        "entity",
        "form",
        "filed_at",
        "accession_no",
    ]
    carried = [
        column
        for column in source_columns
        if column not in RESEARCH_COLUMNS and column not in sec_lead
    ]
    return carried + RESEARCH_COLUMNS + sec_lead


def clean_entity_name(value: Any) -> str:
    text = str(value or "").strip()
    return re.sub(r"(\s+\([^)]*\))+$", "", text).strip()


def event_type_for_form(form: Any) -> str:
    normalized = str(form or "").upper().strip()
    if normalized in {"25-NSE", "15-12B"}:
        return "delisting_or_registration_termination_lead"
    if normalized in {"SC 13E3", "SC 13E3/A"}:
        return "going_private_lead"
    return "merger_or_acquisition_lead"


def sec_archive_url(row: dict[str, Any]) -> str | None:
    cik = str(row.get("cik") or "").lstrip("0")
    accession = str(row.get("accession_no") or "").replace("-", "")
    if not cik or not accession:
        return None
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/"


def research_note(row: dict[str, Any]) -> str:
    return (
        f"SEC triage {row['triage_bucket']} score={row['triage_score']}; "
        f"{row['triage_reason']}; form={row['form']}. Verify issuer, CIK, event, "
        "successor, and final delisting/closing before changing research_status to verified."
    )


if __name__ == "__main__":
    raise SystemExit(main())
