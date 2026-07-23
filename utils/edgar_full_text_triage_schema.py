from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from utils.edgar_full_text_schema import DEFAULT_OUTPUT_ROOT

DEFAULT_LEADS_PATH = DEFAULT_OUTPUT_ROOT / "edgar_full_text_leads.parquet"
DEFAULT_TRIAGE_CSV = DEFAULT_OUTPUT_ROOT / "edgar_full_text_triage.csv"
DEFAULT_TRIAGE_PARQUET = DEFAULT_OUTPUT_ROOT / "edgar_full_text_triage.parquet"
DEFAULT_SUMMARY_PATH = DEFAULT_OUTPUT_ROOT / "edgar_full_text_triage_summary.json"
EVENT_WORDS = frozenset({"acquisition", "acquired", "merger", "merge", "delist", "delisted"})
DEAL_FORMS = frozenset({"425", "S-4", "S-4/A", "DEFM14A", "PREM14A", "SC 13E3", "SC 13E3/A"})
EXIT_FORMS = frozenset({"25-NSE", "15-12B"})
REVIEW_FORMS = frozenset({"8-K", "6-K", "10-K", "10-Q", "DEF 14A", "DEFA14A"})
TRIAGE_SCHEMA = {
    "symbol": pl.String,
    "symbol_era_id": pl.String,
    "priority_rank": pl.String,
    "triage_rank": pl.Int64,
    "triage_bucket": pl.String,
    "triage_score": pl.Int64,
    "triage_reason": pl.String,
    "form_strength": pl.String,
    "date_relation": pl.String,
    "entity_match": pl.String,
    "query": pl.String,
    "search_status": pl.String,
    "total_hits": pl.Int64,
    "hit_rank": pl.Int64,
    "cik": pl.String,
    "entity": pl.String,
    "form": pl.String,
    "filed_at": pl.String,
    "accession_no": pl.String,
    "document_url": pl.String,
    "first_day": pl.String,
    "last_day": pl.String,
}


@dataclass(frozen=True)
class TriageConfig:
    leads_path: Path
    output_csv: Path
    output_parquet: Path
    summary_path: Path
    top_n: int
