from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT

DEFAULT_CANDIDATES_PATH = DEFAULT_OUTPUT_ROOT / "sec_override_candidates.csv"
DEFAULT_OUTPUT_PATH = DEFAULT_OUTPUT_ROOT / "sec_override_candidates_verified_triage.csv"
DEFAULT_SUMMARY_PATH = DEFAULT_OUTPUT_ROOT / "sec_override_candidates_verified_triage_summary.json"
DEFAULT_LOG_PATH = DEFAULT_OUTPUT_ROOT / "sec_override_candidates_verified_triage.jsonl"
REQUIRED_COLUMNS = [
    "symbol",
    "symbol_era_id",
    "proposed_historical_issuer_name",
    "proposed_historical_event_type",
    "primary_source_url",
    "form",
]
EVENT_TERMS = (
    "merger",
    "acquisition",
    "acquired",
    "combination",
    "going private",
    "tender offer",
)
COMPLETION_TERMS = (
    "completed the merger",
    "completed its acquisition",
    "consummated the merger",
    "merger became effective",
    "closing of the merger",
    "transaction closed",
)
DELISTING_TERMS = (
    "delisting",
    "delisted",
    "form 25",
    "terminated registration",
    "suspended from trading",
)


@dataclass(frozen=True)
class VerifyConfig:
    candidates_path: Path
    output_path: Path
    summary_path: Path
    user_agent: str
    timeout_seconds: float
    sleep_seconds: float
    max_rows: int | None
