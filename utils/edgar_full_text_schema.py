from __future__ import annotations

import polars as pl

from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT as DEFAULT_DEAD_TICKER_ROOT

DEFAULT_TEMPLATE_PATH = DEFAULT_DEAD_TICKER_ROOT / "manual_resolution_template.csv"
DEFAULT_OUTPUT_ROOT = DEFAULT_DEAD_TICKER_ROOT / "edgar-full-text"
DEFAULT_ENDPOINT = "https://efts.sec.gov/LATEST/search-index"
DEFAULT_EVENT_TERMS = ("merger", "acquisition", "acquired", "delisting", "delisted")
DEFAULT_FORMS = ("8-K", "S-4", "425", "SC 13E3", "25-NSE", "15-12B")
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_SLEEP_SECONDS = 0.3
DEFAULT_SIZE = 10

LEAD_SCHEMA = {
    "priority_rank": pl.String,
    "symbol": pl.String,
    "symbol_era_id": pl.String,
    "first_day": pl.String,
    "last_day": pl.String,
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
}
