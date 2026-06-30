from __future__ import annotations

from pathlib import Path

DEFAULT_SEC_ERAS_PATH = Path("reports/sec-ticker-cik/symbol_eras_sec_enriched.parquet")
DEFAULT_IEX_ERAS_PATH = Path("reports/iex-entity-enrichment/symbol_eras_iex_enriched.parquet")
DEFAULT_MANUAL_OVERRIDES_PATH = Path("data/manual_overrides/historical_ticker_identities.csv")
DEFAULT_OUTPUT_ROOT = Path("reports/dead-ticker-review")

DEAD_REVIEW_CLASSES = {
    "delisted_or_acquired_candidate",
    "intermittent_or_reused_candidate",
    "intermittent_full_window_candidate",
    "partial_window_candidate",
}

REVIEW_COLUMNS = [
    "symbol",
    "symbol_era_id",
    "source_classification",
    "first_day",
    "last_day",
    "observed_days",
    "trade_rows",
    "main_rows",
    "sec_current_confidence",
    "sec_cik",
    "sec_name",
    "sec_ticker",
    "sec_exchange",
    "iex_entity_confidence",
    "iex_latest_issuer",
    "iex_product_hint",
    "iex_seen_in_latest",
    "historical_identity_status",
    "historical_issuer_name",
    "historical_event_type",
    "historical_event_date",
    "historical_successor",
    "source_url",
    "source_note",
    "identity_evidence_status",
    "instrument_hint",
    "review_priority",
]
