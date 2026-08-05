from __future__ import annotations

from pathlib import Path

DEFAULT_SEC_ERAS_PATH = Path("reports/sec-ticker-cik/symbol_eras_sec_enriched.parquet")
DEFAULT_IEX_ERAS_PATH = Path("reports/iex-entity-enrichment/symbol_eras_iex_enriched.parquet")
DEFAULT_MANUAL_OVERRIDES_PATH = Path("data/manual_overrides/historical_ticker_identities.csv")
DEFAULT_RESOLUTION_LEDGER_PATH = Path("data/manual_overrides/ticker_era_resolution_ledger.csv")
DEFAULT_FACT_ROOT = Path("data/resolution")
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
    "resolution_status",
    "resolution_disposition",
    "evidence_tier",
    "ledger_research_route",
    "ledger_instrument_type",
    "ledger_historical_issuer_name",
    "ledger_event_type",
    "ledger_event_date",
    "ledger_successor",
    "ledger_primary_source_url",
    "ledger_secondary_source_url",
    "ledger_source_note",
    "resolver",
    "resolution_workflow_status",
    "identity_evidence_status",
    "instrument_hint",
    "instrument_type",
    "instrument_reason",
    "research_route",
    "recommended_evidence",
    "routing_reason",
    "review_priority",
    # Canonical V3 fact-store columns (data/resolution/identity_facts.jsonl +
    # event_facts.jsonl), joined via utils/canonical_identity_join.py. These reflect
    # the OpenFIGI-tiered confidence store, which is the current source of truth and
    # is broader than the legacy historical_* / ledger_* columns above (that CSV
    # override path predates the OpenFIGI identity pillar and event catalog).
    "canonical_identity_tier",
    "canonical_identity_issuer",
    "canonical_identity_instrument",
    "canonical_identity_contested",
    "canonical_identity_usable_default",
    "canonical_event_type",
    "canonical_event_date",
    "canonical_event_verification",
]
