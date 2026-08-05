"""Shared best-tier identity/event fact loader for the canonical V3 store.

Both `build_era_identity_enriched.py` (the read-side era x identity product) and
`build_dead_ticker_review_queue.py` (the manual-review worklist) need the same
answer to "what is the best-assurance identity/event fact for this era" from
`data/resolution/identity_facts.jsonl` and `event_facts.jsonl`. Before this module
existed, only the era-identity product read the tiered facts; the review queue
still only saw the legacy `historical_ticker_identities.csv` override file, so the
two reports drifted out of sync with the canonical store and with each other.
Centralizing the join keeps them reconciled by construction. [CA][CSD]
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

TIER_RANK = {"verified": 0, "corroborated": 1, "openfigi_asserted": 2}
DEFAULT_FACT_ROOT = Path("data/resolution")

IDENTITY_SCHEMA = {
    "symbol_era_id": pl.String,
    "identity_tier": pl.String,
    "identity_issuer": pl.String,
    "identity_entity_id": pl.String,
    "identity_method": pl.String,
    "identity_instrument": pl.String,
    "identity_contested": pl.Boolean,
}

EVENT_SCHEMA = {
    "symbol_era_id": pl.String,
    "event_type": pl.String,
    "event_date": pl.String,
    "event_verification": pl.String,
}


def load_best_identity_facts(fact_root: Path = DEFAULT_FACT_ROOT) -> pl.DataFrame:
    """One row per era: the highest-assurance identity fact (verified > corroborated >
    openfigi_asserted). Facts outside that tier set (e.g. `candidate`) are ignored."""
    rows = _best_per_era(_read_jsonl(fact_root / "identity_facts.jsonl"), _identity_row)
    return (
        pl.DataFrame(rows, schema=IDENTITY_SCHEMA) if rows else pl.DataFrame(schema=IDENTITY_SCHEMA)
    )


def load_best_event_facts(fact_root: Path = DEFAULT_FACT_ROOT) -> pl.DataFrame:
    """One row per era: the highest-assurance event fact (verified > corroborated >
    openfigi_asserted, matching the identity tier ranks used for events too)."""
    rows = _best_per_era(_read_jsonl(fact_root / "event_facts.jsonl"), _event_row)
    return pl.DataFrame(rows, schema=EVENT_SCHEMA) if rows else pl.DataFrame(schema=EVENT_SCHEMA)


def load_canonical_facts_for_review(fact_root: Path = DEFAULT_FACT_ROOT) -> pl.DataFrame:
    """Best-tier identity/event facts renamed to a `canonical_` prefix, for joining
    onto review-queue-style frames that already have their own (legacy, pre-OpenFIGI)
    `historical_*`/`ledger_*`/`instrument_type` columns. The prefix keeps both
    generations of identity evidence visible side by side rather than one silently
    overwriting the other. [CDiP][KBT]"""
    identities = (
        load_best_identity_facts(fact_root)
        .with_columns(identity_usable_default_expr().alias("identity_usable_default"))
        .drop("identity_entity_id", "identity_method")
        .rename(
            {
                "identity_tier": "canonical_identity_tier",
                "identity_issuer": "canonical_identity_issuer",
                "identity_instrument": "canonical_identity_instrument",
                "identity_contested": "canonical_identity_contested",
                "identity_usable_default": "canonical_identity_usable_default",
            }
        )
    )
    events = load_best_event_facts(fact_root).rename(
        {
            "event_type": "canonical_event_type",
            "event_date": "canonical_event_date",
            "event_verification": "canonical_event_verification",
        }
    )
    return identities.join(events, on="symbol_era_id", how="full", coalesce=True)


def identity_usable_default_expr() -> pl.Expr:
    """`True` when a joined `identity_tier`/`identity_contested` pair is safe to use
    without an explicit tier choice: `verified`/`corroborated` always, `openfigi_asserted`
    only when not flagged `contested`. Mirrors the default view used across the era
    identity product; keep both call sites using this expression rather than
    re-deriving the rule so the "usable by default" definition cannot drift."""
    return (
        pl.when(pl.col("identity_tier").is_in(["verified", "corroborated"]))
        .then(True)
        .when((pl.col("identity_tier") == "openfigi_asserted") & ~pl.col("identity_contested"))
        .then(True)
        .otherwise(False)
    )


def _best_per_era(facts: list[dict[str, Any]], row_fn: Any) -> list[dict[str, Any]]:
    best: dict[str, tuple[int, dict[str, Any]]] = {}
    for fact in facts:
        rank = TIER_RANK.get(str(fact.get("verification_state")), 9)
        era = fact.get("symbol_era_id")
        if era and (era not in best or rank < best[era][0]):
            best[era] = (rank, row_fn(fact))
    return [row for _, row in best.values()]


def _identity_row(fact: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol_era_id": fact["symbol_era_id"],
        "identity_tier": fact.get("verification_state"),
        "identity_issuer": fact.get("issuer"),
        "identity_entity_id": fact.get("entity_id"),
        "identity_method": fact.get("evidence_method"),
        "identity_instrument": fact.get("instrument"),
        "identity_contested": "contested" in (fact.get("flags") or []),
    }


def _event_row(fact: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol_era_id": fact["symbol_era_id"],
        "event_type": fact.get("event_type"),
        "event_date": fact.get("event_date"),
        "event_verification": fact.get("verification_state"),
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
