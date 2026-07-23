from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Any, Callable, Protocol

from src.framework.logging import get_logger
from utils.resolution_v2_network import CachedPrimaryClient, NetworkConfig, PrimarySourceError
from utils.resolution_v2_policy import BatchYield, StoppingPolicy, cache_policy_for_era
from utils.resolution_v2_registry import CachePolicy, EvidenceRegistry, ResumeRegistry
from utils.resolution_v2_schema import VERIFIED, evidence_fingerprint, prepare_fact
from utils.resolution_v2_sec import resolve_known_identity_event, resolve_sec_identity
from utils.resolution_v2_store import CanonicalFactStore
from utils.sec_identity_evidence import parse_day

LOGGER = get_logger(__name__)
HIGH_IMPACT_WORKPLAN = Path(
    "reports/dead-ticker-review/resolution-workplan/workplan_high_impact_operating.csv"
)


class LaneConfig(Protocol):
    local_only: bool
    fact_root: Any
    user_agent: str
    network_budget: int
    batch_size: int
    delay_seconds: float
    timeout_seconds: float
    retries: int


def run_network_lanes(
    config: LaneConfig, store: CanonicalFactStore, cohort: list[dict[str, Any]]
) -> str:
    if config.local_only:
        return "local_only_dry_run"
    network = NetworkConfig(
        user_agent=config.user_agent,
        budget=config.network_budget,
        delay_seconds=config.delay_seconds,
        timeout_seconds=config.timeout_seconds,
        retries=config.retries,
    )
    registry = EvidenceRegistry(config.fact_root / "evidence_registry.sqlite")
    client = CachedPrimaryClient(network, registry)
    try:
        stop = _event_lane(config, store, cohort, client)
        if stop.startswith("network_"):
            return stop
        return _identity_lane(config, store, cohort, client)
    finally:
        registry.close()


def refresh_decisions(store: CanonicalFactStore) -> None:
    identities = set(_verified_by_era(store.load("identity")))
    events = set(_verified_by_era(store.load("event")))
    refreshed = [_updated_decision(row, identities, events) for row in store.load("decision")]
    store.replace("decision", refreshed)


def _event_lane(
    config: LaneConfig,
    store: CanonicalFactStore,
    cohort: list[dict[str, Any]],
    client: CachedPrimaryClient,
) -> str:
    identities = _verified_by_era(store.load("identity"))
    verified_events = set(_verified_by_era(store.load("event")))
    rows = [
        row
        for row in _ranked(cohort)
        if row["symbol_era_id"] in identities and row["symbol_era_id"] not in verified_events
    ]
    resolve = lambda row: resolve_known_identity_event(  # noqa: E731
        row, identities[row["symbol_era_id"]], client
    )
    return _process_lane(config, store, rows, "known_identity_event_salvage", client, resolve)


def _identity_lane(
    config: LaneConfig,
    store: CanonicalFactStore,
    cohort: list[dict[str, Any]],
    client: CachedPrimaryClient,
) -> str:
    known = set(_verified_by_era(store.load("identity")))
    high_impact = _high_impact_ids(HIGH_IMPACT_WORKPLAN)
    rows = [row for row in _ranked(cohort, high_impact) if row["symbol_era_id"] not in known]

    def resolve(row: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        policy = cache_policy_for_era(parse_day(row.get("last_day")), date.today())
        return resolve_sec_identity(row, client, CachePolicy(**policy))

    return _process_lane(config, store, rows, "identity_recovery", client, resolve)


def _process_lane(
    config: LaneConfig,
    store: CanonicalFactStore,
    rows: list[dict[str, Any]],
    resolver: str,
    client: CachedPrimaryClient,
    resolve: Callable[[dict[str, Any]], tuple[dict[str, Any] | None, list[str]]],
) -> str:
    resume = ResumeRegistry(client.registry.connection)
    stopping = StoppingPolicy()
    lane_trade_rows = sum(int(row.get("trade_rows") or 0) for row in rows)
    for start in range(0, len(rows), config.batch_size):
        outcome = _process_batch(
            rows[start : start + config.batch_size], resolver, resolve, resume, client, store
        )
        if outcome.get("error"):
            return str(outcome["error"])
        stopping.add(_batch_yield(outcome, lane_trade_rows))
        should_stop, reason = stopping.should_stop()
        _log_batch(resolver, start // config.batch_size + 1, outcome, reason)
        if should_stop:
            refresh_decisions(store)
            return reason
    refresh_decisions(store)
    return "lane_exhausted"


def _process_batch(
    rows: list[dict[str, Any]],
    resolver: str,
    resolve: Callable[[dict[str, Any]], tuple[dict[str, Any] | None, list[str]]],
    resume: ResumeRegistry,
    client: CachedPrimaryClient,
    store: CanonicalFactStore,
) -> dict[str, Any]:
    before = client.requests
    outcome = {"rows": 0, "facts": 0, "impact": 0, "requests": 0}
    try:
        for row in rows:
            _process_row(row, resolver, resolve, resume, client, store, outcome)
    except PrimarySourceError as exc:
        _record_network_failure(outcome, resolver, exc)
    outcome["requests"] = client.requests - before
    return outcome


def _process_row(
    row: dict[str, Any],
    resolver: str,
    resolve: Callable[[dict[str, Any]], tuple[dict[str, Any] | None, list[str]]],
    resume: ResumeRegistry,
    client: CachedPrimaryClient,
    store: CanonicalFactStore,
    outcome: dict[str, Any],
) -> None:
    evidence = evidence_fingerprint(row, _requirements(resolver))
    if resume.completed(row["symbol_era_id"], resolver, evidence):
        return
    outcome["rows"] += 1
    row_before = client.requests
    fact, missing = resolve(row)
    if fact:
        store.merge(fact["record_type"], [fact])
        outcome["facts"] += 1
        outcome["impact"] += int(row.get("trade_rows") or 0)
    result = _attempt_result(row, fact, missing, client.requests - row_before)
    resume.record(row["symbol_era_id"], resolver, evidence, result)


def _record_network_failure(
    outcome: dict[str, Any], resolver: str, error: PrimarySourceError
) -> None:
    outcome["error"] = (
        "network_budget_exhausted"
        if "budget" in str(error)
        else "network_transport_circuit_breaker"
    )
    detail = {"resolver": resolver, "error": str(error)}
    LOGGER.warning(
        "Resolution V2 network lane stopped", extra={"event": outcome["error"], "detail": detail}
    )


def _batch_yield(outcome: dict[str, Any], lane_trade_rows: int) -> BatchYield:
    return BatchYield(
        outcome["rows"],
        outcome["requests"],
        outcome["facts"],
        outcome["impact"],
        lane_trade_rows,
    )


def _updated_decision(
    decision: dict[str, Any], identities: set[str], events: set[str]
) -> dict[str, Any]:
    values = {
        key: value
        for key, value in decision.items()
        if key not in {"fact_id", "created_at", "record_type", "resolver_version"}
    }
    era = decision["symbol_era_id"]
    values["identity_status"] = VERIFIED if era in identities else values["identity_status"]
    values["event_status"] = VERIFIED if era in events else values["event_status"]
    if (
        values["identity_status"] == VERIFIED
        and values["event_status"] == VERIFIED
        and values["research_status"] == "action_required"
    ):
        values["eligibility"], values["next_resolver"] = "eligible", "instrument_verification_v2"
    elif values["identity_status"] == VERIFIED and values["research_status"] == "action_required":
        values["next_resolver"] = "known_identity_event_salvage_v2"
    return prepare_fact("decision", values)


def _attempt_result(
    row: dict[str, Any], fact: dict[str, Any] | None, missing: list[str], requests: int
) -> dict[str, Any]:
    return {
        "status": "completed" if fact else "automation_exhausted",
        "missing_requirements": missing,
        "requests": requests,
        "facts": int(fact is not None),
        "trade_rows": int(row.get("trade_rows") or 0) if fact else 0,
    }


def _requirements(resolver: str) -> list[str]:
    return (
        ["hard_gated_endpoint_event"]
        if resolver.startswith("known_identity")
        else ["exact_filer_ticker_or_dei", "unique_date_scoped_entity"]
    )


def _verified_by_era(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["symbol_era_id"]: row for row in rows if row.get("verification_state") == VERIFIED}


def _ranked(
    rows: list[dict[str, Any]], priority_ids: set[str] | None = None
) -> list[dict[str, Any]]:
    priority_ids = priority_ids or set()
    return sorted(
        rows,
        key=lambda row: (
            int(row["symbol_era_id"] not in priority_ids),
            -int(row.get("trade_rows") or 0),
            row["symbol_era_id"],
        ),
    )


def _high_impact_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open(newline="", encoding="utf-8") as handle:
        return {row["symbol_era_id"] for row in csv.DictReader(handle)}


def _log_batch(resolver: str, batch: int, outcome: dict[str, Any], reason: str) -> None:
    LOGGER.info(
        "Resolution V2 batch complete",
        extra={
            "event": "resolution_v2_batch",
            "detail": {
                "resolver": resolver,
                "batch": batch,
                "facts": outcome["facts"],
                "requests": outcome["requests"],
                "stopping_decision": reason,
            },
        },
    )
