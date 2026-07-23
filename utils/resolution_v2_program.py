from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.framework.logging import get_logger
from utils.resolution_v2_lanes import refresh_decisions, run_network_lanes
from utils.resolution_v2_local_reconcile import run_local_reconciliation
from utils.resolution_v2_migration import build_legacy_migration
from utils.resolution_v2_outputs import (
    project_legacy_overrides,
    reconciliation_summary,
    snapshot_cohort,
    write_dimension_queues,
    write_summary,
)
from utils.resolution_v2_registry import EvidenceRegistry
from utils.resolution_v2_salvage import salvage_cached_events, update_decisions_for_events
from utils.resolution_v2_schema import (
    DEFAULT_FACT_ROOT,
    DEFAULT_REPORT_ROOT,
    RESOLVER_VERSION,
    fingerprint,
)
from utils.resolution_v2_store import CanonicalFactStore

LOGGER = get_logger(__name__)
FACT_KINDS = ("identity", "event", "observation", "decision", "attempt")


@dataclass(frozen=True)
class ProgramConfig:
    apply: bool = False
    local_only: bool = False
    fact_root: Path = DEFAULT_FACT_ROOT
    report_root: Path = DEFAULT_REPORT_ROOT
    user_agent: str = ""
    network_budget: int = 25_000
    batch_size: int = 250
    delay_seconds: float = 0.25
    timeout_seconds: float = 10.0
    retries: int = 3


def run_resolution_program(config: ProgramConfig) -> dict[str, Any]:
    migration, cached_events = _prepare_migration()
    stage = _prepare_stage(config, migration)
    facts, output_root = _facts_and_output(config, stage["store"])
    summary = _reconcile(config, migration, cached_events, stage, facts, output_root)
    write_summary(output_root / "resolution_program_summary.json", summary)
    LOGGER.info(
        "Resolution V2 reconciliation complete",
        extra={"event": "resolution_v2_reconciled", "detail": summary},
    )
    return summary


def _prepare_migration() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    migration = build_legacy_migration()
    cached_events = salvage_cached_events(migration["identity"])
    migration["event"] = _unique(migration["event"] + cached_events)
    migration["decision"] = update_decisions_for_events(migration["decision"], migration["event"])
    return migration, cached_events


def _prepare_stage(config: ProgramConfig, migration: dict[str, Any]) -> dict[str, Any]:
    cohort_sha, cohort_created = snapshot_cohort(config.fact_root, migration["cohort"])
    stage_mode = "local" if config.local_only else "network"
    stage_id = fingerprint(
        {"cohort": cohort_sha, "resolver": RESOLVER_VERSION, "mode": stage_mode}
    )[:20]
    stage_root = config.fact_root / "staged" / stage_id
    stage_store = CanonicalFactStore(stage_root)
    manifest = _read_json(stage_root / "stage_manifest.json")
    if config.apply and manifest.get("status") != "complete":
        raise ValueError("--apply requires a completed dry-run stage for this stable cohort")
    if manifest.get("status") != "complete":
        _initialize_stage(stage_store, migration)
        run_local_reconciliation(stage_store, migration["cohort"], stage_root)
        stop_reason = run_network_lanes(config, stage_store, migration["cohort"])
        _finalize_stage(stage_store, stage_root, cohort_sha, migration, stop_reason)
    else:
        stop_reason = str(manifest.get("stopping_reason") or "unchanged_evidence_snapshot")
    return {
        "store": stage_store,
        "cohort_sha": cohort_sha,
        "cohort_created": cohort_created,
        "stage_id": stage_id,
        "stage_mode": stage_mode,
        "stop_reason": stop_reason,
        "local_reconciliation": _read_json(stage_root / "local_reconciliation.json"),
    }


def _facts_and_output(
    config: ProgramConfig, stage_store: CanonicalFactStore
) -> tuple[dict[str, list[dict[str, Any]]], Path]:
    facts = {kind: stage_store.load(kind) for kind in FACT_KINDS}
    if config.apply:
        facts = _apply_stage(config.fact_root, facts)
    output_root = config.report_root if config.apply else config.report_root / "dry-run"
    return facts, output_root


def _reconcile(
    config: ProgramConfig,
    migration: dict[str, Any],
    cached_events: list[dict[str, Any]],
    stage: dict[str, Any],
    facts: dict[str, list[dict[str, Any]]],
    output_root: Path,
) -> dict[str, Any]:
    queue_counts = write_dimension_queues(output_root, facts["decision"], migration["cohort"])
    summary = reconciliation_summary(
        migration["cohort"], facts, queue_counts, _metrics(config), stage["stop_reason"]
    )
    summary.update(_summary_metadata(config, migration, cached_events, stage))
    if config.apply:
        summary["legacy_projection_rows"] = _project(config, facts)
    return summary


def _metrics(config: ProgramConfig) -> dict[str, int]:
    registry = EvidenceRegistry(config.fact_root / "evidence_registry.sqlite")
    metrics = {name: registry.metric(name) for name in ("network_requests", "cache_hits")}
    row = registry.connection.execute(
        "SELECT COALESCE(SUM(facts), 0), COALESCE(SUM(requests), 0) FROM attempts"
    ).fetchone()
    recorded = min(int(row[1]), metrics["network_requests"])
    metrics.update({"resolver_fact_yield": int(row[0]), "resolver_recorded_requests": recorded})
    registry.close()
    return metrics


def _summary_metadata(
    config: ProgramConfig,
    migration: dict[str, Any],
    cached_events: list[dict[str, Any]],
    stage: dict[str, Any],
) -> dict[str, Any]:
    return {
        "applied": config.apply,
        "cohort_sha256": stage["cohort_sha"],
        "cohort_created": stage["cohort_created"],
        "stage_id": stage["stage_id"],
        "stage_mode": stage["stage_mode"],
        "migration_counts": migration["migration_counts"],
        "cached_event_salvage_count": len(cached_events),
        "local_reconciliation": stage["local_reconciliation"],
    }


def _project(config: ProgramConfig, facts: dict[str, list[dict[str, Any]]]) -> int:
    path = config.fact_root / "historical_ticker_identities_projection.csv"
    return project_legacy_overrides(path, facts["identity"], facts["event"])


def _initialize_stage(store: CanonicalFactStore, migration: dict[str, Any]) -> None:
    for kind in FACT_KINDS:
        if not store.path(kind).exists():
            store.replace(kind, migration[kind])


def _finalize_stage(
    store: CanonicalFactStore, root: Path, cohort_sha: str, migration: dict[str, Any], reason: str
) -> None:
    refresh_decisions(store)
    write_summary(
        root / "stage_manifest.json",
        {
            "status": "complete",
            "cohort_sha256": cohort_sha,
            "resolver_version": RESOLVER_VERSION,
            "stopping_reason": reason,
            "migration_counts": migration["migration_counts"],
        },
    )


def _apply_stage(
    root: Path, facts: dict[str, list[dict[str, Any]]]
) -> dict[str, list[dict[str, Any]]]:
    canonical = CanonicalFactStore(root)
    for kind in FACT_KINDS:
        added, total = _apply_kind(canonical, kind, facts[kind])
        LOGGER.info(
            "Canonical facts imported",
            extra={
                "event": "resolution_v2_import",
                "detail": {"kind": kind, "added": added, "total": total},
            },
        )
    return {kind: canonical.load(kind) for kind in FACT_KINDS}


def _apply_kind(
    canonical: CanonicalFactStore, kind: str, records: list[dict[str, Any]]
) -> tuple[int, int]:
    if kind == "decision":
        total = canonical.replace(kind, records)
        return 0, total
    return canonical.merge(kind, records)


def _unique(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return list({row["fact_id"]: row for row in rows}.values())


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
