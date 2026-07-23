from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from utils.resolution_v2_local import group_derivative_children, propagate_known_gaps
from utils.resolution_v2_store import CanonicalFactStore


def run_local_reconciliation(
    store: CanonicalFactStore, cohort: list[dict[str, Any]], stage_root: Path
) -> dict[str, int]:
    identities = store.load("identity")
    propagated, observations = propagate_known_gaps(cohort, identities)
    store.merge("identity", propagated)
    store.merge("observation", observations)
    groups = _derivative_groups(cohort, store.load("identity"))
    _write_jsonl(stage_root / "derivative_groups.jsonl", groups)
    counts = {
        "propagated_identity_facts": len(propagated),
        "continuity_observation_facts": len(observations),
        "derivative_parent_groups": len(groups),
    }
    (stage_root / "local_reconciliation.json").write_text(
        json.dumps(counts, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return counts


def _derivative_groups(
    cohort: list[dict[str, Any]], identities: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    parents = {
        row["symbol"]: row
        for row in identities
        if row.get("verification_state") == "verified" and row.get("entity_id")
    }
    children = [row for row in cohort if _is_derivative(str(row.get("instrument_type") or ""))]
    return group_derivative_children(children, parents)


def _is_derivative(instrument: str) -> bool:
    return any(
        term in instrument for term in ("warrant", "unit", "right", "preferred", "share_class")
    )


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
