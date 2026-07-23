from __future__ import annotations

from collections import Counter
from pathlib import Path

from utils.resolution_v2_migration import build_legacy_migration
from utils.resolution_v2_outputs import project_legacy_overrides, write_dimension_queues
from utils.resolution_v2_salvage import salvage_cached_events, update_decisions_for_events


def test_legacy_migration_counts_and_independent_statuses() -> None:
    migration = build_legacy_migration()
    counts = migration["migration_counts"]
    assert counts == {
        "legacy_identities": 364,
        "verified_events": 127,
        "event_candidates": 237,
        "promoted_identity_holds": 454,
        "research_closures": 5694,
        "lifecycle_v1_attempts": 5659,
        "cohort_rows": 26184,
    }
    decisions = migration["decision"]
    assert len(decisions) == len(migration["cohort"]) == 26184
    required = {
        "identity_status",
        "instrument_status",
        "event_status",
        "observation_status",
        "research_status",
    }
    assert all(required <= row.keys() for row in decisions)
    reasons = Counter(row["exclusion_reason"] for row in decisions)
    assert reasons["low_materiality_data_artifact"] == 3916
    assert reasons["parent_link_workflow_closure"] == 1778


def test_promoted_identity_holds_do_not_invent_terminal_events() -> None:
    migration = build_legacy_migration()
    promoted = [
        row
        for row in migration["identity"]
        if row["evidence_method"] == "sec_date_scoped_display_names"
    ]
    events = {row["symbol_era_id"] for row in migration["event"]}
    assert len(promoted) == 454
    assert all(row["verification_state"] == "verified" for row in promoted)
    assert any(row["symbol_era_id"] not in events for row in promoted)


def test_closures_leave_action_queue_but_remain_in_identity_and_event_gaps(
    tmp_path: Path,
) -> None:
    migration = build_legacy_migration()
    counts = write_dimension_queues(tmp_path, migration["decision"], migration["cohort"])
    assert counts["research_action"] == 26184 - 5694
    closure = next(
        row for row in migration["decision"] if row["research_status"] == "research_closed"
    )
    identity_text = (tmp_path / "identity_gap_queue.csv").read_text(encoding="utf-8")
    event_text = (tmp_path / "event_gap_queue.csv").read_text(encoding="utf-8")
    action_text = (tmp_path / "research_action_gap_queue.csv").read_text(encoding="utf-8")
    if closure["identity_status"] != "verified":
        assert closure["symbol_era_id"] in identity_text
    if closure["event_status"] != "verified":
        assert closure["symbol_era_id"] in event_text
    assert closure["symbol_era_id"] not in action_text


def test_legacy_projection_requires_verified_identity_and_event(tmp_path: Path) -> None:
    migration = build_legacy_migration()
    salvaged = salvage_cached_events(migration["identity"])
    events = migration["event"] + salvaged
    decisions = update_decisions_for_events(migration["decision"], events)
    assert len(decisions) == 26184
    output = tmp_path / "projection.csv"
    projected = project_legacy_overrides(output, migration["identity"], events)
    verified_event_eras = {
        row["symbol_era_id"] for row in events if row["verification_state"] == "verified"
    }
    verified_identity_eras = {
        row["symbol_era_id"]
        for row in migration["identity"]
        if row["verification_state"] == "verified"
    }
    assert projected == len(verified_event_eras & verified_identity_eras)
    assert "_lead" not in output.read_text(encoding="utf-8")
