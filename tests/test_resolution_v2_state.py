from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path

import pytest

from src.framework.logging import JSONL_KEYS, enforce_dual_sinks, setup_logging
from utils.resolution_v2_policy import BatchYield, StoppingPolicy, directory_cache_policy
from utils.resolution_v2_program import ProgramConfig, _apply_kind, _metrics
from utils.resolution_v2_registry import CachePolicy, EvidenceRegistry, ResumeRegistry
from utils.resolution_v2_schema import evidence_fingerprint, prepare_fact
from utils.resolution_v2_store import CanonicalFactStore


def test_negative_cache_and_evidence_fingerprint_resume(tmp_path: Path) -> None:
    registry = EvidenceRegistry(tmp_path / "registry.sqlite")
    request = {"q": "OLD", "startdt": "2020-01-01", "enddt": "2020-12-31"}
    registry.put("sec_efts_search", request, {"hits": {"hits": []}}, negative=True)
    immutable = CachePolicy(immutable_negative=True)
    assert registry.get("sec_efts_search", request, immutable) == {"hits": {"hits": []}}

    registry.connection.execute("UPDATE requests SET resolver_version='obsolete_version'")
    registry.connection.commit()
    assert registry.get("sec_efts_search", request, immutable) is None
    registry.put("sec_efts_search", request, {"hits": {"hits": []}}, negative=True)

    registry.connection.execute("UPDATE requests SET fetched_at='2020-01-01T00:00:00+00:00'")
    registry.connection.commit()
    assert registry.get("sec_efts_search", request, CachePolicy(max_age=timedelta(days=1))) is None

    resume = ResumeRegistry(registry.connection)
    row = {"symbol_era_id": "OLD#001", "first_day": "20200101", "last_day": "20201231"}
    first = evidence_fingerprint(row, ["exact_filer_ticker"])
    changed = evidence_fingerprint(row, ["exact_filer_ticker", "dei_trading_symbol"])
    resume.record("OLD#001", "identity", first, _result())
    assert resume.completed("OLD#001", "identity", first)
    assert not resume.completed("OLD#001", "identity", changed)
    registry.close()
    assert directory_cache_policy()["max_age"] == timedelta(hours=24)


def test_document_registry_never_persists_complete_filing_text(tmp_path: Path) -> None:
    path = tmp_path / "registry.sqlite"
    body = "COMPLETE-FILING-SECRET " + "x" * 20_000
    registry = EvidenceRegistry(path)
    registry.record_document(
        "https://sec.test/doc",
        body,
        {"filing_date": "2024-01-01", "filer_cik": "1", "subject_cik": "2"},
        ["bounded completion snippet"],
    )
    row = registry.connection.execute(
        "SELECT sha256, metadata_json, snippets_json FROM documents"
    ).fetchone()
    registry.close()
    assert row and len(row[0]) == 64
    assert "bounded completion snippet" in row[2]
    assert "COMPLETE-FILING-SECRET" not in path.read_bytes().decode("utf-8", errors="ignore")


def test_stopping_requires_two_low_yield_batches_and_minimum_observation() -> None:
    policy = StoppingPolicy()
    policy.add(BatchYield(250, 500, 0, 0, 1_000_000))
    assert policy.should_stop() == (False, "minimum_observation_not_reached")
    policy.add(BatchYield(250, 500, 0, 0, 1_000_000))
    assert policy.should_stop()[0] is True

    nonzero = StoppingPolicy()
    nonzero.add(BatchYield(500, 1_000, 0, 0, 1_000_000))
    nonzero.add(BatchYield(100, 100, 1, 100, 1_000_000))
    assert nonzero.should_stop()[0] is False


def test_fact_store_is_idempotent_for_unchanged_evidence(tmp_path: Path) -> None:
    store = CanonicalFactStore(tmp_path)
    fact = prepare_fact(
        "identity",
        {
            "symbol": "OLD",
            "symbol_era_id": "OLD#001",
            "entity_id": "1",
            "issuer": "Old Corp",
            "verification_state": "verified",
        },
    )
    assert store.merge("identity", [fact]) == (1, 1)
    assert store.merge("identity", [fact]) == (0, 1)
    assert len(store.load("identity")) == 1


def test_apply_replaces_decision_projection(tmp_path: Path) -> None:
    store = CanonicalFactStore(tmp_path)
    old = prepare_fact("decision", _decision("unresolved"))
    new = prepare_fact("decision", _decision("verified"))
    assert store.merge("decision", [old]) == (1, 1)
    assert _apply_kind(store, "decision", [new]) == (0, 1)
    rows = store.load("decision")
    assert len(rows) == 1
    assert rows[0]["identity_status"] == "verified"


def test_metrics_use_actual_network_counter_for_legacy_attempt_sums(tmp_path: Path) -> None:
    registry = EvidenceRegistry(tmp_path / "evidence_registry.sqlite")
    registry.connection.execute("INSERT INTO metrics VALUES ('network_requests', 7)")
    registry.connection.execute("INSERT INTO metrics VALUES ('cache_hits', 2)")
    ResumeRegistry(registry.connection).record("OLD#001", "identity", "fp", _result(requests=100))
    registry.close()
    values = _metrics(ProgramConfig(fact_root=tmp_path))
    assert values["network_requests"] == 7
    assert values["resolver_recorded_requests"] == 7


def test_dual_logging_enforcement_and_jsonl_fields(tmp_path: Path) -> None:
    root = logging.getLogger()
    old_handlers = root.handlers[:]
    old_flag = getattr(root, "_iexscoper_logging_configured", False)
    try:
        root.handlers = []
        root.__dict__.pop("_iexscoper_logging_configured", None)
        path = tmp_path / "app.jsonl"
        setup_logging(str(path), logging.DEBUG)
        enforce_dual_sinks(root)
        logging.getLogger("test.v2").info(
            "hello", extra={"subsys": "resolution", "event": "unit", "detail": {"ok": True}}
        )
        for handler in root.handlers:
            handler.flush()
        payload = json.loads(path.read_text(encoding="utf-8").splitlines()[-1])
        assert set(JSONL_KEYS) == set(payload)
        root.handlers = root.handlers[:1]
        with pytest.raises(RuntimeError, match="exactly"):
            enforce_dual_sinks(root)
    finally:
        for handler in root.handlers:
            if handler not in old_handlers:
                handler.close()
        root.handlers = old_handlers
        if old_flag:
            root._iexscoper_logging_configured = True  # type: ignore[attr-defined]
        else:
            root.__dict__.pop("_iexscoper_logging_configured", None)


def _decision(identity_status: str) -> dict[str, object]:
    return {
        "symbol": "OLD",
        "symbol_era_id": "OLD#001",
        "identity_status": identity_status,
        "instrument_status": "unresolved",
        "event_status": "unresolved",
        "observation_status": "local_observed",
        "research_status": "action_required",
    }


def _result(requests: int = 1) -> dict[str, object]:
    return {
        "status": "automation_exhausted",
        "missing_requirements": ["dei_trading_symbol"],
        "requests": requests,
        "facts": 0,
        "trade_rows": 0,
    }
