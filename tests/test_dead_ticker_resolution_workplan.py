from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import utils.dead_ticker_resolution_workplan as workplan_module
from utils.dead_ticker_resolution_workplan import (
    ResolutionWorkplanConfig,
    build_resolution_workplan,
)
from utils.resolution_ledger import RESOLUTION_COLUMNS, validate_resolution_ledger


def test_workplan_routes_default_buckets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lanes_path = tmp_path / "resolution_lanes.csv"
    output_root = tmp_path / "workplan"
    monkeypatch.setattr(workplan_module, "HIGH_IMPACT_RANK_LIMIT", 1)
    _write_lanes(lanes_path)

    result = build_resolution_workplan(_config(lanes_path, output_root))

    rows = pl.read_csv(output_root / "workplan_all.csv", infer_schema_length=0)
    by_id = {row["symbol_era_id"]: row["workplan_bucket"] for row in rows.to_dicts()}
    assert by_id["BIG#001"] == "high_impact_operating"
    assert by_id["SPARSE#001"] == "low_materiality_bulk_disposition"
    assert by_id["UNITU#001"] == "derivative_parent_resolution"
    assert by_id["PART#001"] == "operating_lifecycle_search"
    assert by_id["ODD#001"] == "manual_review_hold"
    assert result["summary"]["bucket_counts"]["high_impact_operating"] == 1


def test_workplan_impact_math_is_deterministic(tmp_path: Path) -> None:
    lanes_path = tmp_path / "resolution_lanes.csv"
    output_root = tmp_path / "workplan"
    _write_lanes(lanes_path)

    build_resolution_workplan(_config(lanes_path, output_root))

    rows = pl.read_csv(output_root / "workplan_all.csv", infer_schema_length=0)
    assert rows["symbol_era_id"].to_list()[:3] == ["BIG#001", "PART#001", "UNITU#001"]
    assert rows["impact_rank"].to_list()[:3] == ["1", "2", "3"]
    assert rows["cumulative_trade_rows"].to_list()[:3] == ["100000", "105000", "107000"]
    assert rows["cumulative_trade_rows_pct"].to_list()[-1] == "100.0"


def test_workplan_attaches_automation_exhaustion_status(tmp_path: Path) -> None:
    lanes_path = tmp_path / "resolution_lanes.csv"
    automation_path = tmp_path / "automation.csv"
    output_root = tmp_path / "workplan"
    _write_lanes(lanes_path)
    pl.DataFrame(
        {
            "symbol_era_id": ["BIG#001"],
            "automation_status": ["automation_exhausted"],
            "automation_workflow_version": ["sec_identity_first_v1"],
            "automation_attempt_count": [2],
            "automation_last_bucket": ["identity_only_hold"],
        }
    ).write_csv(automation_path)

    build_resolution_workplan(
        ResolutionWorkplanConfig(
            lanes_path=lanes_path,
            output_root=output_root,
            sec_evidence_path=None,
            automation_path=automation_path,
        )
    )

    rows = pl.read_csv(output_root / "workplan_all.csv", infer_schema_length=0)
    big = rows.filter(pl.col("symbol_era_id") == "BIG#001").row(0, named=True)
    assert big["automation_status"] == "automation_exhausted"
    assert big["automation_last_bucket"] == "identity_only_hold"


def test_low_materiality_ledger_candidates_validate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lanes_path = tmp_path / "resolution_lanes.csv"
    output_root = tmp_path / "workplan"
    monkeypatch.setattr(workplan_module, "HIGH_IMPACT_RANK_LIMIT", 1)
    _write_lanes(lanes_path)

    build_resolution_workplan(_config(lanes_path, output_root))

    candidates = pl.read_csv(
        output_root / "workplan_low_materiality_ledger_candidates.csv",
        infer_schema_length=0,
    )
    assert candidates.columns == RESOLUTION_COLUMNS
    assert candidates["resolution_status"].to_list() == ["terminal_disposition"]
    assert candidates["resolution_disposition"].to_list() == [
        "low_materiality_market_data_artifact"
    ]
    validate_resolution_ledger(candidates)


def test_low_materiality_duplicate_symbol_era_id_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lanes_path = tmp_path / "resolution_lanes.csv"
    monkeypatch.setattr(workplan_module, "HIGH_IMPACT_RANK_LIMIT", 0)
    _write_lanes(lanes_path, duplicate_sparse=True)

    with pytest.raises(ValueError, match="duplicate resolution ledger"):
        build_resolution_workplan(_config(lanes_path, tmp_path / "workplan"))


def test_low_materiality_missing_symbol_era_id_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lanes_path = tmp_path / "resolution_lanes.csv"
    monkeypatch.setattr(workplan_module, "HIGH_IMPACT_RANK_LIMIT", 0)
    _write_lanes(lanes_path, missing_sparse_id=True)

    with pytest.raises(ValueError, match="missing symbol_era_id"):
        build_resolution_workplan(_config(lanes_path, tmp_path / "workplan"))


def test_sec_doc_graph_keeps_top_five_docs(tmp_path: Path) -> None:
    lanes_path = tmp_path / "resolution_lanes.csv"
    evidence_path = tmp_path / "sec_evidence.csv"
    output_root = tmp_path / "workplan"
    _write_lanes(lanes_path)
    _write_sec_evidence(evidence_path)

    build_resolution_workplan(
        ResolutionWorkplanConfig(
            lanes_path=lanes_path,
            output_root=output_root,
            sec_evidence_path=evidence_path,
            sec_doc_top_n=5,
        )
    )

    doc_graph = pl.read_csv(output_root / "workplan_sec_document_graph.csv")
    graph_row = doc_graph.filter(pl.col("symbol_era_id") == "BIG#001").row(0, named=True)
    assert graph_row["sec_doc_graph_doc_count"] == 5
    assert graph_row["sec_doc_graph_bucket"] == "auto_verified_candidate"


def _config(lanes_path: Path, output_root: Path) -> ResolutionWorkplanConfig:
    return ResolutionWorkplanConfig(
        lanes_path=lanes_path,
        output_root=output_root,
        sec_evidence_path=None,
    )


def _write_lanes(
    path: Path,
    duplicate_sparse: bool = False,
    missing_sparse_id: bool = False,
) -> None:
    rows = _base_lane_rows()
    if duplicate_sparse:
        rows.append({**rows[1], "symbol": "SPARSE2"})
    if missing_sparse_id:
        rows[1]["symbol_era_id"] = ""
    pl.DataFrame(rows).write_csv(path)


def _base_lane_rows() -> list[dict[str, object]]:
    return [
        _lane(
            "BIG", "BIG#001", "operating_company_sec_event", "operating_terminal_event", 100_000, 90
        ),
        _lane(
            "SPARSE",
            "SPARSE#001",
            "operating_company_sec_event",
            "operating_sparse_intermit",
            50,
            3,
        ),
        _lane(
            "UNITU",
            "UNITU#001",
            "warrant_unit_right_security_action",
            "warrant_unit_right_action",
            2_000,
            6,
        ),
        _lane(
            "PART", "PART#001", "operating_company_sec_event", "operating_partial_window", 5_000, 20
        ),
        _lane("ODD", "ODD#001", "manual_syntax_review", "manual_syntax", 10, 2),
    ]


def _lane(
    symbol: str,
    symbol_era_id: str,
    route: str,
    lane: str,
    trade_rows: int,
    observed_days: int,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "symbol_era_id": symbol_era_id,
        "source_classification": "intermittent_or_reused_candidate",
        "research_route": route,
        "instrument_type": "probable_operating_company",
        "resolution_lane": lane,
        "first_day": "20200102",
        "last_day": "20210102",
        "observed_days": observed_days,
        "trade_rows": trade_rows,
        "sec_current_confidence": "sec_unmatched",
        "iex_entity_confidence": "iex_snapshot_unmatched",
    }


def _write_sec_evidence(path: Path) -> None:
    pl.DataFrame([_sec_doc(rank) for rank in range(1, 7)]).write_csv(path)


def _sec_doc(rank: int) -> dict[str, object]:
    return {
        "symbol_era_id": "BIG#001",
        "verifier_flags": "issuer_name_match|symbol_match|event_language|completion_language",
        "triage_reason": "deal_form; near_after_symbol_era; symbol_token_in_entity",
        "triage_score": 120 - rank,
        "form": "8-K",
        "filed_at": "2021-01-10",
        "last_day": "20210102",
        "verifier_document_url": f"https://example.test/doc-{rank}.htm",
        "primary_source_url": f"https://example.test/archive-{rank}/",
    }
