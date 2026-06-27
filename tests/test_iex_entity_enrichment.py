from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from utils.build_iex_entity_enrichment import (
    EntityEnrichmentConfig,
    build_iex_entity_enrichment,
    lifecycle_frame,
)
from utils.diff_iex_entities_snapshots import Snapshot


def test_lifecycle_tracks_latest_state_and_issuer_variants() -> None:
    snapshots = [
        Snapshot(
            "2026-02-22",
            Path("a.json"),
            {
                "AAA": _entity("ALPHA INC", "1", "1"),
                "OLD": _entity("OLD TRUST", "1", "1"),
            },
        ),
        Snapshot(
            "2026-02-23",
            Path("b.json"),
            {
                "AAA": _entity("ALPHA HOLDINGS INC", "1", "0"),
                "NEW": _entity("NEW ETF", "1", "1"),
            },
        ),
    ]

    rows = {row["symbol"]: row for row in lifecycle_frame(snapshots).to_dicts()}

    assert rows["AAA"]["iex_first_seen"] == "2026-02-22"
    assert rows["AAA"]["iex_seen_days"] == 2
    assert rows["AAA"]["iex_issuer_variant_count"] == 2
    assert rows["AAA"]["iex_latest_lit"] == "0"
    assert rows["OLD"]["iex_removed_after_seen"] is True
    assert rows["NEW"]["iex_product_hint"] == "etf"


def test_build_iex_entity_enrichment_outputs_confidence_labels(tmp_path: Path) -> None:
    entities_root = tmp_path / "entities"
    symbol_eras_path = tmp_path / "symbol_eras.parquet"
    stable_path = tmp_path / "stable.parquet"
    output_root = tmp_path / "out"
    entities_root.mkdir()
    _write_snapshot(entities_root / "2026-02-22.json", {"AAA": "ALPHA INC", "OLD": "OLD TRUST"})
    _write_snapshot(
        entities_root / "2026-02-23.json",
        {"AAA": "ALPHA HOLDINGS INC", "CUR": "CURRENT CORP"},
    )
    (entities_root / "2026-02-24.json").write_text("Bad Gateway", encoding="utf-8")
    _write_symbol_eras(symbol_eras_path)
    _write_stable(stable_path)

    result = build_iex_entity_enrichment(
        EntityEnrichmentConfig(
            entities_root=entities_root,
            symbol_eras_path=symbol_eras_path,
            stable_universe_path=stable_path,
            output_root=output_root,
        )
    )

    rows = {
        row["symbol_era_id"]: row
        for row in pl.read_parquet(output_root / "symbol_eras_iex_enriched.parquet").to_dicts()
    }
    assert rows["AAA#001"]["iex_entity_confidence"] == "iex_snapshot_changed_during_window"
    assert rows["CUR#001"]["iex_entity_confidence"] == "iex_current_symbol_only"
    assert rows["OLD#001"]["iex_entity_confidence"] == "iex_snapshot_removed_before_latest"
    assert rows["ZZZ#001"]["iex_entity_confidence"] == "iex_snapshot_unmatched"
    assert result["summary"]["invalid_snapshot_count"] == 1
    assert result["summary"]["entity_symbol_count"] == 1003
    assert (output_root / "iex_entity_lifecycle.csv").exists()
    assert (output_root / "stable_long_window_universe_iex_enriched.parquet").exists()
    assert (output_root / "report.md").exists()


def _entity(issuer: str, enabled: str = "1", lit: str = "1") -> dict[str, str]:
    return {"Symbol": "", "Issuer": issuer, "date": "2026-02-22", "isEnabled": enabled, "lit": lit}


def _write_snapshot(path: Path, overrides: dict[str, str]) -> None:
    rows = []
    for symbol, issuer in overrides.items():
        row = _entity(issuer)
        row["Symbol"] = symbol
        rows.append(row)
    for index in range(1_000):
        symbol = f"T{index:04d}"
        row = _entity(f"TEST {index} INC")
        row["Symbol"] = symbol
        rows.append(row)
    path.write_text(json.dumps(rows), encoding="utf-8")


def _write_symbol_eras(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["AAA", "CUR", "OLD", "ZZZ"],
            "symbol_era_id": ["AAA#001", "CUR#001", "OLD#001", "ZZZ#001"],
            "first_day": ["20260222", "20250101", "20260222", "20260222"],
            "last_day": ["20260223", "20250131", "20260222", "20260223"],
            "recommended_use": ["long_window_candidate"] * 4,
        }
    ).write_parquet(path)


def _write_stable(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["AAA", "CUR"],
            "symbol_era_id": ["AAA#001", "CUR#001"],
            "first_day": ["20260222", "20250101"],
            "last_day": ["20260223", "20250131"],
            "liquidity_tier": ["core_liquid", "active"],
        }
    ).write_parquet(path)
