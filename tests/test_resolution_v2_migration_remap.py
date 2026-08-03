from __future__ import annotations

import csv
from pathlib import Path

import pytest

from utils.resolution_v2_migration import build_legacy_migration

REVIEW_COLUMNS = [
    "symbol",
    "symbol_era_id",
    "source_classification",
    "first_day",
    "last_day",
    "trade_rows",
    "instrument_type",
]
OVERRIDE_COLUMNS = [
    "symbol",
    "symbol_era_id",
    "historical_identity_status",
    "historical_issuer_name",
    "historical_event_type",
    "historical_event_date",
    "historical_successor",
    "source_url",
    "source_note",
]
LEDGER_COLUMNS = ["symbol", "symbol_era_id", "resolution_disposition"]


def _write(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _base_files(tmp_path: Path) -> dict[str, Path]:
    paths = {
        "review": tmp_path / "review.csv",
        "override": tmp_path / "overrides.csv",
        "ledger": tmp_path / "ledger.csv",
        "workplan": tmp_path / "workplan.csv",
        "remap": tmp_path / "remap.csv",
        "state": tmp_path / "missing_state.json",
    }
    _write(
        paths["review"],
        REVIEW_COLUMNS,
        [
            {"symbol": "AAA", "symbol_era_id": "AAA#001", "source_classification": "delisted_or_acquired_candidate", "first_day": "20161212", "last_day": "20200101", "trade_rows": "100", "instrument_type": "probable_operating_company"},
            {"symbol": "BBB", "symbol_era_id": "BBB#001", "source_classification": "delisted_or_acquired_candidate", "first_day": "20161212", "last_day": "20200101", "trade_rows": "50", "instrument_type": "probable_operating_company"},
        ],
    )
    _write(
        paths["override"],
        OVERRIDE_COLUMNS,
        [
            {"symbol": "AAA", "symbol_era_id": "AAA#009", "historical_identity_status": "manual_verified_acquired_delisted", "historical_issuer_name": "A Corp", "historical_event_type": "acquired_delisted", "historical_event_date": "2020-01-01", "historical_successor": "B Corp", "source_url": "https://example.com", "source_note": "n"},
        ],
    )
    _write(
        paths["ledger"],
        LEDGER_COLUMNS,
        [
            {"symbol": "AAA", "symbol_era_id": "AAA#009", "resolution_disposition": "terminal_parent_security_linked"},
            {"symbol": "ZZZ", "symbol_era_id": "ZZZ#001", "resolution_disposition": "low_materiality_market_data_artifact"},
        ],
    )
    _write(paths["workplan"], ["symbol", "symbol_era_id", "workplan_bucket"], [])
    _write(
        paths["remap"],
        ["old_era_id", "new_era_id", "match_kind"],
        [
            {"old_era_id": "AAA#009", "new_era_id": "AAA#001", "match_kind": "id_shift"},
            {"old_era_id": "BBB#001", "new_era_id": "BBB#001", "match_kind": "unchanged"},
        ],
    )
    return paths


def test_migration_translates_legacy_keys_and_drops_vanished(tmp_path: Path) -> None:
    paths = _base_files(tmp_path)
    migration = build_legacy_migration(
        review_path=paths["review"],
        override_path=paths["override"],
        ledger_path=paths["ledger"],
        workplan_path=paths["workplan"],
        identity_state_path=paths["state"],
        remap_path=paths["remap"],
    )
    counts = migration["migration_counts"]
    assert counts["legacy_identities"] == 1
    assert counts["overrides_remapped"] == 1
    assert counts["ledger_remapped"] == 1
    assert counts["ledger_vanished_dropped"] == 1
    identity = migration["identity"][0]
    assert identity["symbol_era_id"] == "AAA#001"
    assert identity["valid_from"] == "20161212"
    decision = next(d for d in migration["decision"] if d["symbol_era_id"] == "AAA#001")
    assert decision["research_status"] == "research_closed"


def test_migration_raises_when_override_sits_on_vanished_era(tmp_path: Path) -> None:
    paths = _base_files(tmp_path)
    _write(
        paths["remap"],
        ["old_era_id", "new_era_id", "match_kind"],
        [{"old_era_id": "BBB#001", "new_era_id": "BBB#001", "match_kind": "unchanged"}],
    )
    with pytest.raises(ValueError, match="verified override on vanished era: AAA#009"):
        build_legacy_migration(
            review_path=paths["review"],
            override_path=paths["override"],
            ledger_path=paths["ledger"],
            workplan_path=paths["workplan"],
            identity_state_path=paths["state"],
            remap_path=paths["remap"],
        )
