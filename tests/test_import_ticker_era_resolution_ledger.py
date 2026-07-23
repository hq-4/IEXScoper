from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utils.import_ticker_era_resolution_ledger import (
    ResolutionLedgerImportConfig,
    import_resolution_ledger,
)


def test_import_resolution_ledger_appends_new_rows(tmp_path: Path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    ledger_path = tmp_path / "ledger.csv"
    summary_path = tmp_path / "summary.json"
    _write_candidates(candidates_path)
    _write_ledger(ledger_path)

    result = import_resolution_ledger(
        ResolutionLedgerImportConfig(
            candidates_path=candidates_path,
            ledger_path=ledger_path,
            summary_path=summary_path,
            dry_run=False,
        )
    )

    rows = pl.read_csv(ledger_path, infer_schema_length=0)
    assert result["summary"]["appended_row_count"] == 1
    assert set(rows["symbol_era_id"].to_list()) == {"OLD#001", "ABC-A#001"}
    assert summary_path.exists()


def test_import_resolution_ledger_dry_run_does_not_modify_file(tmp_path: Path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    ledger_path = tmp_path / "ledger.csv"
    _write_candidates(candidates_path)
    _write_ledger(ledger_path)
    before = ledger_path.read_text(encoding="utf-8")

    import_resolution_ledger(
        ResolutionLedgerImportConfig(
            candidates_path=candidates_path,
            ledger_path=ledger_path,
            summary_path=tmp_path / "summary.json",
            dry_run=True,
        )
    )

    assert ledger_path.read_text(encoding="utf-8") == before


def test_import_resolution_ledger_rejects_missing_required_values(tmp_path: Path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    ledger_path = tmp_path / "ledger.csv"
    _write_candidates(candidates_path, source_note=None)
    _write_ledger(ledger_path)

    with pytest.raises(ValueError, match="missing required values"):
        import_resolution_ledger(
            ResolutionLedgerImportConfig(
                candidates_path=candidates_path,
                ledger_path=ledger_path,
                summary_path=tmp_path / "summary.json",
                dry_run=False,
            )
        )


def _write_candidates(path: Path, source_note: str | None = "ABC preferred linked.") -> None:
    pl.DataFrame(
        {
            "symbol": ["ABC-A"],
            "symbol_era_id": ["ABC-A#001"],
            "resolution_status": ["terminal_disposition"],
            "resolution_disposition": ["terminal_parent_security_linked"],
            "evidence_tier": ["local_parent_root_evidence"],
            "research_route": ["preferred_redemption_or_delisting"],
            "instrument_type": ["probable_preferred"],
            "historical_issuer_name": ["ABC Corp"],
            "event_type": ["preferred_security_terminal_action"],
            "event_date": ["20220102"],
            "successor": [None],
            "primary_source_url": ["local:parent-root-current-evidence"],
            "secondary_source_url": [None],
            "source_note": [source_note],
            "resolver": ["unit_test"],
        }
    ).write_csv(path)


def _write_ledger(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["OLD"],
            "symbol_era_id": ["OLD#001"],
            "resolution_status": ["terminal_disposition"],
            "resolution_disposition": ["terminal_parent_security_linked"],
            "evidence_tier": ["local_parent_root_evidence"],
            "research_route": ["warrant_unit_right_security_action"],
            "instrument_type": ["probable_warrant"],
            "historical_issuer_name": ["Old Corp"],
            "event_type": ["warrant_unit_right_terminal_action"],
            "event_date": ["20200102"],
            "successor": [None],
            "primary_source_url": ["local:parent-root-current-evidence"],
            "secondary_source_url": [None],
            "source_note": ["Existing row."],
            "resolver": ["unit_test"],
        }
    ).write_csv(path)
