from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utils.import_dead_ticker_manual_overrides import (
    ManualOverrideImportConfig,
    import_manual_overrides,
)


def test_import_manual_overrides_appends_verified_rows_only(tmp_path: Path) -> None:
    template_path = tmp_path / "template.csv"
    overrides_path = tmp_path / "overrides.csv"
    summary_path = tmp_path / "summary.json"
    _write_template(template_path)
    _write_existing(overrides_path)

    result = import_manual_overrides(
        ManualOverrideImportConfig(
            template_path=template_path,
            overrides_path=overrides_path,
            summary_path=summary_path,
            dry_run=False,
        )
    )

    rows = {
        row["symbol_era_id"]: row
        for row in pl.read_csv(overrides_path, infer_schema_length=0).to_dicts()
    }
    assert result["summary"]["verified_row_count"] == 1
    assert "AAA#001" in rows
    assert "BBB#001" not in rows
    assert rows["AAA#001"]["historical_issuer_name"] == "AAA Corp."
    assert rows["AAA#001"]["source_url"] == "https://example.test/aaa-8k"
    assert summary_path.exists()


def test_import_manual_overrides_rejects_existing_symbol_era_id(tmp_path: Path) -> None:
    template_path = tmp_path / "template.csv"
    overrides_path = tmp_path / "overrides.csv"
    _write_template(template_path)
    _write_existing(overrides_path, symbol_era_id="AAA#001")

    with pytest.raises(ValueError, match="already exist"):
        import_manual_overrides(
            ManualOverrideImportConfig(
                template_path=template_path,
                overrides_path=overrides_path,
                summary_path=tmp_path / "summary.json",
                dry_run=False,
            )
        )


def test_import_manual_overrides_rejects_missing_verified_evidence(tmp_path: Path) -> None:
    template_path = tmp_path / "template.csv"
    overrides_path = tmp_path / "overrides.csv"
    _write_template(template_path, primary_source_url=None)
    _write_existing(overrides_path)

    with pytest.raises(ValueError, match="missing required"):
        import_manual_overrides(
            ManualOverrideImportConfig(
                template_path=template_path,
                overrides_path=overrides_path,
                summary_path=tmp_path / "summary.json",
                dry_run=False,
            )
        )


def test_import_manual_overrides_dry_run_does_not_modify_file(tmp_path: Path) -> None:
    template_path = tmp_path / "template.csv"
    overrides_path = tmp_path / "overrides.csv"
    _write_template(template_path)
    _write_existing(overrides_path)
    before = overrides_path.read_text(encoding="utf-8")

    import_manual_overrides(
        ManualOverrideImportConfig(
            template_path=template_path,
            overrides_path=overrides_path,
            summary_path=tmp_path / "summary.json",
            dry_run=True,
        )
    )

    assert overrides_path.read_text(encoding="utf-8") == before


def _write_template(
    path: Path, primary_source_url: str | None = "https://example.test/aaa-8k"
) -> None:
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB"],
            "symbol_era_id": ["AAA#001", "BBB#001"],
            "research_status": ["verified", "todo"],
            "proposed_historical_identity_status": ["manual_verified_acquired_delisted", None],
            "proposed_historical_issuer_name": ["AAA Corp.", None],
            "proposed_historical_event_type": ["acquired_delisted", None],
            "proposed_historical_event_date": ["2024-01-02", None],
            "proposed_historical_successor": ["Buyer Corp.", None],
            "primary_source_url": [primary_source_url, None],
            "research_note": ["AAA merger closed.", None],
        }
    ).write_csv(path)


def _write_existing(path: Path, symbol_era_id: str = "OLD#001") -> None:
    pl.DataFrame(
        {
            "symbol": ["OLD"],
            "symbol_era_id": [symbol_era_id],
            "historical_identity_status": ["manual_verified_acquired_delisted"],
            "historical_issuer_name": ["Old Corp."],
            "historical_event_type": ["acquired_delisted"],
            "historical_event_date": ["2020-01-02"],
            "historical_successor": ["Buyer Inc."],
            "source_url": ["https://example.test/old"],
            "source_note": ["Old merger closed."],
        }
    ).write_csv(path)
