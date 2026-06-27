from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.validate_stable_daily_panel import PanelValidationConfig, validate_stable_daily_panel


def test_validate_stable_daily_panel_passes_clean_panel(tmp_path: Path) -> None:
    panel_path = tmp_path / "panel.parquet"
    events_path = tmp_path / "events.parquet"
    report_root = tmp_path / "report"
    _write_panel(panel_path)
    _write_events(events_path, include_expected=True)

    result = validate_stable_daily_panel(
        PanelValidationConfig(
            panel_path=panel_path,
            quality_events_path=events_path,
            report_root=report_root,
        )
    )

    summary = result["summary"]
    assert summary["validation_status"] == "pass"
    assert summary["duplicate_key_count"] == 0
    assert summary["critical_null_count"] == 0
    assert summary["quality_flag_source_mismatch_count"] == 0
    assert (report_root / "year_tier_coverage.csv").exists()
    assert (report_root / "stable_daily_panel_validation_report.md").exists()


def test_validate_stable_daily_panel_fails_quality_source_mismatch(tmp_path: Path) -> None:
    panel_path = tmp_path / "panel.parquet"
    events_path = tmp_path / "events.parquet"
    _write_panel(panel_path)
    _write_events(events_path, include_expected=False)

    result = validate_stable_daily_panel(
        PanelValidationConfig(
            panel_path=panel_path,
            quality_events_path=events_path,
            report_root=tmp_path / "report",
        )
    )

    summary = result["summary"]
    assert summary["validation_status"] == "fail"
    assert summary["quality_flag_source_mismatch_count"] == 1


def _write_panel(path: Path) -> None:
    pl.DataFrame(
        {
            "day": ["20250102", "20250103"],
            "symbol": ["AAA", "AAA"],
            "symbol_era_id": ["AAA#001", "AAA#001"],
            "open": [10.0, 10.5],
            "high": [11.0, 11.5],
            "low": [9.0, 10.0],
            "close": [10.5, 11.0],
            "volume": [1000, 1100],
            "trade_count": [100, 110],
            "notional": [10_000.0, 12_000.0],
            "vwap": [10.0, 10.9],
            "first_timestamp": [1, 3],
            "last_timestamp": [2, 4],
            "liquidity_tier": ["core_liquid", "core_liquid"],
            "expected_days_in_era": [2, 2],
            "iex_latest_issuer": ["ALPHA INC", "ALPHA INC"],
            "iex_entity_confidence": ["iex_snapshot_overlap", "iex_snapshot_overlap"],
            "quality_invalid_ohlc": [False, False],
            "quality_nonpositive_price": [False, False],
            "quality_near_zero_price": [False, False],
            "quality_extreme_return": [True, False],
            "quality_notional_outlier": [False, False],
            "quality_volume_outlier": [False, False],
            "quality_has_event": [True, False],
        }
    ).write_parquet(path)


def _write_events(path: Path, include_expected: bool) -> None:
    rows = []
    if include_expected:
        rows.append(
            {
                "day": "20250102",
                "symbol": "AAA",
                "symbol_era_id": "AAA#001",
                "event_type": "extreme_return",
            }
        )
    pl.DataFrame(
        rows,
        schema={
            "day": pl.String,
            "symbol": pl.String,
            "symbol_era_id": pl.String,
            "event_type": pl.String,
        },
    ).write_parquet(path)
