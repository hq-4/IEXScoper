from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utils.build_stable_returns_table import StableReturnsConfig, build_stable_returns_table


def test_build_stable_returns_table_computes_returns_and_clean_flags(tmp_path: Path) -> None:
    panel_path = tmp_path / "panel.parquet"
    output_path = tmp_path / "returns.parquet"
    report_root = tmp_path / "report"
    _write_panel(panel_path)

    result = build_stable_returns_table(
        StableReturnsConfig(
            panel_path=panel_path,
            output_path=output_path,
            report_root=report_root,
            compression="zstd",
            replace=False,
        )
    )

    rows = pl.read_parquet(output_path).sort(["symbol_era_id", "day"]).to_dicts()
    assert rows[0]["dirty_return_reason"] == "first_observation"
    assert rows[0]["is_clean_return_day"] is False
    assert rows[1]["raw_close_return"] == pytest.approx(0.10)
    assert rows[1]["is_clean_return_day"] is True
    assert rows[1]["potential_corporate_action"] is False
    assert rows[2]["dirty_return_reason"] == "current_day_quality_event"
    assert rows[2]["is_clean_return_day"] is False
    assert rows[3]["dirty_return_reason"] == "previous_day_quality_event"
    assert rows[3]["potential_corporate_action"] is True
    assert result["summary"]["row_count"] == 4
    assert result["summary"]["clean_return_count"] == 1
    assert result["summary"]["potential_corporate_action_count"] == 1
    assert (report_root / "stable_returns_report.md").exists()


def test_build_stable_returns_table_refuses_existing_output(tmp_path: Path) -> None:
    panel_path = tmp_path / "panel.parquet"
    output_path = tmp_path / "returns.parquet"
    _write_panel(panel_path)
    output_path.write_text("exists", encoding="utf-8")

    with pytest.raises(FileExistsError):
        build_stable_returns_table(
            StableReturnsConfig(
                panel_path=panel_path,
                output_path=output_path,
                report_root=tmp_path / "report",
                compression="zstd",
                replace=False,
            )
        )


def _write_panel(path: Path) -> None:
    pl.DataFrame(
        {
            "day": ["20250102", "20250103", "20250106", "20250107"],
            "symbol": ["AAA"] * 4,
            "symbol_era_id": ["AAA#001"] * 4,
            "liquidity_tier": ["core_liquid"] * 4,
            "close": [10.0, 11.0, 12.0, 18.0],
            "volume": [1000, 1100, 1200, 1300],
            "trade_count": [100, 110, 120, 130],
            "notional": [10_000.0, 12_100.0, 14_400.0, 23_400.0],
            "vwap": [10.0, 11.0, 12.0, 18.0],
            "quality_has_event": [False, False, True, False],
            "iex_latest_issuer": ["ALPHA INC"] * 4,
            "iex_entity_confidence": ["iex_snapshot_overlap"] * 4,
        }
    ).write_parquet(path)
