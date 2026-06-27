from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utils.build_stable_daily_panel import StableDailyPanelConfig, build_stable_daily_panel


def test_build_stable_daily_panel_filters_and_joins_quality(tmp_path: Path) -> None:
    universe_path = tmp_path / "universe.parquet"
    bars_root = tmp_path / "bars"
    events_path = tmp_path / "quality_events.parquet"
    output_path = tmp_path / "panel" / "stable_daily_panel.parquet"
    report_root = tmp_path / "report"
    _write_universe(universe_path)
    _write_bar_day(bars_root, "20250102")
    _write_bar_day(bars_root, "20250103")
    _write_quality_events(events_path)

    result = build_stable_daily_panel(
        StableDailyPanelConfig(
            universe_path=universe_path,
            daily_bars_root=bars_root,
            quality_events_path=events_path,
            output_path=output_path,
            report_root=report_root,
            accepted_confidence=("iex_snapshot_overlap",),
            start_day="20250102",
            end_day="20250103",
            compression="zstd",
            replace=False,
        )
    )

    rows = pl.read_parquet(output_path).sort(["symbol_era_id", "day"]).to_dicts()
    assert [row["symbol_era_id"] for row in rows] == ["AAA#001", "AAA#001"]
    assert rows[0]["iex_latest_issuer"] == "ALPHA INC"
    assert rows[0]["quality_extreme_return"] is True
    assert rows[0]["quality_has_event"] is True
    assert rows[1]["quality_extreme_return"] is False
    assert result["summary"]["selected_stable_era_count"] == 1
    assert result["summary"]["panel_row_count"] == 2
    assert (report_root / "stable_daily_panel_report.md").exists()


def test_build_stable_daily_panel_refuses_existing_output(tmp_path: Path) -> None:
    universe_path = tmp_path / "universe.parquet"
    bars_root = tmp_path / "bars"
    events_path = tmp_path / "quality_events.parquet"
    output_path = tmp_path / "stable_daily_panel.parquet"
    _write_universe(universe_path)
    _write_bar_day(bars_root, "20250102")
    _write_quality_events(events_path)
    output_path.write_text("exists", encoding="utf-8")

    with pytest.raises(FileExistsError):
        build_stable_daily_panel(
            StableDailyPanelConfig(
                universe_path=universe_path,
                daily_bars_root=bars_root,
                quality_events_path=events_path,
                output_path=output_path,
                report_root=tmp_path / "report",
                accepted_confidence=("iex_snapshot_overlap",),
                start_day=None,
                end_day=None,
                compression="zstd",
                replace=False,
            )
        )


def _write_universe(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB"],
            "symbol_era_id": ["AAA#001", "BBB#001"],
            "liquidity_tier": ["core_liquid", "active"],
            "first_day": ["20250102", "20250102"],
            "last_day": ["20250103", "20250103"],
            "expected_days_in_era": [2, 2],
            "coverage_ratio": [1.0, 1.0],
            "trade_day_coverage_ratio": [1.0, 1.0],
            "median_daily_notional": [1_000_000.0, 500_000.0],
            "median_daily_volume": [100_000.0, 50_000.0],
            "median_daily_trade_count": [1000.0, 500.0],
            "identity_status": ["ticker_continuity_candidate", "ticker_continuity_candidate"],
            "iex_latest_issuer": ["ALPHA INC", "BETA INC"],
            "iex_product_hint": ["operating_or_other", "operating_or_other"],
            "iex_issuer_variant_count": [1, 1],
            "iex_seen_in_latest": [True, True],
            "iex_removed_after_seen": [False, False],
            "iex_entity_confidence": ["iex_snapshot_overlap", "iex_snapshot_unmatched"],
        }
    ).write_parquet(path)


def _write_bar_day(root: Path, day: str) -> None:
    target = root / day[:4] / day[4:6] / f"{day}_confirmed_trade_bars.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "day": [day, day],
            "symbol": ["AAA", "BBB"],
            "symbol_era_id": ["AAA#001", "BBB#001"],
            "open": [10.0, 20.0],
            "high": [11.0, 21.0],
            "low": [9.0, 19.0],
            "close": [10.5, 20.5],
            "volume": [1000, 2000],
            "trade_count": [100, 200],
            "notional": [10_000.0, 40_000.0],
            "vwap": [10.0, 20.0],
            "first_timestamp": [1, 3],
            "last_timestamp": [2, 4],
        }
    ).write_parquet(target)


def _write_quality_events(path: Path) -> None:
    pl.DataFrame(
        {
            "day": ["20250102", "20250102"],
            "symbol": ["AAA", "BBB"],
            "symbol_era_id": ["AAA#001", "BBB#001"],
            "liquidity_tier": ["core_liquid", "active"],
            "event_type": ["extreme_return", "volume_outlier"],
            "event_detail": ["x", "y"],
        }
    ).write_parquet(path)
