from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_stable_long_window_universe import (
    StableUniverseConfig,
    build_stable_long_window_universe,
)


def test_build_stable_long_window_universe_filters_and_tiers(tmp_path: Path) -> None:
    eras_path = tmp_path / "symbol_eras.parquet"
    bars_root = tmp_path / "bars"
    output_root = tmp_path / "out"
    _write_eras(eras_path)
    _write_bar_day(bars_root, "20250102")
    _write_bar_day(bars_root, "20250103")

    result = build_stable_long_window_universe(
        StableUniverseConfig(
            symbol_eras_path=eras_path,
            daily_bars_root=bars_root,
            output_root=output_root,
            min_trade_day_coverage=0.5,
            core_median_daily_notional=1_000_000,
            active_median_daily_notional=100_000,
        )
    )

    rows = {row["symbol_era_id"]: row for row in result["rows"]}
    assert set(rows) == {"AAA#001", "BBB#001", "CCC#001"}
    assert rows["AAA#001"]["liquidity_tier"] == "core_liquid"
    assert rows["BBB#001"]["liquidity_tier"] == "active"
    assert rows["CCC#001"]["liquidity_tier"] == "no_confirmed_trades"
    assert rows["AAA#001"]["traded_days"] == 2
    assert rows["AAA#001"]["trade_day_coverage_ratio"] == 1.0
    assert result["summary"]["stable_era_count"] == 3
    assert (output_root / "stable_long_window_universe.parquet").exists()
    assert (output_root / "stable_long_window_universe_report.md").exists()


def _write_eras(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC", "NEW"],
            "symbol_era_id": ["AAA#001", "BBB#001", "CCC#001", "NEW#001"],
            "first_day": ["20250102"] * 4,
            "last_day": ["20250103"] * 4,
            "expected_days_in_era": [2, 2, 2, 2],
            "coverage_ratio": [1.0, 1.0, 1.0, 1.0],
            "recommended_use": [
                "long_window_candidate",
                "long_window_candidate",
                "long_window_candidate",
                "point_in_time_era",
            ],
            "identity_status": ["ticker_continuity_candidate"] * 4,
        }
    ).write_parquet(path)


def _write_bar_day(root: Path, day: str) -> None:
    target = root / day[:4] / day[4:6] / f"{day}_confirmed_trade_bars.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "day": [day, day, day],
            "symbol": ["AAA", "BBB", "NEW"],
            "symbol_era_id": ["AAA#001", "BBB#001", "NEW#001"],
            "open": [10.0, 20.0, 30.0],
            "high": [11.0, 21.0, 31.0],
            "low": [9.0, 19.0, 29.0],
            "close": [10.5, 20.5, 30.5],
            "volume": [200_000, 10_000, 99_999],
            "trade_count": [1000, 100, 10],
            "notional": [2_000_000.0, 200_000.0, 3_000_000.0],
            "vwap": [10.0, 20.0, 30.0],
            "first_timestamp": [1, 1, 1],
            "last_timestamp": [2, 2, 2],
        }
    ).write_parquet(target)
