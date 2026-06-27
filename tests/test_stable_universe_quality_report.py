from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_stable_universe_quality_report import (
    QualityConfig,
    build_quality_report,
)


def test_build_quality_report_flags_daily_bar_issues(tmp_path: Path) -> None:
    universe_path = tmp_path / "stable.parquet"
    bars_root = tmp_path / "bars"
    output_root = tmp_path / "quality"
    _write_universe(universe_path)
    _write_day(bars_root, "20250102", close=10.0, low=9.0, high=11.0, notional=1_000.0)
    _write_day(bars_root, "20250103", close=20.0, low=21.0, high=19.0, notional=50_000.0)

    result = build_quality_report(
        QualityConfig(
            universe_path=universe_path,
            daily_bars_root=bars_root,
            output_root=output_root,
            extreme_return_threshold=0.50,
            near_zero_price_threshold=0.01,
            outlier_multiple=1.5,
            min_outlier_notional=1_000.0,
        )
    )

    events = pl.read_parquet(output_root / "quality_events.parquet")
    by_symbol = pl.read_parquet(output_root / "quality_by_symbol.parquet")
    event_types = set(events["event_type"].to_list())
    assert result["summary"]["stable_era_count"] == 1
    assert result["summary"]["symbols_with_quality_issues"] == 1
    assert result["summary"]["symbols_with_daily_events"] == 1
    assert "NEW#001" not in set(events["symbol_era_id"].to_list())
    assert {"extreme_return", "invalid_ohlc", "notional_outlier"}.issubset(event_types)
    assert by_symbol["quality_issue_days"].item() >= 3
    assert (output_root / "quality_report.md").exists()


def _write_universe(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["AAA"],
            "symbol_era_id": ["AAA#001"],
            "liquidity_tier": ["core_liquid"],
            "expected_days_in_era": [2],
        }
    ).write_parquet(path)


def _write_day(
    root: Path, day: str, *, close: float, low: float, high: float, notional: float
) -> None:
    target = root / day[:4] / day[4:6] / f"{day}_confirmed_trade_bars.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "day": [day, day],
            "symbol": ["AAA", "NEW"],
            "symbol_era_id": ["AAA#001", "NEW#001"],
            "open": [10.0, 100.0],
            "high": [high, 101.0],
            "low": [low, 99.0],
            "close": [close, 100.5],
            "volume": [100, 100],
            "trade_count": [10, 10],
            "notional": [notional, 10_000.0],
            "vwap": [10.0, 100.0],
            "first_timestamp": [1, 1],
            "last_timestamp": [2, 2],
        }
    ).write_parquet(target)
