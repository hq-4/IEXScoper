from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_daily_trade_bars import (
    TradeBarConfig,
    build_daily_trade_bars,
    build_day_bars,
    trade_bar_output_path,
)


def test_build_day_bars_filters_confirmed_trades_and_assigns_era(tmp_path: Path) -> None:
    main_path = tmp_path / "20250102_IEXTP1_TOPS1.6.parquet"
    _write_main(
        main_path,
        {
            "type": ["TradeReport", "QuoteUpdate", "TradeReport", "TradeReport"],
            "timestamp": [3, 2, 1, 4],
            "symbol": ["AAA", "AAA", "AAA", "BBB"],
            "size": [10, 99, 20, 5],
            "price": [11.0, 12.0, 10.0, None],
            "price_int": [110000, 120000, 100000, 250000],
        },
    )
    eras = pl.DataFrame(
        {
            "symbol": ["AAA", "BBB"],
            "symbol_era_id": ["AAA#001", "BBB#001"],
            "first_day": ["20250102", "20250102"],
            "last_day": ["20250102", "20250102"],
        }
    )

    bars, trade_rows, unmatched = build_day_bars(main_path, "20250102", eras)

    rows = {row["symbol_era_id"]: row for row in bars.to_dicts()}
    assert trade_rows == 3
    assert unmatched == 0
    assert rows["AAA#001"]["open"] == 10.0
    assert rows["AAA#001"]["high"] == 11.0
    assert rows["AAA#001"]["low"] == 10.0
    assert rows["AAA#001"]["close"] == 11.0
    assert rows["AAA#001"]["volume"] == 30
    assert rows["AAA#001"]["trade_count"] == 2
    assert rows["BBB#001"]["open"] == 25.0


def test_build_day_bars_reports_unmatched_trade_rows(tmp_path: Path) -> None:
    main_path = tmp_path / "20250102_IEXTP1_TOPS1.6.parquet"
    _write_main(
        main_path,
        {
            "type": ["TradeReport", "TradeReport"],
            "timestamp": [1, 2],
            "symbol": ["AAA", "MISSING"],
            "size": [10, 10],
            "price": [10.0, 20.0],
        },
    )
    eras = pl.DataFrame(
        {
            "symbol": ["AAA"],
            "symbol_era_id": ["AAA#001"],
            "first_day": ["20250102"],
            "last_day": ["20250102"],
        }
    )

    bars, trade_rows, unmatched = build_day_bars(main_path, "20250102", eras)

    assert trade_rows == 2
    assert unmatched == 1
    assert bars["symbol_era_id"].to_list() == ["AAA#001"]


def test_build_daily_trade_bars_is_resumable_and_splits_ticker_eras(tmp_path: Path) -> None:
    parquet_root = tmp_path / "pq"
    output_root = tmp_path / "bars"
    eras_path = tmp_path / "symbol_eras.parquet"
    _write_tops_day(parquet_root, "20250102", ["SNOW"], [10.0])
    _write_tops_day(parquet_root, "20250120", ["SNOW"], [20.0])
    pl.DataFrame(
        {
            "symbol": ["SNOW", "SNOW"],
            "symbol_era_id": ["SNOW#001", "SNOW#002"],
            "first_day": ["20250102", "20250120"],
            "last_day": ["20250103", "20250120"],
        }
    ).write_parquet(eras_path)

    first = build_daily_trade_bars(
        TradeBarConfig(
            parquet_root=parquet_root,
            output_root=output_root,
            symbol_eras_path=eras_path,
            start_day="20250102",
            end_day="20250120",
            compression="zstd",
            limit_days=None,
            replace=False,
        )
    )
    second = build_daily_trade_bars(
        TradeBarConfig(
            parquet_root=parquet_root,
            output_root=output_root,
            symbol_eras_path=eras_path,
            start_day="20250102",
            end_day="20250120",
            compression="zstd",
            limit_days=None,
            replace=False,
        )
    )

    early = pl.read_parquet(trade_bar_output_path(output_root, "20250102")).to_dicts()[0]
    late = pl.read_parquet(trade_bar_output_path(output_root, "20250120")).to_dicts()[0]
    assert first["summary"]["processed_day_count"] == 2
    assert second["summary"]["skipped_existing_day_count"] == 2
    assert early["symbol_era_id"] == "SNOW#001"
    assert late["symbol_era_id"] == "SNOW#002"
    assert (output_root / "daily_trade_bars_summary.json").exists()
    assert (output_root / "daily_trade_bars_results.jsonl").exists()


def _write_tops_day(
    parquet_root: Path, day: str, symbols: list[str], prices: list[float]
) -> None:
    target_dir = parquet_root / day[:4] / day[4:6]
    target_dir.mkdir(parents=True, exist_ok=True)
    main_path = target_dir / f"{day}_IEXTP1_TOPS1.6.parquet"
    quote_path = target_dir / f"{day}_IEXTP1_TOPS1.6_QuoteUpdate.parquet"
    _write_main(
        main_path,
        {
            "type": ["TradeReport"] * len(symbols),
            "timestamp": list(range(len(symbols))),
            "symbol": symbols,
            "size": [100] * len(symbols),
            "price": prices,
        },
    )
    pl.DataFrame({"symbol": symbols}).write_parquet(quote_path)


def _write_main(path: Path, data: dict[str, list[object]]) -> None:
    pl.DataFrame(data).write_parquet(path)
