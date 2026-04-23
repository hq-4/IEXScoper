from __future__ import annotations

import csv
from datetime import UTC, datetime

from src.adapters.io.csv_trades_reader import resolve_trade_csv_path, scan_trades_csv_for_day


def _ns(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000_000)


def test_resolve_tops_trade_csv_path(tmp_path):
    day = "20250102"
    target = tmp_path / "2025" / "01" / f"{day}_IEXTP1_TOPS1.6_trd.csv"
    target.parent.mkdir(parents=True)
    target.write_text("Exchange Timestamp,Symbol,Size,Price,Trade ID,Sale Condition\n")

    assert resolve_trade_csv_path(str(tmp_path), day) == target


def test_scan_tops_trades_filters_dedupes_and_aggregates(tmp_path):
    day = "20250102"
    target = tmp_path / "2025" / "01" / f"{day}_IEXTP1_TOPS1.6_trd.csv"
    target.parent.mkdir(parents=True)
    rows = [
        {
            "Exchange Timestamp": _ns(datetime(2025, 1, 2, 14, 30, 0, tzinfo=UTC)),
            "Symbol": "aapl",
            "Size": 100,
            "Price": 10.0,
            "Trade ID": "1",
            "Sale Condition": "REGULAR_HOURS|ODD_LOT",
        },
        {
            "Exchange Timestamp": _ns(datetime(2025, 1, 2, 14, 30, 0, tzinfo=UTC)),
            "Symbol": "AAPL",
            "Size": 100,
            "Price": 10.0,
            "Trade ID": "1",
            "Sale Condition": "REGULAR_HOURS|ODD_LOT",
        },
        {
            "Exchange Timestamp": _ns(datetime(2025, 1, 2, 14, 30, 0, tzinfo=UTC)),
            "Symbol": "AAPL",
            "Size": 200,
            "Price": 11.0,
            "Trade ID": "2",
            "Sale Condition": "REGULAR_HOURS",
        },
        {
            "Exchange Timestamp": _ns(datetime(2025, 1, 2, 15, 0, 0, tzinfo=UTC)),
            "Symbol": "AAPL",
            "Size": 999,
            "Price": 99.0,
            "Trade ID": "3",
            "Sale Condition": "CANCEL",
        },
    ]
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    df = scan_trades_csv_for_day(
        csv_root=str(tmp_path),
        yyyymmdd=day,
        symbols=None,
        display_tz="America/New_York",
    )

    assert df is not None
    assert df.height == 1
    row = df.to_dicts()[0]
    assert row["symbol"] == "AAPL"
    assert row["session"] == "regular"
    assert row["share_volume"] == 300
    assert row["trade_count"] == 2
    assert row["vwap"] == (100 * 10.0 + 200 * 11.0) / 300


def test_scan_tops_trades_accepts_parser_raw_timestamp_header(tmp_path):
    day = "20250102"
    target = tmp_path / "2025" / "01" / f"{day}_IEXTP1_TOPS1.6_trd.csv"
    target.parent.mkdir(parents=True)
    rows = [
        {
            "Packet Capture Time": "1735828200000000100",
            "Send Time": "1735828200000000050",
            "Raw Timestamp": _ns(datetime(2025, 1, 2, 14, 30, 0, tzinfo=UTC)),
            "Tick Type": "T",
            "Symbol": "MSFT",
            "Size": 50,
            "Price": 20.0,
            "Trade ID": "10",
            "Sale Condition": "REGULAR_HOURS",
        }
    ]
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    df = scan_trades_csv_for_day(
        csv_root=str(tmp_path),
        yyyymmdd=day,
        symbols=["MSFT"],
        display_tz="America/New_York",
    )

    assert df is not None
    assert df.to_dicts()[0]["share_volume"] == 50
