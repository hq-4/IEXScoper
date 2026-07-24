from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

from src.framework.config import Settings
from src.usecases.aggregate_per_second import run_aggregate_per_second

HEADER = ["Exchange Timestamp", "Symbol", "Size", "Price", "Trade ID", "Sale Condition"]


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        iex_csv_root=str(tmp_path / "csv"),
        iex_parquet_root=str(tmp_path / "pq"),
        iex_work_root=str(tmp_path / "work"),
        iex_report_root=str(tmp_path / "reports"),
        display_tz="America/New_York",
        log_jsonl_path=str(tmp_path / "logs" / "app.jsonl"),
        database_url=None,
    )


def _write_day_csv(csv_root: Path, day: str, trade_id: str = "1") -> None:
    target = csv_root / day[:4] / day[4:6] / f"{day}_IEXTP1_TOPS1.6_trd.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    ts = int(datetime(2025, 1, 2, 15, 0, 0, tzinfo=UTC).timestamp() * 1_000_000_000)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        writer.writerow([ts, "AAPL", 100, 10.0, trade_id, "REGULAR_HOURS"])


def test_limit_days_skips_non_trading_days(tmp_path: Path) -> None:
    # Jan 1 2025 is a market holiday with no CSV; Jan 2 has one. A 1-day limit must
    # still process the real trading day instead of burning the attempt on the holiday.
    csv_root = tmp_path / "csv"
    _write_day_csv(csv_root, "20250102")

    code = run_aggregate_per_second(
        year=2025,
        symbols=None,
        rebuild=False,
        dry_run=True,
        limit_days=1,
        settings=_settings(tmp_path),
    )

    assert code == 0


def test_zero_processed_days_returns_nonzero(tmp_path: Path) -> None:
    code = run_aggregate_per_second(
        year=2025,
        symbols=None,
        rebuild=False,
        dry_run=True,
        limit_days=3,
        settings=_settings(tmp_path),
    )

    assert code == 1


def test_corrupt_day_does_not_abort_run(tmp_path: Path) -> None:
    csv_root = tmp_path / "csv"
    _write_day_csv(csv_root, "20250102")
    # Corrupt CSV on Jan 3 (missing required columns), valid day on Jan 6.
    bad = csv_root / "2025" / "01" / "20250103_IEXTP1_TOPS1.6_trd.csv"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text("not,a,valid,header\n1,2,3,4\n")
    _write_day_csv(csv_root, "20250106", trade_id="9")

    code = run_aggregate_per_second(
        year=2025,
        symbols=None,
        rebuild=False,
        dry_run=True,
        limit_days=5,
        settings=_settings(tmp_path),
    )

    # both valid days processed despite the corrupt one; non-zero exit flags the failure
    assert code == 1
