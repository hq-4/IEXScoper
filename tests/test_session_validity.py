from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_symbol_stability_audit import AuditConfig, build_symbol_stability_audit
from utils.session_validity import (
    REASON_LOW_SHARE,
    REASON_NO_TRADES,
    REASON_WEEKEND,
    build_validity_manifest,
    classify_session,
    is_weekend,
    load_quarantined_days,
    summarize_session,
    write_validity_manifest,
)


def test_is_weekend_flags_saturday_sessions() -> None:
    assert is_weekend("20170826")  # Saturday test session
    assert not is_weekend("20170825")  # Friday trading day


def test_classify_session_reasons() -> None:
    weekend = classify_session("20170826", total_rows=31_497, trade_rows=165)
    assert weekend["valid"] is False and weekend["reason"] == REASON_WEEKEND

    no_trades = classify_session("20250106", total_rows=9_000, trade_rows=0)
    assert no_trades["valid"] is False and no_trades["reason"] == REASON_NO_TRADES

    low_share = classify_session("20250106", total_rows=10_000, trade_rows=100)
    assert low_share["valid"] is False and low_share["reason"] == REASON_LOW_SHARE

    normal = classify_session("20250106", total_rows=657_759, trade_rows=657_000)
    assert normal["valid"] is True and normal["reason"] is None


def test_summarize_session_counts_message_types(tmp_path: Path) -> None:
    path = tmp_path / "day.parquet"
    pl.DataFrame(
        {"type": ["TradeReport"] * 8 + ["OperationalHalt"] * 2, "symbol": ["AAA"] * 10}
    ).write_parquet(path)
    stats = summarize_session(path)
    assert stats["total_rows"] == 10
    assert stats["trade_rows"] == 8
    assert stats["type_counts"]["OperationalHalt"] == 2


def test_manifest_round_trip_and_quarantine_load(tmp_path: Path) -> None:
    parquet_root = tmp_path / "pq"
    _write_day(parquet_root, "20250103", ["TradeReport"] * 5)  # Friday
    _write_day(parquet_root, "20250104", ["TradeReport"] * 5)  # Saturday

    manifest = build_validity_manifest(parquet_root, ["20250103", "20250104"])
    assert manifest["scanned_day_count"] == 2
    assert manifest["quarantined_day_count"] == 1
    assert manifest["quarantined_days"][0]["day"] == "20250104"

    path = tmp_path / "session_validity.json"
    write_validity_manifest(path, manifest)
    assert load_quarantined_days(path) == {"20250104"}
    assert load_quarantined_days(tmp_path / "missing.json") == set()


def test_audit_excludes_quarantined_days_from_eras(tmp_path: Path) -> None:
    parquet_root = tmp_path / "pq"
    _write_day(parquet_root, "20250102", ["REAL", "REAL"])  # Thursday
    _write_day(parquet_root, "20250104", ["REAL", "GHOST"])  # Saturday test session
    _write_day(parquet_root, "20250106", ["REAL"])  # Monday

    manifest = {
        "quarantined_days": [
            {
                "day": "20250104",
                "valid": False,
                "reason": "weekend_session",
                "total_rows": 2,
                "trade_rows": 2,
                "trade_share": 1.0,
            }
        ]
    }
    quarantine_path = tmp_path / "session_validity.json"
    write_validity_manifest(quarantine_path, manifest)

    result = _run_audit(parquet_root, tmp_path / "report", quarantine_path)
    eras = {(row["symbol"], row["symbol_era_id"]) for row in result["era_rows"]}
    assert eras == {("REAL", "REAL#001")}  # GHOST weekend-only era is gone
    assert result["summary"]["quarantined_day_count"] == 1

    baseline = _run_audit(parquet_root, tmp_path / "baseline", None)
    baseline_eras = {(row["symbol"], row["symbol_era_id"]) for row in baseline["era_rows"]}
    assert ("GHOST", "GHOST#001") in baseline_eras  # without quarantine the junk era exists


def _run_audit(parquet_root: Path, output_root: Path, quarantine_path: Path | None):
    return build_symbol_stability_audit(
        AuditConfig(
            parquet_root=parquet_root,
            output_root=output_root,
            start_day="20250102",
            end_day="20250106",
            min_coverage=0.5,
            major_gap_days=14,
            limit_days=None,
            quarantine_path=quarantine_path,
        )
    )


def _write_day(parquet_root: Path, day: str, symbols: list[str]) -> None:
    target_dir = parquet_root / day[:4] / day[4:6]
    target_dir.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "type": ["TradeReport"] * len(symbols),
            "timestamp": list(range(len(symbols))),
            "symbol": symbols,
        }
    )
    frame.write_parquet(target_dir / f"{day}_IEXTP1_TOPS1.6.parquet")
    pl.DataFrame({"symbol": symbols}).write_parquet(
        target_dir / f"{day}_IEXTP1_TOPS1.6_QuoteUpdate.parquet"
    )
