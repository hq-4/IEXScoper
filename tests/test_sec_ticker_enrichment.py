from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from utils.enrich_symbol_eras_sec import (
    SecTickerConfig,
    enrich_symbol_eras_sec,
    sec_lookup_frame,
    sec_ticker_frame,
    symbol_key,
)


def test_symbol_key_normalizes_common_share_class_spellings() -> None:
    assert symbol_key("BRK.B") == "BRKB"
    assert symbol_key("BRK-B") == "BRKB"
    assert symbol_key("abc/ws") == "ABCWS"


def test_sec_lookup_collapses_duplicate_normalized_tickers() -> None:
    rows = sec_ticker_frame(
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [1067983, "BERKSHIRE HATHAWAY INC", "BRK-B", "NYSE"],
                [1067983, "BERKSHIRE HATHAWAY INC", "BRK.B", "NYSE"],
            ],
        }
    )

    lookup = sec_lookup_frame(rows).to_dicts()

    assert lookup == [
        {
            "sec_symbol_key": "BRKB",
            "sec_match_count": 2,
            "sec_cik": "0001067983",
            "sec_name": "BERKSHIRE HATHAWAY INC",
            "sec_ticker": "BRK-B",
            "sec_exchange": "NYSE",
            "sec_ticker_variants": "BRK-B | BRK.B",
        }
    ]


def test_enrich_symbol_eras_sec_joins_and_writes_outputs(tmp_path: Path) -> None:
    eras_path = tmp_path / "symbol_eras.parquet"
    cache_path = tmp_path / "company_tickers_exchange.json"
    output_root = tmp_path / "out"
    _write_symbol_eras(eras_path)
    cache_path.write_text(json.dumps(_sec_payload()), encoding="utf-8")

    result = enrich_symbol_eras_sec(
        SecTickerConfig(
            symbol_eras_path=eras_path,
            output_root=output_root,
            cache_path=cache_path,
            user_agent="test",
            timeout_seconds=1,
            refresh=False,
        )
    )

    rows = {
        row["symbol_era_id"]: row
        for row in pl.read_parquet(output_root / "symbol_eras_sec_enriched.parquet").to_dicts()
    }
    assert rows["AAPL#001"]["sec_current_confidence"] == "sec_current_match"
    assert rows["AAPL#001"]["sec_cik"] == "0000320193"
    assert rows["BRK.B#001"]["sec_current_confidence"] == "sec_multiple_current_matches"
    assert rows["MISS#001"]["sec_current_confidence"] == "sec_unmatched"
    assert result["summary"]["confidence_counts"]["sec_current_match"] == 1
    assert (output_root / "sec_ticker_cik_report.md").exists()


def _write_symbol_eras(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["AAPL", "BRK.B", "MISS"],
            "symbol_era_id": ["AAPL#001", "BRK.B#001", "MISS#001"],
            "source_classification": [
                "stable_candidate",
                "intermittent_or_reused_candidate",
                "delisted_or_acquired_candidate",
            ],
            "first_day": ["20161212", "20161212", "20161212"],
            "last_day": ["20260622", "20200101", "20190101"],
        }
    ).write_parquet(path)


def _sec_payload() -> dict[str, object]:
    return {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [320193, "Apple Inc.", "AAPL", "Nasdaq"],
            [1067983, "BERKSHIRE HATHAWAY INC", "BRK-B", "NYSE"],
            [1067983, "BERKSHIRE HATHAWAY INC", "BRK.B", "NYSE"],
        ],
    }
