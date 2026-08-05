from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from utils.build_openfigi_stable_universe import build_stable_universe_input


def test_filters_to_stable_and_ipo_classes_only(tmp_path: Path) -> None:
    symbol_eras_path = tmp_path / "symbol_eras.parquet"
    output_path = tmp_path / "out" / "input.jsonl"
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC", "DDD"],
            "symbol_era_id": ["AAA#001", "BBB#001", "CCC#001", "DDD#001"],
            "first_day": ["20170101", "20180101", "20190101", "20200101"],
            "last_day": ["20170601", "20260622", "20190601", "20260622"],
            "source_classification": [
                "delisted_or_acquired_candidate",
                "stable_candidate",
                "intermittent_or_reused_candidate",
                "ipo_or_new_listing_candidate",
            ],
        }
    ).write_parquet(symbol_eras_path)

    result = build_stable_universe_input(symbol_eras_path, output_path)

    assert result["eras"] == 2
    assert result["unique_symbols"] == 2
    lines = [json.loads(line) for line in output_path.read_text().splitlines()]
    symbols = {row["symbol"] for row in lines}
    assert symbols == {"BBB", "DDD"}
    assert set(lines[0].keys()) == {"symbol", "symbol_era_id", "first_day", "last_day"}


def test_custom_classes_argument(tmp_path: Path) -> None:
    symbol_eras_path = tmp_path / "symbol_eras.parquet"
    output_path = tmp_path / "out" / "input.jsonl"
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB"],
            "symbol_era_id": ["AAA#001", "BBB#001"],
            "first_day": ["20170101", "20180101"],
            "last_day": ["20170601", "20260622"],
            "source_classification": ["delisted_or_acquired_candidate", "stable_candidate"],
        }
    ).write_parquet(symbol_eras_path)

    result = build_stable_universe_input(
        symbol_eras_path, output_path, classes=("delisted_or_acquired_candidate",)
    )

    assert result["eras"] == 1
    lines = [json.loads(line) for line in output_path.read_text().splitlines()]
    assert lines[0]["symbol"] == "AAA"


def test_raises_on_missing_input(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        build_stable_universe_input(tmp_path / "missing.parquet", tmp_path / "out.jsonl")
