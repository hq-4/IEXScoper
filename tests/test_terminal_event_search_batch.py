from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utils.build_terminal_event_search_batch import (
    TerminalEventSearchBatchConfig,
    build_terminal_event_search_batch,
)


def test_build_terminal_event_search_batch_rewrites_dates_and_preserves_originals(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "out.csv"
    summary_path = tmp_path / "summary.json"
    _write_input(input_path)

    result = build_terminal_event_search_batch(
        TerminalEventSearchBatchConfig(
            input_path=input_path,
            output_path=output_path,
            summary_path=summary_path,
            symbols=("BIG",),
            limit=None,
            lookback_days=10,
            lookahead_days=5,
        )
    )

    rows = pl.read_csv(output_path, infer_schema_length=0).to_dicts()
    assert result["summary"]["row_count"] == 1
    assert rows[0]["symbol"] == "BIG"
    assert rows[0]["original_first_day"] == "20170101"
    assert rows[0]["original_last_day"] == "20240909"
    assert rows[0]["first_day"] == "20240830"
    assert rows[0]["last_day"] == "20240914"
    assert rows[0]["search_window_reason"] == "terminal_window_10d_before_5d_after"


def test_build_terminal_event_search_batch_rejects_negative_windows(tmp_path: Path) -> None:
    input_path = tmp_path / "input.csv"
    _write_input(input_path)

    with pytest.raises(ValueError, match="lookback"):
        build_terminal_event_search_batch(
            TerminalEventSearchBatchConfig(
                input_path=input_path,
                output_path=tmp_path / "out.csv",
                summary_path=tmp_path / "summary.json",
                symbols=(),
                limit=None,
                lookback_days=-1,
                lookahead_days=0,
            )
        )


def _write_input(path: Path) -> None:
    pl.DataFrame(
        {
            "priority_rank": ["1", "2"],
            "symbol": ["BIG", "HOME"],
            "symbol_era_id": ["BIG#001", "HOME#001"],
            "first_day": ["20170101", "20170101"],
            "last_day": ["20240909", "20210723"],
        }
    ).write_csv(path)
