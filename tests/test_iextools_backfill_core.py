from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from utils.iextools_backfill_core import (
    HistFileRecord,
    choose_tops_record,
    publish_parquet_pair,
    select_missing_tops_days,
    tops_output_paths,
)


def test_tops_output_paths(tmp_path: Path) -> None:
    main_path, quote_path = tops_output_paths(tmp_path, "20250501")
    assert main_path == tmp_path / "2025" / "05" / "20250501_IEXTP1_TOPS1.6.parquet"
    assert quote_path.name == "20250501_IEXTP1_TOPS1.6_QuoteUpdate.parquet"


def test_select_missing_tops_days_skips_existing_pair(tmp_path: Path) -> None:
    main_path, quote_path = tops_output_paths(tmp_path, "20250501")
    main_path.parent.mkdir(parents=True)
    main_path.write_bytes(b"x")
    quote_path.write_bytes(b"y")
    records = {
        "20250501": [HistFileRecord("20250501", "TOPS", "IEXTP1", "1.6", 1, "u1")],
        "20250502": [HistFileRecord("20250502", "TOPS", "IEXTP1", "1.6", 1, "u2")],
    }
    assert select_missing_tops_days(
        records, tmp_path, start_day="20250501", end_day=None, limit_days=None
    ) == ["20250502"]


def test_publish_parquet_pair_copies_and_verifies(tmp_path: Path) -> None:
    local_main = tmp_path / "local_main.parquet"
    local_quote = tmp_path / "local_quote.parquet"
    pq.write_table(pa.table({"x": [1, 2]}), local_main)
    pq.write_table(pa.table({"y": [3]}), local_quote)
    publish = publish_parquet_pair(
        local_main, local_quote, tmp_path / "nas", "20250501", publish_token="t"
    )
    assert Path(publish["main_path"]).exists()
    assert Path(publish["quote_path"]).exists()
    assert publish["main_rows"] == 2
    assert publish["quote_rows"] == 1


def test_choose_tops_record() -> None:
    record = choose_tops_record(
        {
            "20250501": [
                HistFileRecord("20250501", "DEEP", "IEXTP1", "1.0", 1, "a"),
                HistFileRecord("20250501", "TOPS", "IEXTP1", "1.6", 2, "b"),
            ]
        },
        "20250501",
    )
    assert record.feed == "TOPS"
    assert record.link == "b"


def test_choose_tops_record_prefers_full_size_when_hist_has_placeholder() -> None:
    record = choose_tops_record(
        {
            "20171101": [
                HistFileRecord("20171101", "TOPS", "IEXTP1", "1.5", 247, "placeholder"),
                HistFileRecord("20171101", "TOPS", "IEXTP1", "1.6", 619_412_948, "full"),
            ]
        },
        "20171101",
    )

    assert record.version == "1.6"
    assert record.link == "full"
