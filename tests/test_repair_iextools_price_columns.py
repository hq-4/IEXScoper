from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from utils.iextools_backfill_core import tops_output_paths
from utils.iextools_price_repair import (
    MAIN_REPAIRS,
    QUOTE_REPAIRS,
    audit_or_repair_file,
    repair_day,
    select_days,
)


def test_select_days_uses_existing_parquet_pairs(tmp_path: Path) -> None:
    for day in ("20170103", "20170104"):
        main, quote = tops_output_paths(tmp_path, day)
        main.parent.mkdir(parents=True, exist_ok=True)
        main.write_bytes(b"x")
        quote.write_bytes(b"y")

    assert select_days(
        tmp_path,
        start_day="20170104",
        end_day=None,
        days_arg=None,
        limit_days=None,
    ) == ["20170104"]


def test_audit_detects_fillable_main_price_nulls(tmp_path: Path) -> None:
    path = tmp_path / "main.parquet"
    _write_main(path, price=[None, 12.34, None], price_int=[10000, 123400, None])

    result = audit_or_repair_file(path, repairs=MAIN_REPAIRS, apply=False)

    assert result.needs_repair
    assert not result.repaired
    assert result.columns[0].float_non_null == 1
    assert result.columns[0].int_non_null == 2
    assert result.columns[0].fillable_nulls == 1


def test_repair_fills_prices_without_schema_or_row_group_drift(tmp_path: Path) -> None:
    path = tmp_path / "main.parquet"
    _write_main(path, price=[None, 12.34], price_int=[10000, 123400])
    before = pq.ParquetFile(path)

    result = audit_or_repair_file(path, repairs=MAIN_REPAIRS, apply=True)
    after = pq.ParquetFile(path)
    table = pq.read_table(path)

    assert result.repaired
    assert after.schema_arrow == before.schema_arrow
    assert after.metadata.num_rows == before.metadata.num_rows
    assert after.metadata.num_row_groups == before.metadata.num_row_groups
    assert after.metadata.row_group(0).column(0).compression == "SNAPPY"
    assert table["price"].to_pylist() == [1.0, 12.34]


def test_repair_day_fills_quote_bid_and_ask_prices(tmp_path: Path) -> None:
    main, quote = tops_output_paths(tmp_path, "20170103")
    main.parent.mkdir(parents=True)
    _write_main(main, price=[None], price_int=[250000])
    _write_quote(
        quote,
        bid_price=[None, 1.25],
        bid_price_int=[10000, 12500],
        ask_price=[None, None],
        ask_price_int=[20000, None],
    )

    payload = repair_day(parquet_root=tmp_path, day="20170103", apply=True)

    assert payload["status"] == "repaired"
    assert pq.read_table(main, columns=["price"])["price"].to_pylist() == [25.0]
    quote_table = pq.read_table(quote, columns=["bid_price", "ask_price"])
    assert quote_table["bid_price"].to_pylist() == [1.0, 1.25]
    assert quote_table["ask_price"].to_pylist() == [2.0, None]


def test_repaired_file_has_no_remaining_fillable_quote_nulls(tmp_path: Path) -> None:
    path = tmp_path / "quote.parquet"
    _write_quote(
        path,
        bid_price=[None],
        bid_price_int=[10000],
        ask_price=[None],
        ask_price_int=[20000],
    )

    audit_or_repair_file(path, repairs=QUOTE_REPAIRS, apply=True)
    result = audit_or_repair_file(path, repairs=QUOTE_REPAIRS, apply=False)

    assert not result.needs_repair
    assert pc.count(pq.read_table(path)["bid_price"]).as_py() == 1
    assert pc.count(pq.read_table(path)["ask_price"]).as_py() == 1


def _write_main(path: Path, *, price: list[float | None], price_int: list[int | None]) -> None:
    table = pa.table(
        {
            "type": pa.array(["TradeReport"] * len(price), pa.string()),
            "price": pa.array(price, pa.float64()),
            "price_int": pa.array(price_int, pa.int64()),
        }
    )
    pq.write_table(table, path, compression="snappy", row_group_size=1)


def _write_quote(
    path: Path,
    *,
    bid_price: list[float | None],
    bid_price_int: list[int | None],
    ask_price: list[float | None],
    ask_price_int: list[int | None],
) -> None:
    table = pa.table(
        {
            "type": pa.array(["QuoteUpdate"] * len(bid_price), pa.string()),
            "bid_price": pa.array(bid_price, pa.float64()),
            "bid_price_int": pa.array(bid_price_int, pa.int64()),
            "ask_price": pa.array(ask_price, pa.float64()),
            "ask_price_int": pa.array(ask_price_int, pa.int64()),
        }
    )
    pq.write_table(table, path, compression="snappy", row_group_size=1)
