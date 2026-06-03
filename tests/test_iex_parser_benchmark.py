from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from utils.benchmark_iex_parsers import parse_time_output, stage_archive_days, wall_to_seconds
from utils.iex_benchmark_adapters import normalize_hq4_message, normalize_rob_message
from utils.iex_benchmark_core import (
    QUOTE_COLUMNS,
    REFERENCE_MAIN_PATH,
    REFERENCE_QUOTE_PATH,
    benchmark_output_paths,
    load_reference_schemas,
    parquet_write_options,
    resolve_archive_day,
)
from utils.iex_parser_repo_runner import _should_log_progress
from utils.iex_benchmark_reporting import render_markdown_report


def test_resolve_archive_day(tmp_path: Path) -> None:
    path = tmp_path / "20240105_IEXTP1_TOPS1.6.pcap.gz"
    path.write_bytes(b"x")
    assert resolve_archive_day(tmp_path, "20240105") == path


def test_load_reference_schemas() -> None:
    main_schema, quote_schema = load_reference_schemas(REFERENCE_MAIN_PATH, REFERENCE_QUOTE_PATH)
    assert len(main_schema) == 22
    assert len(quote_schema) == 10


def test_normalize_rob_quote_routes_to_quote_file() -> None:
    target, row = normalize_rob_message(
        {
            "type": "quote_update",
            "timestamp": datetime(2024, 1, 5, tzinfo=timezone.utc),
            "symbol": b"AAPL",
            "flags": 64,
            "bid_size": 10,
            "bid_price": 100.25,
            "ask_price": 100.30,
            "ask_size": 12,
        }
    )
    assert target == "quote"
    assert set(row) == set(QUOTE_COLUMNS)
    assert row["bid_price_int"] == 1002500
    assert row["type"] == "QuoteUpdate"


def test_normalize_rob_trade_preserves_null_shape() -> None:
    target, row = normalize_rob_message(
        {
            "type": "trade_report",
            "timestamp": datetime(2024, 1, 5, tzinfo=timezone.utc),
            "symbol": b"MSFT",
            "flags": 0x60,
            "size": 50,
            "price": 123.45,
            "trade_id": 77,
        }
    )
    assert target == "main"
    assert row["price_int"] == 1234500
    assert row["sale_flags"] == "extended_hours|odd_lot"
    assert row["reason"] is None


@dataclass
class QuoteUpdate:
    flags: int
    timestamp: int
    symbol: str
    bid_size: int
    bid_price_int: int
    ask_price_int: int
    ask_size: int
    bid_price: float
    ask_price: float


def test_normalize_hq4_quote_routes_to_quote_file() -> None:
    target, row = normalize_hq4_message(
        QuoteUpdate(
            flags=1,
            timestamp=1704457239512544830,
            symbol="AAPL",
            bid_size=10,
            bid_price_int=1002500,
            ask_price_int=1003000,
            ask_size=11,
            bid_price=100.25,
            ask_price=100.30,
        )
    )
    assert target == "quote"
    assert row["ask_price_int"] == 1003000


def test_benchmark_output_paths_are_flat(tmp_path: Path) -> None:
    main_output, quote_output = benchmark_output_paths(tmp_path, "20240105", "hq-4", "snappy")
    assert main_output.parent == tmp_path
    assert quote_output.name == "20240105_hq-4_IEXTools_TOPS1.6_QuoteUpdate.parquet"


def test_requested_compression_writes_expected_codec(tmp_path: Path) -> None:
    _, quote_schema = load_reference_schemas(REFERENCE_MAIN_PATH, REFERENCE_QUOTE_PATH)
    target = tmp_path / "quote.parquet"
    table = pa.Table.from_pylist(
        [
            {
                "type": "QuoteUpdate",
                "timestamp": 1,
                "symbol": "AAPL",
                "flags": 1,
                "bid_size": 1,
                "bid_price_int": 10000,
                "ask_price_int": 10001,
                "ask_size": 1,
                "bid_price": 1.0,
                "ask_price": 1.0001,
            }
        ],
        schema=quote_schema,
    )
    opts = parquet_write_options("zstd3")
    pq.write_table(
        table, target, compression=opts["compression"], compression_level=opts["compression_level"]
    )
    pf = pq.ParquetFile(target)
    assert pf.metadata.row_group(0).column(0).compression == "ZSTD"


def test_report_generator_renders_summary() -> None:
    report = render_markdown_report(
        {
            "benchmark_mode": "parity",
            "days": ["20240105"],
            "archive_root": "/media/tn/iex",
            "output_root": "/media/tn/pq/tmp",
            "results_path": "utils/benchmark_results/iex_parser_benchmark_results.json",
            "environment": {"platform": "linux"},
            "reference": {
                "main": {"path": "main.parquet", "columns": [1] * 22, "compression": ["SNAPPY"]},
                "quote": {"path": "quote.parquet", "columns": [1] * 10, "compression": ["SNAPPY"]},
            },
            "repos": [{"repo_key": "hq-4", "commit": "abc", "path": "/tmp/repo"}],
            "runs": [
                {
                    "repo_key": "hq-4",
                    "day": "20240105",
                    "status": "succeeded",
                    "resources": {},
                    "preprocessing": {"input_mode": "pcap.gz_direct"},
                }
            ],
            "compression_sweeps": [],
        }
    )
    assert "# IEX Parser Benchmark Report" in report
    assert "| Repo | Day | Status |" in report


def test_time_output_parser() -> None:
    parsed = parse_time_output(
        "\n".join(
            [
                "User time (seconds): 12.34",
                "System time (seconds): 1.25",
                "Elapsed (wall clock) time (h:mm:ss or m:ss): 1:02.50",
                "Maximum resident set size (kbytes): 123456",
            ]
        )
    )
    assert parsed["elapsed_wall_seconds"] == 62.5
    assert parsed["max_rss_kb"] == 123456
    assert wall_to_seconds("01:02:03.5") == 3723.5


def test_progress_logging_thresholds() -> None:
    assert _should_log_progress(1_000_000, 0, 1.0, 1_000_000, 30.0)
    assert _should_log_progress(10, 0, 30.0, 1_000_000, 30.0)
    assert not _should_log_progress(10, 0, 5.0, 1_000_000, 30.0)


def test_stage_archive_days_copies_to_local_cache(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    cache_root = tmp_path / "cache"
    archive_root.mkdir()
    source = archive_root / "20240105_IEXTP1_TOPS1.6.pcap.gz"
    source.write_bytes(b"abc")

    effective_root = stage_archive_days(archive_root, ["20240105"], cache_root)

    assert effective_root == cache_root
    assert (cache_root / source.name).read_bytes() == b"abc"
