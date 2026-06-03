from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from utils.iex_benchmark_core import load_reference_schemas, parquet_write_options

MAIN_FLUSH_ROWS = 50_000
QUOTE_FLUSH_ROWS = 200_000


class StreamWriters:
    def __init__(
        self, main_path: Path, quote_path: Path, compression: str, logger: logging.Logger
    ) -> None:
        self.main_schema, self.quote_schema = load_reference_schemas()
        self.main_path = main_path
        self.quote_path = quote_path
        self.write_options = parquet_write_options(compression)
        self.logger = logger
        self.main_rows: list[dict[str, Any]] = []
        self.quote_rows: list[dict[str, Any]] = []
        self.main_writer: pq.ParquetWriter | None = None
        self.quote_writer: pq.ParquetWriter | None = None
        self.main_count = 0
        self.quote_count = 0
        self.write_seconds = 0.0

    def add(self, target: str, row: dict[str, Any]) -> None:
        buffer_rows = self.quote_rows if target == "quote" else self.main_rows
        buffer_rows.append(row)
        limit = QUOTE_FLUSH_ROWS if target == "quote" else MAIN_FLUSH_ROWS
        if len(buffer_rows) >= limit:
            self.flush(target)

    def flush(self, target: str) -> None:
        rows = self.quote_rows if target == "quote" else self.main_rows
        if not rows:
            return
        schema = self.quote_schema if target == "quote" else self.main_schema
        path = self.quote_path if target == "quote" else self.main_path
        writer = self.quote_writer if target == "quote" else self.main_writer
        start = time.perf_counter()
        table = pa.Table.from_pylist(rows, schema=schema)
        if writer is None:
            writer = pq.ParquetWriter(
                path,
                schema,
                compression=self.write_options["compression"],
                compression_level=self.write_options["compression_level"],
            )
            if target == "quote":
                self.quote_writer = writer
            else:
                self.main_writer = writer
        writer.write_table(table)
        self.write_seconds += time.perf_counter() - start
        row_count = len(rows)
        if target == "quote":
            self.quote_count += row_count
            self.quote_rows = []
        else:
            self.main_count += row_count
            self.main_rows = []
        self.logger.info(
            "flush complete",
            extra={
                "event": "iex_benchmark_flush",
                "detail": {
                    "target": target,
                    "rows_written": row_count,
                    "main_count": self.main_count,
                    "quote_count": self.quote_count,
                    "path": str(path),
                    "size_bytes": path.stat().st_size if path.exists() else None,
                },
            },
        )

    def close(self) -> None:
        self.flush("main")
        self.flush("quote")
        if self.main_writer:
            self.main_writer.close()
        if self.quote_writer:
            self.quote_writer.close()


def artifact(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    pf = pq.ParquetFile(path)
    codecs = sorted(
        {
            pf.metadata.row_group(i).column(j).compression
            for i in range(pf.metadata.num_row_groups)
            for j in range(pf.metadata.row_group(i).num_columns)
        }
    )
    return {
        "path": str(path),
        "exists": True,
        "size_bytes": path.stat().st_size,
        "row_count": pf.metadata.num_rows,
        "row_groups": pf.metadata.num_row_groups,
        "compression": codecs,
    }
