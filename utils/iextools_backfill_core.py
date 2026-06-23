from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from utils.parse_iex_hist_index import HistFileRecord


def tops_output_paths(parquet_root: Path, day: str) -> tuple[Path, Path]:
    target_dir = parquet_root / day[:4] / day[4:6]
    return (
        target_dir / f"{day}_IEXTP1_TOPS1.6.parquet",
        target_dir / f"{day}_IEXTP1_TOPS1.6_QuoteUpdate.parquet",
    )


def existing_tops_days(parquet_root: Path) -> set[str]:
    days: set[str] = set()
    for path in parquet_root.glob("*/*/*_IEXTP1_TOPS1.6.parquet"):
        day = path.name[:8]
        quote_path = path.with_name(f"{day}_IEXTP1_TOPS1.6_QuoteUpdate.parquet")
        if quote_path.exists():
            days.add(day)
    return days


def select_missing_tops_days(
    records_by_day: dict[str, list[HistFileRecord]],
    parquet_root: Path,
    *,
    start_day: str,
    end_day: str | None,
    limit_days: int | None,
) -> list[str]:
    existing = existing_tops_days(parquet_root)
    days = []
    for day in sorted(records_by_day):
        if day < start_day:
            continue
        if end_day is not None and day > end_day:
            continue
        if day in existing:
            continue
        if any(record.feed == "TOPS" for record in records_by_day[day]):
            days.append(day)
    if limit_days is not None:
        days = days[:limit_days]
    return days


def choose_tops_record(records_by_day: dict[str, list[HistFileRecord]], day: str) -> HistFileRecord:
    tops_records = [record for record in records_by_day[day] if record.feed == "TOPS"]
    if tops_records:
        return max(tops_records, key=lambda record: (record.size_bytes, record.version))
    raise KeyError(f"missing TOPS record for {day}")


def verify_parquet_pair(main_path: Path, quote_path: Path) -> dict[str, int]:
    main_pf = pq.ParquetFile(main_path)
    quote_pf = pq.ParquetFile(quote_path)
    return {
        "main_rows": main_pf.metadata.num_rows,
        "quote_rows": quote_pf.metadata.num_rows,
        "main_row_groups": main_pf.metadata.num_row_groups,
        "quote_row_groups": quote_pf.metadata.num_row_groups,
    }


def publish_parquet_pair(
    local_main: Path,
    local_quote: Path,
    parquet_root: Path,
    day: str,
    *,
    publish_token: str,
) -> dict[str, Any]:
    final_main, final_quote = tops_output_paths(parquet_root, day)
    final_main.parent.mkdir(parents=True, exist_ok=True)
    if final_main.exists() or final_quote.exists():
        raise FileExistsError(f"refusing to overwrite existing parquet outputs for {day}")

    tmp_main = final_main.with_name(final_main.name + f".{publish_token}.tmp")
    tmp_quote = final_quote.with_name(final_quote.name + f".{publish_token}.tmp")
    shutil.copy2(local_main, tmp_main)
    shutil.copy2(local_quote, tmp_quote)
    stats = verify_parquet_pair(tmp_main, tmp_quote)
    tmp_main.replace(final_main)
    tmp_quote.replace(final_quote)
    return {
        "main_path": str(final_main),
        "quote_path": str(final_quote),
        "main_size_bytes": final_main.stat().st_size,
        "quote_size_bytes": final_quote.stat().st_size,
        **stats,
    }


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")
