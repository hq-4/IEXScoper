from __future__ import annotations

import shutil
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from utils.iextools_backfill_core import existing_tops_days, tops_output_paths

PRICE_SCALE = 10_000.0
MAIN_REPAIRS = (("price", "price_int"),)
QUOTE_REPAIRS = (("bid_price", "bid_price_int"), ("ask_price", "ask_price_int"))


@dataclass
class ColumnAudit:
    float_column: str
    int_column: str
    float_non_null: int
    int_non_null: int
    fillable_nulls: int


@dataclass
class FileRepairResult:
    path: str
    exists: bool
    repaired: bool
    row_count: int
    row_groups: int
    size_bytes_before: int
    size_bytes_after: int | None
    columns: list[ColumnAudit]

    @property
    def needs_repair(self) -> bool:
        return any(column.fillable_nulls > 0 for column in self.columns)


def select_days(
    parquet_root: Path,
    *,
    start_day: str,
    end_day: str | None,
    days_arg: str | None,
    limit_days: int | None,
) -> list[str]:
    if days_arg:
        days = sorted({day.strip() for day in days_arg.split(",") if day.strip()})
    else:
        days = [
            day
            for day in sorted(existing_tops_days(parquet_root))
            if day >= start_day and (end_day is None or day <= end_day)
        ]
    if limit_days is not None:
        return days[:limit_days]
    return days


def repair_day(
    *,
    parquet_root: Path,
    day: str,
    apply: bool,
    backup_root: Path | None = None,
) -> dict[str, Any]:
    main_path, quote_path = tops_output_paths(parquet_root, day)
    main = audit_or_repair_file(
        main_path,
        repairs=MAIN_REPAIRS,
        apply=apply,
        backup_path=_backup_path(backup_root, main_path, parquet_root) if backup_root else None,
    )
    quote = audit_or_repair_file(
        quote_path,
        repairs=QUOTE_REPAIRS,
        apply=apply,
        backup_path=_backup_path(backup_root, quote_path, parquet_root) if backup_root else None,
    )
    return {
        "day": day,
        "status": "repaired" if apply and (main.repaired or quote.repaired) else "audited",
        "needs_repair": main.needs_repair or quote.needs_repair,
        "main": _result_payload(main),
        "quote": _result_payload(quote),
    }


def audit_or_repair_file(
    path: Path,
    *,
    repairs: tuple[tuple[str, str], ...],
    apply: bool,
    backup_path: Path | None = None,
) -> FileRepairResult:
    if not path.exists():
        return FileRepairResult(str(path), False, False, 0, 0, 0, None, [])
    before_size = path.stat().st_size
    audit = audit_file(path, repairs)
    if not apply or not any(column.fillable_nulls > 0 for column in audit.columns):
        return FileRepairResult(
            str(path),
            True,
            False,
            audit.row_count,
            audit.row_groups,
            before_size,
            None,
            audit.columns,
        )
    tmp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.repair.tmp")
    try:
        write_repaired_file(path, tmp_path, repairs)
        verify_repaired_file(original_path=path, repaired_path=tmp_path, repairs=repairs)
        if backup_path is not None:
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, backup_path)
        tmp_path.replace(path)
    finally:
        tmp_path.unlink(missing_ok=True)
    return FileRepairResult(
        str(path),
        True,
        True,
        audit.row_count,
        audit.row_groups,
        before_size,
        path.stat().st_size,
        audit.columns,
    )


def audit_file(path: Path, repairs: tuple[tuple[str, str], ...]) -> FileRepairResult:
    pf = pq.ParquetFile(path)
    totals = {repair: [0, 0, 0] for repair in repairs}
    for row_group_index in range(pf.metadata.num_row_groups):
        table = pf.read_row_group(row_group_index, columns=_repair_columns(repairs))
        for float_column, int_column in repairs:
            float_array = table[float_column]
            int_array = table[int_column]
            fillable = pc.and_(pc.is_null(float_array), pc.invert(pc.is_null(int_array)))
            totals[(float_column, int_column)][0] += pc.count(float_array).as_py()
            totals[(float_column, int_column)][1] += pc.count(int_array).as_py()
            totals[(float_column, int_column)][2] += _sum_bool(fillable)
    columns = [
        ColumnAudit(
            float_column=float_column,
            int_column=int_column,
            float_non_null=values[0],
            int_non_null=values[1],
            fillable_nulls=values[2],
        )
        for (float_column, int_column), values in totals.items()
    ]
    return FileRepairResult(
        str(path),
        True,
        False,
        pf.metadata.num_rows,
        pf.metadata.num_row_groups,
        path.stat().st_size,
        None,
        columns,
    )


def write_repaired_file(
    source_path: Path,
    target_path: Path,
    repairs: tuple[tuple[str, str], ...],
) -> None:
    source = pq.ParquetFile(source_path)
    writer = pq.ParquetWriter(
        target_path,
        source.schema_arrow,
        compression=_compression_arg(source),
    )
    try:
        for row_group_index in range(source.metadata.num_row_groups):
            table = source.read_row_group(row_group_index)
            writer.write_table(_repair_table(table, repairs))
    finally:
        writer.close()


def verify_repaired_file(
    *,
    original_path: Path,
    repaired_path: Path,
    repairs: tuple[tuple[str, str], ...],
) -> None:
    original = pq.ParquetFile(original_path)
    repaired = pq.ParquetFile(repaired_path)
    if repaired.metadata.num_rows != original.metadata.num_rows:
        raise RuntimeError("row count changed during price repair")
    if repaired.metadata.num_row_groups != original.metadata.num_row_groups:
        raise RuntimeError("row group count changed during price repair")
    if repaired.schema_arrow != original.schema_arrow:
        raise RuntimeError("schema changed during price repair")
    remaining = [column for column in audit_file(repaired_path, repairs).columns if column.fillable_nulls]
    if remaining:
        raise RuntimeError(f"price repair left fillable nulls: {remaining}")


def _repair_table(table: pa.Table, repairs: tuple[tuple[str, str], ...]) -> pa.Table:
    repaired = table
    for float_column, int_column in repairs:
        field_index = repaired.schema.get_field_index(float_column)
        field = repaired.schema.field(field_index)
        derived = pc.divide(pc.cast(repaired[int_column], pa.float64()), pa.scalar(PRICE_SCALE))
        filled = pc.if_else(pc.is_null(repaired[float_column]), derived, repaired[float_column])
        repaired = repaired.set_column(field_index, field, filled)
    return repaired


def _compression_arg(source: pq.ParquetFile) -> str | dict[str, str | None] | None:
    codecs_by_column: dict[str, str | None] = {}
    schema = source.schema_arrow
    for column_index, field in enumerate(schema):
        codec = source.metadata.row_group(0).column(column_index).compression
        codecs_by_column[field.name] = _codec_arg(codec)
    codecs = set(codecs_by_column.values())
    if len(codecs) == 1:
        return codecs.pop()
    return codecs_by_column


def _codec_arg(codec: str) -> str | None:
    if codec == "UNCOMPRESSED":
        return None
    return codec.lower()


def _repair_columns(repairs: tuple[tuple[str, str], ...]) -> list[str]:
    return [column for repair in repairs for column in repair]


def _backup_path(backup_root: Path, source_path: Path, parquet_root: Path) -> Path:
    return backup_root / source_path.relative_to(parquet_root)


def _result_payload(result: FileRepairResult) -> dict[str, Any]:
    payload = asdict(result)
    payload["needs_repair"] = result.needs_repair
    return payload


def _sum_bool(array: pa.Array | pa.ChunkedArray) -> int:
    total = pc.sum(pc.cast(array, pa.int64())).as_py()
    return int(total or 0)
