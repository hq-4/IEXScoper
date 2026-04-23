from __future__ import annotations

import uuid
from datetime import date, timedelta
from pathlib import Path

import polars as pl

ZSTD_LEVEL = 5


def staging_root(parquet_root: str, year: int) -> Path:
    return Path(parquet_root) / "derived" / "_staging" / f"year={year}"


def master_root(parquet_root: str, year: int) -> Path:
    return Path(parquet_root) / "derived" / f"{year}"


def write_staging_parts(
    parquet_root: str,
    year: int,
    day: str,
    df: pl.DataFrame,
    filter_version: str,
    rebuild: bool,
) -> list[dict]:
    root = staging_root(parquet_root, year)
    root.mkdir(parents=True, exist_ok=True)
    summaries: list[dict] = []
    partitions = df.partition_by("symbol", as_dict=True)
    for symbol, frame in partitions.items():
        symbol_dir = root / f"symbol={symbol}"
        symbol_dir.mkdir(parents=True, exist_ok=True)
        if rebuild:
            for existing in symbol_dir.glob(f"part-{day}-{filter_version}-*.parquet"):
                existing.unlink(missing_ok=True)
        part_name = f"part-{day}-{filter_version}-{uuid.uuid4().hex[:8]}.parquet"
        target = symbol_dir / part_name
        tmp_path = target.with_suffix(".tmp")
        frame.write_parquet(
            tmp_path,
            compression="zstd",
            compression_level=ZSTD_LEVEL,
            statistics=True,
            use_pyarrow=True,
        )
        tmp_path.replace(target)
        summaries.append(
            {
                "path": str(target),
                "symbol": symbol,
                "day": day,
                "rows": frame.height,
                "size_bytes": target.stat().st_size,
                "min_ts": frame["ts_second_utc"].min(),
                "max_ts": frame["ts_second_utc"].max(),
            }
        )
    return summaries


def compact_master_from_staging(
    parquet_root: str,
    year: int,
    filter_version: str,
) -> dict | None:
    root = staging_root(parquet_root, year)
    if not root.exists():
        return None
    part_files = sorted(root.rglob(f"part-*-{filter_version}-*.parquet"))
    if not part_files:
        return None

    master_dir = master_root(parquet_root, year)
    master_dir.mkdir(parents=True, exist_ok=True)
    master_path = master_dir / f"{year}_IEX_TOPS1.6_trades_persec.parquet"
    tmp_path = master_path.with_suffix(".tmp")

    lf = pl.scan_parquet([str(p) for p in part_files])
    lf = lf.sort(["symbol", "ts_second_ny"])
    df = lf.collect(streaming=True)
    if df.height == 0:
        return None

    df.write_parquet(
        tmp_path,
        compression="zstd",
        compression_level=ZSTD_LEVEL,
        statistics=True,
        use_pyarrow=True,
        row_group_size=500_000,
    )
    tmp_path.replace(master_path)

    return {
        "path": str(master_path),
        "rows": df.height,
        "size_bytes": master_path.stat().st_size,
        "min_ts": df["ts_second_utc"].min(),
        "max_ts": df["ts_second_utc"].max(),
        "input_parts": len(part_files),
    }


def delete_old_staging_parts(
    parquet_root: str,
    year: int,
    keep_days: int,
    reference_date: date | None = None,
) -> list[str]:
    root = staging_root(parquet_root, year)
    removed: list[str] = []
    if not root.exists():
        return removed
    ref = reference_date or date.today()
    cutoff = ref - timedelta(days=keep_days)
    cutoff_str = cutoff.strftime("%Y%m%d")
    for symbol_dir in root.glob("symbol=*"):
        for part in symbol_dir.glob("part-*.parquet"):
            try:
                day_token = part.name.split("-")[1]
            except IndexError:
                continue
            if day_token < cutoff_str:
                part.unlink(missing_ok=True)
                removed.append(str(part))
    return removed
