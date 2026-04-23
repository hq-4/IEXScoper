from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl

SALE_CONDITION_EXCLUDES: tuple[str, ...] = ("CANCEL", "CORRECTION", "CORR")


def resolve_trade_csv_path(csv_root: str, yyyymmdd: str) -> Path:
    root = Path(csv_root)
    path = root / yyyymmdd[:4] / yyyymmdd[4:6] / (
        f"data_feeds_{yyyymmdd}_{yyyymmdd}_IEXTP1_DEEP1.0_trd.csv"
    )
    if not path.exists():
        raise FileNotFoundError(f"Trade CSV not found for {yyyymmdd}: {path}")
    return path


def _session_expression(ts_col: str) -> pl.Expr:
    minutes = (pl.col(ts_col).dt.hour() * 60) + pl.col(ts_col).dt.minute()
    pre_start = 4 * 60
    regular_start = 9 * 60 + 30
    after_start = 16 * 60
    after_end = 20 * 60
    return (
        pl.when((minutes >= pre_start) & (minutes < regular_start))
        .then(pl.lit("pre"))
        .when((minutes >= regular_start) & (minutes < after_start))
        .then(pl.lit("regular"))
        .when((minutes >= after_start) & (minutes < after_end))
        .then(pl.lit("after"))
        .when(minutes < pre_start)
        .then(pl.lit("pre"))
        .otherwise(pl.lit("after"))
    )


def scan_trades_csv_for_day(
    csv_root: str,
    yyyymmdd: str,
    symbols: Iterable[str] | None,
    display_tz: str,
) -> pl.DataFrame | None:
    path = resolve_trade_csv_path(csv_root, yyyymmdd)
    lf = pl.scan_csv(path, infer_schema_length=2000)
    rename_map = {
        "Exchange Timestamp": "exchange_timestamp_ns",
        "Symbol": "symbol",
        "Size": "size",
        "Price": "price",
        "Trade ID": "trade_id",
        "Sale Condition": "sale_condition",
    }
    missing = [src for src in rename_map if src not in lf.columns]
    if missing:
        raise ValueError(f"Missing columns in {path}: {missing}")
    lf = lf.rename(rename_map)
    if symbols:
        upper = [s.upper() for s in symbols]
        lf = lf.filter(pl.col("symbol").str.to_uppercase().is_in(upper))
    lf = lf.with_columns(
        pl.col("exchange_timestamp_ns").cast(pl.Int64),
        pl.col("symbol").str.to_uppercase().alias("symbol"),
        pl.col("size").cast(pl.Int64),
        pl.col("price").cast(pl.Float64),
        pl.col("trade_id").cast(pl.Utf8),
        pl.col("sale_condition").cast(pl.Utf8).fill_null("")
    )
    pattern = "|".join(SALE_CONDITION_EXCLUDES)
    lf = lf.filter(~pl.col("sale_condition").str.contains(pattern, literal=False, strict=False))
    lf = lf.unique(subset=["trade_id", "exchange_timestamp_ns", "symbol"], keep="first")
    lf = lf.with_columns(
        pl.col("exchange_timestamp_ns")
        .cast(pl.Datetime(time_zone="UTC", time_unit="ns"))
        .alias("exchange_ts_utc")
    )
    lf = lf.with_columns(
        pl.col("exchange_ts_utc").dt.convert_time_zone(display_tz).alias("exchange_ts_local")
    )
    lf = lf.with_columns(
        pl.col("exchange_ts_local").dt.truncate("1s").alias("ts_second_ny"),
        pl.col("exchange_ts_utc").dt.truncate("1s").alias("ts_second_utc"),
        pl.col("exchange_ts_local").dt.date().alias("day"),
        pl.col("exchange_ts_local").dt.year().cast(pl.Int16).alias("year"),
    )
    lf = lf.with_columns(_session_expression("ts_second_ny").alias("session"))
    agg = (
        lf.groupby(["symbol", "ts_second_ny"])
        .agg(
            pl.col("ts_second_utc").first().alias("ts_second_utc"),
            pl.col("session").first().alias("session"),
            pl.col("day").first().alias("day"),
            pl.col("year").first().alias("year"),
            pl.col("size").sum().alias("share_volume"),
            pl.count().alias("trade_count"),
            (pl.col("price") * pl.col("size")).sum().alias("dollar_volume"),
            pl.col("price").mean().alias("mean_price"),
        )
        .with_columns(
            pl.when(pl.col("share_volume") > 0)
            .then(pl.col("dollar_volume") / pl.col("share_volume"))
            .otherwise(pl.lit(None))
            .alias("vwap")
        )
        .drop("dollar_volume")
        .sort(["symbol", "ts_second_ny"])
    )
    df = agg.collect(streaming=True)
    return df if df.height > 0 else None
