from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl

from src.framework.logging import get_logger

# Sale conditions emitted by the bundled TOPS parser (decode_messages.cpp):
# INTERMARKET_SWEEP, EXTENDED_HOURS, REGULAR_HOURS, ODD_LOT, TRADE_THROUGH_EXEMPT,
# SINGLE_PRICE_CROSS. Cancels are NOT a sale condition — they arrive as Trade Break
# messages, which this trade-only CSV path does not carry, so busted trades cannot be
# excluded here. Odd lots are not last-sale eligible; excluding them is the default so
# VWAP follows consolidated-tape conventions (pass exclude_odd_lots=False to keep them).
ODD_LOT_CONDITION = "ODD_LOT"


def resolve_trade_csv_path(csv_root: str, yyyymmdd: str, feed: str = "TOPS") -> Path:
    root = Path(csv_root)
    day_dir = root / yyyymmdd[:4] / yyyymmdd[4:6]
    feed_upper = feed.upper()
    candidates = [
        day_dir / f"{yyyymmdd}_IEXTP1_{feed_upper}1.6_trd.csv",
        day_dir / f"{yyyymmdd}_IEXTP1_{feed_upper}1.6.pcap_trd.csv",
        day_dir / f"data_feeds_{yyyymmdd}_{yyyymmdd}_IEXTP1_{feed_upper}1.6_trd.csv",
    ]
    if feed_upper == "DEEP":
        candidates.append(day_dir / f"data_feeds_{yyyymmdd}_{yyyymmdd}_IEXTP1_DEEP1.0_trd.csv")
    for path in candidates:
        if path.exists():
            return path

    globbed = sorted(day_dir.glob(f"*{yyyymmdd}*IEXTP1*{feed_upper}*_trd.csv"))
    if globbed:
        return globbed[0]

    checked = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"Trade CSV not found for {yyyymmdd}; checked: {checked}")


def _session_expression(ts_col: str) -> pl.Expr:
    # Clock-time session labels (America/New_York). No `unknown` bucket would let
    # corrupted timestamps pass as plausible sessions: anything outside 04:00-20:00
    # is anomalous on IEX and must be labeled "unknown". Note: labels are clock-based
    # and do not model early-close (half-day) calendars.
    minutes = (pl.col(ts_col).dt.hour().cast(pl.Int16) * 60) + pl.col(ts_col).dt.minute().cast(
        pl.Int16
    )
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
        .otherwise(pl.lit("unknown"))
    )


def scan_trades_csv_for_day(
    csv_root: str,
    yyyymmdd: str,
    symbols: Iterable[str] | None,
    display_tz: str,
    feed: str = "TOPS",
    *,
    exclude_odd_lots: bool = True,
) -> pl.DataFrame | None:
    path = resolve_trade_csv_path(csv_root, yyyymmdd, feed=feed)
    lf = pl.scan_csv(path, infer_schema_length=2000)
    columns = lf.collect_schema().names()
    timestamp_column = "Exchange Timestamp" if "Exchange Timestamp" in columns else "Raw Timestamp"
    rename_map = {
        timestamp_column: "exchange_timestamp_ns",
        "Symbol": "symbol",
        "Size": "size",
        "Price": "price",
        "Trade ID": "trade_id",
        "Sale Condition": "sale_condition",
    }
    missing = [src for src in rename_map if src not in columns]
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
        pl.col("sale_condition").cast(pl.Utf8).fill_null(""),
    )
    if exclude_odd_lots:
        lf = lf.filter(~pl.col("sale_condition").str.contains(ODD_LOT_CONDITION, literal=True))
    # Trade IDs are unique per symbol per day; dedupe on (trade_id, symbol) alone so
    # retransmitted messages with perturbed timestamps (IEX-TP gap fill, PCAP replay)
    # cannot survive as duplicate trades. Collisions are counted as a DQ metric.
    raw = lf.collect()
    collisions = raw.height - raw.select(pl.struct("trade_id", "symbol").n_unique()).item()
    if collisions:
        get_logger(__name__).warning(
            "trade_id_collisions",
            extra={
                "event": "trade_id_collisions",
                "day": yyyymmdd,
                "detail": {"collisions": collisions},
            },
        )
    lf = raw.unique(subset=["trade_id", "symbol"], keep="first").lazy()
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
        lf.group_by(["symbol", "ts_second_ny"])
        .agg(
            pl.col("ts_second_utc").first().alias("ts_second_utc"),
            pl.col("session").first().alias("session"),
            pl.col("day").first().alias("day"),
            pl.col("year").first().alias("year"),
            pl.col("size").sum().alias("share_volume"),
            pl.len().alias("trade_count"),
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
    df = agg.collect()
    return df if df.height > 0 else None
