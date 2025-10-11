import polars as pl

def scan_trades_csv_for_day(csv_root: str, yyyymmdd: str, symbols: list[str] | None) -> pl.LazyFrame:
    pattern = f"{csv_root}/{yyyymmdd[:4]}/{yyyymmdd[4:6]}/data_feeds_{yyyymmdd}_{yyyymmdd}_IEXTP1_DEEP1.0_trd.csv"
    lf = pl.scan_csv(pattern, has_header=True, infer_schema_length=1000, ignore_errors=True)
    if symbols:
        lf = lf.filter(pl.col("Symbol").is_in(symbols))
    return lf
