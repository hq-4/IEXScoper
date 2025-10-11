from dataclasses import dataclass

@dataclass
class PerSecondTradeRow:
    symbol: str
    ts_second_ny: str
    ts_second_utc: str
    session: str
    vwap: float
    mean_price: float
    trade_count: int
    share_volume: int
    year: int
    day: str
