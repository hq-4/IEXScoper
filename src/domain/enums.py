from enum import Enum

class DatasetType(str, Enum):
    TRADES_PER_SECOND = "trades_per_second"

class Session(str, Enum):
    PRE = "pre"
    REGULAR = "regular"
    AFTER = "after"
