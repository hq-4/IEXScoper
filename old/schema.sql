CREATE TABLE tradereport (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    flags INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    size INTEGER NOT NULL,
    price_int INTEGER NOT NULL,
    trade_id INTEGER NOT NULL UNIQUE
);
CREATE TABLE quoteupdate (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    flags INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    bid_size INTEGER NOT NULL,
    bid_price_int INTEGER NOT NULL,
    ask_price_int INTEGER NOT NULL,
    ask_size INTEGER NOT NULL
);