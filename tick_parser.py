import os
import sqlite3
import config
from IEXTools import Parser, messages

'''
takes raw TOPS files and dumps to sqlite database in chunks of 1000 inserts at a time
'''

def get_pcap_filepath(filename: str) -> str:
    """Returns the full path of a PCAP file by appending the filename to PCAP_FOLDER."""
    return os.path.abspath(os.path.join(config.PCAP_FOLDER, filename))

def get_db_filepath() -> str:
    """Returns the full path of the SQLite database file in the same folder as PCAP files."""
    return os.path.abspath(os.path.join(config.PCAP_FOLDER, "data.db"))

### iterate over every message when IEX Parser is instantiated and lazily loads this with a generator
def iter_trade_reports(parser):
    allowed = [messages.TradeReport]
    while True:
        msg = parser.get_next_message(allowed)
        if msg is None:
            break
        yield msg

if __name__ == "__main__":
    pcap_file_name = get_pcap_filepath("20241231_IEXTP1_TOPS1.6.pcap.gz")
    db_file_name = get_db_filepath()
    print(f"Using database: {db_file_name}")
    print(f"Processing PCAP file: {pcap_file_name}")
    
    p = Parser(pcap_file_name)
    
    # Set up SQLite database
    conn = sqlite3.connect(db_file_name)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tradereport (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            flags INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            size INTEGER NOT NULL,
            price_int INTEGER NOT NULL,
            trade_id INTEGER NOT NULL UNIQUE
        )
    """)
    conn.commit()
    print("Database setup completed.")
    
    batch = []
    batch_size = 1000
    record_count = 0
    
    for trade in iter_trade_reports(p):
        batch.append((trade.flags, trade.timestamp, trade.symbol, trade.size, trade.price_int, trade.trade_id))
        
        if len(batch) >= batch_size:
            cursor.executemany("""
                INSERT INTO tradereport (flags, timestamp, symbol, size, price_int, trade_id) 
                VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(trade_id) DO NOTHING
            """, batch)
            conn.commit()
            record_count += len(batch)
            print(f"Inserted {record_count} records so far...")
            batch.clear()
    
    # Insert any remaining records
    if batch:
        cursor.executemany("""
            INSERT INTO tradereport (flags, timestamp, symbol, size, price_int, trade_id) 
            VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(trade_id) DO NOTHING
        """, batch)
        conn.commit()
        record_count += len(batch)
        print(f"Final batch inserted. Total records inserted: {record_count}")
    
    print("Data processing complete. Closing database connection.")
    conn.close()
