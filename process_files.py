#!/usr/bin/env python3
import os
import sqlite3
from multiprocessing import Pool
from PCAPProcessor import PCAPProcessor  # Make sure this file is in your PYTHONPATH
import config

def get_tops_links(db_path):
    """
    Connects to the SQLite database and returns a list of links (as strings)
    for rows where protocol = 'TOPS', ordered from oldest date to newest.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT link FROM iex_feeds WHERE protocol = 'TOPS' ORDER BY date ASC")
    rows = cursor.fetchall()
    conn.close()
    # Each row is a one-item tuple; return a list of the link strings.
    return [row[0] for row in rows]

def log_processed_day(db_path, date_str, quote_filepath, other_filepath):
    """
    Logs the processed files by inserting two rows into the date_protocol_filepaths table.
    To avoid violating the UNIQUE constraint on date_protocol, we combine the date with a file-type
    identifier. For example:
        - QuoteUpdate row: date_protocol = "20171028-QuoteUpdate"
        - Other row:       date_protocol = "20171028-Other"
    """
    quote_key = f"{date_str}-QuoteUpdate"
    other_key = f"{date_str}-Other"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Use INSERT OR REPLACE to update the log if the key already exists.
    cursor.execute("""
        INSERT OR REPLACE INTO date_protocol_filepaths (date_protocol, filepath)
        VALUES (?, ?)
    """, (quote_key, quote_filepath))
    cursor.execute("""
        INSERT OR REPLACE INTO date_protocol_filepaths (date_protocol, filepath)
        VALUES (?, ?)
    """, (other_key, other_filepath))
    conn.commit()
    conn.close()

def process_link(link):
    """
    Worker function to process a single TOPS PCAP link.
    
    This function:
      - Instantiates a PCAPProcessor with the given link (which downloads the file if necessary).
      - Processes the file (with delete_after_processing enabled).
      - Extracts the trading day from the first eight characters of the PCAP filename.
      - Returns a tuple: (trading_day, QuoteUpdate_filepath, Other_filepath, status)
        where status is True if processing succeeded, False otherwise.
    """
    try:
        processor = PCAPProcessor(link, delete_after_processing=True)
        quote_filepath, other_filepath = processor.process_file()
        # Extract the trading day from the PCAP filename (assumed to be in the first 8 characters)
        base_filename = os.path.basename(processor.pcap_filepath)
        if base_filename.endswith('.pcap.gz'):
            date_str = base_filename[:8]
        else:
            # If the file extension is different, try splitting on '_' and use the first part.
            date_str = base_filename.split('_')[0]
        return (date_str, quote_filepath, other_filepath, processor.status)
    except Exception as e:
        print(f"Error processing link {link}: {e}")
        return None

def main():
    db_path = "iex_data.db"  # Path to your SQLite database
    links = get_tops_links(db_path)
    if not links:
        print("No TOPS links found in the database.")
        return

    print(f"Found {len(links)} TOPS links to process.")

    # Use multiprocessing to process 2 links at a time.
    with Pool(2) as pool:
        results = pool.map(process_link, links)

    # Log the processed trading days.
    for result in results:
        if result is None:
            continue  # Skip links that failed processing.
        date_str, quote_filepath, other_filepath, status = result
        if status:
            log_processed_day(db_path, date_str, quote_filepath, other_filepath)
            print(f"Logged processed trading day {date_str}.")
        else:
            print(f"Processing failed for trading day {date_str}, not logging.")

if __name__ == "__main__":
    main()
