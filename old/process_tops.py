#!/usr/bin/env python3
import os
import sqlite3
from multiprocessing import Pool
from PCAPProcessor import PCAPProcessor  # Ensure this is in your PYTHONPATH
import config

def get_pending_tops_pcaps(db_path):
    """
    Connects to the SQLite database and returns a list of pending PCAP links for processing.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT link FROM pcap_downloads WHERE feed = 'TOPS' AND status = 'pending' ORDER BY date ASC")
    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]  # Extract the links from the rows

def update_pcap_status(db_path, link, status, filepath=None):
    """
    Updates the status of a PCAP file in the database.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE pcap_downloads
        SET status = ?, filepath = ?
        WHERE link = ?
    """, (status, filepath, link))
    conn.commit()
    conn.close()

def log_processed_day(db_path, date_str, quote_filepath, other_filepath):
    """
    Logs processed files into the database by inserting them into date_protocol_filepaths.
    """
    quote_key = f"{date_str}-QuoteUpdate"
    other_key = f"{date_str}-Other"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
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
    Worker function to process a single TOPS PCAP file.
    """
    db_path = "iex_data.db"  # Database path

    try:
        print(f"⬇ Processing PCAP: {link}")

        # ✅ Mark as 'processing' before starting
        update_pcap_status(db_path, link, "processing")

        # ✅ Process PCAP file
        processor = PCAPProcessor(link, delete_after_processing=True)
        quote_filepath, other_filepath = processor.process_file()

        # ✅ Extract trading day from filename
        base_filename = os.path.basename(processor.pcap_filepath)
        date_str = base_filename[:8] if base_filename.endswith('.pcap.gz') else base_filename.split('_')[0]

        # ✅ Mark as complete in database
        update_pcap_status(db_path, link, "complete", processor.pcap_filepath)

        return (date_str, quote_filepath, other_filepath, processor.status)

    except Exception as e:
        print(f"❌ Error processing {link}: {e}")
        update_pcap_status(db_path, link, "failed")  # Mark as failed
        return None

def main():
    db_path = "iex_data.db"  # Path to your SQLite database
    links = get_pending_tops_pcaps(db_path)

    if not links:
        print("✅ No pending TOPS PCAPs to process.")
        return

    print(f"📂 Found {len(links)} TOPS PCAPs to process.")

    # ✅ Process multiple files in parallel (adjust pool size if needed)
    with Pool(2) as pool:
        results = pool.map(process_link, links)

    # ✅ Log completed processing
    for result in results:
        if result is None:
            continue  # Skip failed processing
        date_str, quote_filepath, other_filepath, status = result
        if status:
            log_processed_day(db_path, date_str, quote_filepath, other_filepath)
            print(f"✅ Logged processed trading day {date_str}.")
        else:
            print(f"❌ Processing failed for {date_str}, not logging.")

if __name__ == "__main__":
    main()
