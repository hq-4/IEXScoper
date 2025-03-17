import os
import sqlite3
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
import concurrent.futures
import urllib

# ✅ Database file path
DB_PATH = "iex_data.db"

# ✅ Directory for PCAP downloads
PCAP_DIR = "V:/raw_pcap"
os.makedirs(PCAP_DIR, exist_ok=True)

# ✅ Max parallel downloads (adjustable)
MAX_WORKERS = 5
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "*/*",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.google.com/"
}

def setup_pcap_table():
    """Creates a dedicated table to track PCAP file downloads."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pcap_downloads (
            date TEXT NOT NULL,
            feed TEXT NOT NULL,
            link TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',  -- pending, downloading, complete
            filepath TEXT DEFAULT NULL,
            PRIMARY KEY (date, feed)
        )
    ''')

    conn.commit()
    conn.close()

def fetch_pending_pcaps():
    """Gets all pending PCAP downloads (TOPS 1.6)."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT iex_feeds.date, iex_feeds.feed, iex_feeds.link
        FROM iex_feeds
        LEFT JOIN pcap_downloads ON iex_feeds.date = pcap_downloads.date AND iex_feeds.feed = pcap_downloads.feed
        WHERE iex_feeds.feed = 'TOPS' AND iex_feeds.version = '1.6'
        AND (pcap_downloads.status IS NULL OR pcap_downloads.status != 'complete')
    """)

    rows = cursor.fetchall()
    conn.close()
    return rows

def update_pcap_status(date, feed, status, filepath=None):
    """Updates download status in `pcap_downloads`."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO pcap_downloads (date, feed, link, status, filepath)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(date, feed) DO UPDATE SET
            status = excluded.status,
            filepath = excluded.filepath
    """, (date, feed, None, status, filepath))

    conn.commit()
    conn.close()

def download_pcap(date, feed, url):
    """Downloads a PCAP file and updates status."""
    try:
        # ✅ Decode the URL (fix Google Cloud Storage encoding issues)
        url = urllib.parse.unquote(url)

        # Extract filename
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        save_path = os.path.join(PCAP_DIR, filename)

        # ✅ Print what we are downloading
        print(f"⬇ Downloading {filename} from {url}...")

        # Skip if already downloaded
        if os.path.exists(save_path):
            print(f"✅ Already downloaded: {filename}")
            update_pcap_status(date, feed, "complete", save_path)
            return

        update_pcap_status(date, feed, "downloading")

        # ✅ Perform the request & print the status code
        response = requests.get(url, stream=True, timeout=60)
        print(f"🔍 HTTP Response: {response.status_code} for {filename}")

        if response.status_code == 200:
            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"✅ Download complete: {filename} ({save_path})")
            update_pcap_status(date, feed, "complete", save_path)
        else:
            print(f"❌ Failed {filename}: HTTP {response.status_code}")
            update_pcap_status(date, feed, "pending")
    except requests.RequestException as e:
        print(f"❌ Error downloading {filename}: {e}")
        update_pcap_status(date, feed, "pending")

def refresh_and_download_pcaps():
    """Continuously refreshes URLs and downloads PCAPs in parallel."""
    setup_pcap_table()

    while True:
        print("\n🔄 Refreshing database links and downloading pending PCAPs...")

        # Fetch pending PCAPs
        pending_pcaps = fetch_pending_pcaps()

        if not pending_pcaps:
            print("✅ No new PCAPs to download.")
        else:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(download_pcap, date, feed, url) for date, feed, url in pending_pcaps}

                # ✅ Wait until all downloads finish before sleeping
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"❌ Error in thread: {e}")

        print("\n🕒 Waiting 4 hours before refreshing again...")
        time.sleep(4 * 60 * 60)  # Sleep AFTER downloads finish
if __name__ == "__main__":
    refresh_and_download_pcaps()
