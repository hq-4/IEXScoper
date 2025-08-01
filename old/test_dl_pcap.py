import os
import sys
import sqlite3
import requests
import urllib.parse
from multiprocessing import Pool, cpu_count
import time

# ✅ Max concurrent downloads (reduce if Google is blocking)
MAX_WORKERS = min(2, cpu_count())

# ✅ Database path
DB_PATH = "iex_data.db"

# ✅ Output directory for downloaded files
DOWNLOAD_DIR = "data/raw_pcap"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ✅ Browser-like headers (some services block automation)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "*/*",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.google.com/"
}

def get_pending_pcaps(year):
    """Fetches all pending PCAP downloads for the given year."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT date, link FROM pcap_downloads
        WHERE feed = 'TOPS' AND status = 'pending' AND date LIKE ?
        ORDER BY date ASC
    """, (f"{year}%",))

    rows = cursor.fetchall()
    conn.close()
    return rows  # List of (date, link) tuples

def update_pcap_status(date, status, filepath=None):
    """Updates the status of a downloaded PCAP file in the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE pcap_downloads
        SET status = ?, filepath = ?
        WHERE date = ?
    """, (status, filepath, date))

    conn.commit()
    conn.close()

def download_pcap(data):
    """Worker function to download a single PCAP file."""
    date, url = data

    # ✅ Decode URL properly
    url = urllib.parse.unquote(url)

    # ✅ Extract filename
    filename = os.path.basename(urllib.parse.urlparse(url).path)
    save_path = os.path.join(DOWNLOAD_DIR, filename)

    # ✅ Skip if already downloaded
    if os.path.exists(save_path):
        print(f"✅ Already downloaded: {filename}")
        update_pcap_status(date, "complete", save_path)
        return filename

    try:
        print(f"⬇ Downloading {filename} from {url}...")

        response = requests.get(url, headers=HEADERS, stream=True, timeout=60)
        print(f"🔍 HTTP Response for {filename}: {response.status_code}")

        if response.status_code == 200:
            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"✅ Download complete: {filename}")
            update_pcap_status(date, "complete", save_path)
            return filename
        else:
            print(f"❌ Failed: {filename} (HTTP {response.status_code})")
            update_pcap_status(date, "failed")
            return None
    except requests.RequestException as e:
        print(f"❌ Error downloading {filename}: {e}")
        update_pcap_status(date, "failed")
        return None

def main():
    """Main function to handle parallel downloading."""
    if len(sys.argv) < 2:
        print("❌ Usage: python download_tops.py <year>")
        sys.exit(1)

    year = sys.argv[1]
    print(f"📅 Filtering PCAP downloads for year {year}...")

    # ✅ Fetch files for the given year
    pending_pcaps = get_pending_pcaps(year)

    if not pending_pcaps:
        print(f"✅ No pending PCAPs found for year {year}.")
        return

    print(f"📂 Found {len(pending_pcaps)} PCAPs. Starting downloads using {MAX_WORKERS} workers...")

    # ✅ Use multiprocessing to download multiple PCAPs in parallel
    with Pool(MAX_WORKERS) as pool:
        results = pool.map(download_pcap, pending_pcaps)

    # ✅ Print summary of downloaded files
    for filename in results:
        if filename:
            print(f"✅ Successfully downloaded: {filename}")
        else:
            print("❌ Some downloads failed. Check logs.")

if __name__ == "__main__":
    main()
