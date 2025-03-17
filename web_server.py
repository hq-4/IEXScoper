import os
import requests
import sqlite3
import time
import threading
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from jinja2 import Template
from contextlib import asynccontextmanager
import uvicorn

DB_PATH = "iex_data.db"
TXT_OUTPUT_DIR = "pcap_link_lists"

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Pending IEX Trading Data</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; padding: 0; background-color: #f4f4f4; }
        h2 { text-align: center; }
        table { width: 100%; border-collapse: collapse; background: white; margin-top: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #4CAF50; color: white; }
        tr:nth-child(even) { background-color: #f9f9f9; }
        tr:hover { background-color: #ddd; }
    </style>
</head>
<body>
    <h2>Pending IEX Trading Data</h2>
    <table>
        <tr>
            <th>Date</th>
            <th>Feed</th>
            <th>Link</th>
            <th>Version</th>
            <th>Protocol</th>
            <th>Size</th>
        </tr>
        {% for row in data %}
        <tr>
            <td>{{ row.date }}</td>
            <td>{{ row.feed }}</td>
            <td><a href="{{ row.link }}" target="_blank">Download</a></td>
            <td>{{ row.version }}</td>
            <td>{{ row.protocol }}</td>
            <td>{{ row.size }}</td>
        </tr>
        {% endfor %}
    </table>
</body>
</html>
"""

def setup_database():
    """Sets up the SQLite database with necessary schema."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(""" 
        CREATE TABLE IF NOT EXISTS iex_feeds (
            date TEXT NOT NULL,
            feed TEXT NOT NULL,
            link TEXT NOT NULL,
            version TEXT NOT NULL,
            protocol TEXT NOT NULL,
            size INTEGER NOT NULL,
            PRIMARY KEY (date, feed)
        )
    """)

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pcap_downloads (
            date TEXT NOT NULL,
            feed TEXT NOT NULL,
            link TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            PRIMARY KEY (date, feed),
            FOREIGN KEY (date, feed) REFERENCES iex_feeds(date, feed) ON DELETE CASCADE
        )
    ''')

    conn.commit()
    conn.close()

def refresh_pcap_links():
    """Ensures `pcap_downloads` always has the latest links from `iex_feeds`."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE pcap_downloads
        SET link = (
            SELECT iex_feeds.link
            FROM iex_feeds
            WHERE iex_feeds.date = pcap_downloads.date
              AND iex_feeds.feed = pcap_downloads.feed
        )
        WHERE EXISTS (
            SELECT 1 FROM iex_feeds
            WHERE iex_feeds.date = pcap_downloads.date
              AND iex_feeds.feed = pcap_downloads.feed
              AND iex_feeds.link != pcap_downloads.link
        );
    """)

    conn.commit()
    conn.close()
    print("🔄 Updated stale links in `pcap_downloads`.")

def generate_yearly_txt_files():
    """Generates yearly text files with fresh links for feed=TOPS and version=1.6."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ✅ Get unique years in the database
    cursor.execute("SELECT DISTINCT SUBSTR(date, 1, 4) AS year FROM iex_feeds ORDER BY year DESC;")
    years = [row[0] for row in cursor.fetchall()]

    # ✅ Create a directory to store text files
    os.makedirs(TXT_OUTPUT_DIR, exist_ok=True)

    for year in years:
        # ✅ Get all links for the specified year, feed=TOPS, and version=1.6
        cursor.execute("""
            SELECT link FROM iex_feeds
            WHERE feed = 'TOPS' AND version = '1.6' AND date LIKE ?;
        """, (f"{year}%",))

        links = [row[0] for row in cursor.fetchall()]
        
        # ✅ Write to text file if there are links
        if links:
            file_path = os.path.join(TXT_OUTPUT_DIR, f"pcap_links_{year}.txt")
            with open(file_path, "w") as f:
                f.write("\n".join(links))
            print(f"📄 Generated {file_path} with {len(links)} links.")

    conn.close()

def fetch_iex_data():
    """Fetches data from the IEX API and correctly processes all dates."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get("https://iextrading.com/api/1.0/hist", headers=headers)

    if response.status_code == 200:
        data = response.json()
        parsed_data = [(d["date"], d["feed"], d["link"], d["version"], d["protocol"], int(d["size"])) for date, feeds in data.items() for d in feeds]
        return parsed_data
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

def upsert_data(data):
    """Upserts data into iex_feeds and pcap_downloads."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for entry in data:
        date, feed, link, version, protocol, size = entry

        cursor.execute('''
            INSERT INTO iex_feeds (date, feed, link, version, protocol, size)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, feed) DO UPDATE SET link = excluded.link, size = excluded.size;
        ''', (date, feed, link, version, protocol, size))

        cursor.execute('''
            INSERT INTO pcap_downloads (date, feed, link, status)
            VALUES (?, ?, ?, 'pending')
            ON CONFLICT(date, feed) DO UPDATE SET link = excluded.link;
        ''', (date, feed, link))

    conn.commit()
    conn.close()

def auto_update():
    """Runs the update process in the background indefinitely."""
    while True:
        print("🔄 Fetching latest data from IEX...")
        data = fetch_iex_data()
        if data:
            upsert_data(data)
            refresh_pcap_links()  # ✅ Ensures fresh links in `pcap_downloads`
            generate_yearly_txt_files()  # ✅ Generates yearly text files
            print("✅ Database updated, and yearly TXT files generated.")
        else:
            print("❌ No new data retrieved.")

        time.sleep(30 * 60)  # 🔄 Update every 30 minutes

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_database()
    updater_thread = threading.Thread(target=auto_update, daemon=True)
    updater_thread.start()
    yield

app = FastAPI(lifespan=lifespan)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")
