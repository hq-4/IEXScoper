import requests
import sqlite3
import time

def setup_database(db_path="iex_data.db"):
    """Sets up the SQLite database with the necessary schema and upsert logic."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS iex_feeds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            feed TEXT NOT NULL,
            link TEXT NOT NULL,
            version TEXT NOT NULL,
            protocol TEXT NOT NULL,
            size INTEGER NOT NULL,
            UNIQUE(date, feed) ON CONFLICT(date, feed) DO UPDATE SET
                link=excluded.link
        )
    ''')
    conn.commit()
    conn.close()

def fetch_iex_data(api_url="https://iextrading.com/api/1.0/hist"):
    """Fetches data from the IEX API with a generic user-agent."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

def upsert_data(data, db_path="iex_data.db"):
    """Upserts data into the SQLite database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for date, feeds in data.items():
        for feed_entry in feeds:
            cursor.execute('''
                INSERT INTO iex_feeds (date, feed, link, version, protocol, size)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, feed) DO UPDATE SET
                    link=excluded.link
            ''', (feed_entry["date"], feed_entry["feed"], feed_entry["link"], 
                  feed_entry["version"], feed_entry["protocol"], int(feed_entry["size"])))
    
    conn.commit()
    conn.close()

def main():
    setup_database()
    while True:
        data = fetch_iex_data()
        if data:
            upsert_data(data)
            print("Data updated successfully.")
        else:
            print("No data updated.")
        time.sleep(4 * 60 * 60)  # Run every 4 hours

if __name__ == "__main__":
    main()
