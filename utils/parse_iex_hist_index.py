from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import urlopen

DEFAULT_HIST_URL = "https://iextrading.com/api/1.0/hist"
DEFAULT_DOWNLOAD_PATH = Path("utils/benchmark_results/iex_hist_index.json")


@dataclass(frozen=True)
class HistFileRecord:
    date: str
    feed: str
    protocol: str
    version: str
    size_bytes: int
    link: str


def download_hist_index(url: str, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with urlopen(url) as response:  # noqa: S310
        payload = response.read()
    output_path.write_bytes(payload)
    return output_path


def load_hist_index(path: Path) -> dict[str, list[HistFileRecord]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {
        day: [
            HistFileRecord(
                date=item["date"],
                feed=item["feed"],
                protocol=item["protocol"],
                version=item["version"],
                size_bytes=int(item["size"]),
                link=item["link"],
            )
            for item in rows
        ]
        for day, rows in raw.items()
    }


def summarize_hist_index(
    records_by_day: dict[str, list[HistFileRecord]], *, feed: str | None = None
) -> dict[str, Any]:
    selected: dict[str, list[HistFileRecord]] = {}
    for day, rows in records_by_day.items():
        matching = [row for row in rows if feed is None or row.feed == feed]
        if matching:
            selected[day] = matching

    flattened = [row for rows in selected.values() for row in rows]
    feed_counts = Counter(row.feed for row in flattened)
    protocol_counts = Counter(row.protocol for row in flattened)
    version_counts = Counter(row.version for row in flattened)

    return {
        "days": len(selected),
        "files": len(flattened),
        "first_day": min(selected) if selected else None,
        "last_day": max(selected) if selected else None,
        "total_size_bytes": sum(row.size_bytes for row in flattened),
        "feeds": dict(sorted(feed_counts.items())),
        "protocols": dict(sorted(protocol_counts.items())),
        "versions": dict(sorted(version_counts.items())),
    }


def latest_records(
    records_by_day: dict[str, list[HistFileRecord]], *, limit: int = 5, feed: str | None = None
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in sorted(records_by_day.keys(), reverse=True):
        for record in records_by_day[day]:
            if feed is not None and record.feed != feed:
                continue
            rows.append(
                {
                    "date": record.date,
                    "feed": record.feed,
                    "protocol": record.protocol,
                    "version": record.version,
                    "size_bytes": record.size_bytes,
                    "link": record.link,
                }
            )
            if len(rows) >= limit:
                return rows
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_HIST_URL)
    parser.add_argument("--download-path", default=str(DEFAULT_DOWNLOAD_PATH))
    parser.add_argument("--input-path")
    parser.add_argument("--feed")
    parser.add_argument("--latest", type=int, default=5)
    parser.add_argument("--download", action="store_true")
    args = parser.parse_args()

    download_path = Path(args.download_path)
    input_path = Path(args.input_path) if args.input_path else download_path

    if args.download or not input_path.exists():
        input_path = download_hist_index(args.url, download_path)

    records_by_day = load_hist_index(input_path)
    summary = summarize_hist_index(records_by_day, feed=args.feed)
    payload = {
        "input_path": str(input_path),
        "feed_filter": args.feed,
        "summary": summary,
        "latest_records": latest_records(records_by_day, limit=args.latest, feed=args.feed),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
