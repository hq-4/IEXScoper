from __future__ import annotations

import json
from pathlib import Path

import utils.parse_iex_hist_index as hist
from utils.parse_iex_hist_index import (
    download_hist_index,
    latest_records,
    load_hist_index,
    summarize_hist_index,
)


def test_load_and_summarize_hist_index(tmp_path: Path) -> None:
    sample = {
        "20240102": [
            {
                "link": "https://example.test/tops",
                "date": "20240102",
                "feed": "TOPS",
                "version": "1.6",
                "protocol": "IEXTP1",
                "size": "100",
            },
            {
                "link": "https://example.test/deep",
                "date": "20240102",
                "feed": "DEEP",
                "version": "1.0",
                "protocol": "IEXTP1",
                "size": "200",
            },
        ],
        "20240103": [
            {
                "link": "https://example.test/tops2",
                "date": "20240103",
                "feed": "TOPS",
                "version": "1.6",
                "protocol": "IEXTP1",
                "size": "300",
            }
        ],
    }
    path = tmp_path / "hist.json"
    path.write_text(json.dumps(sample), encoding="utf-8")

    records = load_hist_index(path)
    summary = summarize_hist_index(records, feed="TOPS")

    assert summary["days"] == 2
    assert summary["files"] == 2
    assert summary["first_day"] == "20240102"
    assert summary["last_day"] == "20240103"
    assert summary["total_size_bytes"] == 400
    assert summary["feeds"] == {"TOPS": 2}
    assert summary["versions"] == {"1.6": 2}


def test_latest_records_filters_and_orders(tmp_path: Path) -> None:
    sample = {
        "20240102": [
            {
                "link": "https://example.test/old",
                "date": "20240102",
                "feed": "TOPS",
                "version": "1.6",
                "protocol": "IEXTP1",
                "size": "100",
            }
        ],
        "20240103": [
            {
                "link": "https://example.test/new",
                "date": "20240103",
                "feed": "TOPS",
                "version": "1.6",
                "protocol": "IEXTP1",
                "size": "200",
            }
        ],
    }
    path = tmp_path / "hist.json"
    path.write_text(json.dumps(sample), encoding="utf-8")

    records = load_hist_index(path)
    rows = latest_records(records, limit=1, feed="TOPS")

    assert len(rows) == 1
    assert rows[0]["date"] == "20240103"
    assert rows[0]["size_bytes"] == 200


def test_download_hist_index_replaces_target_atomically(monkeypatch, tmp_path: Path) -> None:
    class FakeResponse:
        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"ok": []}'

    monkeypatch.setattr(hist, "urlopen", lambda _url: FakeResponse())
    target = tmp_path / "hist.json"

    assert download_hist_index("https://example.test/hist", target) == target
    assert target.read_text(encoding="utf-8") == '{"ok": []}'
    assert not target.with_name("hist.json.tmp").exists()
