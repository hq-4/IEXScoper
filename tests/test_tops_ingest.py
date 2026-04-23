from __future__ import annotations

import csv

import polars as pl

from src.usecases.tops_ingest import (
    discover_hist_files,
    run_tops_ingest_validation,
    write_tops_spec_audit,
    _convert_csv_to_parquet,
    _is_gzip_url,
    _is_pcapng,
)
from src.framework.config import Settings


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _Session:
    def __init__(self, payload):
        self.payload = payload

    def get(self, url, timeout):
        assert url == "https://iextrading.com/api/1.0/hist"
        assert timeout == 60
        return _Response(self.payload)


def test_spec_audit_writes_json_and_csv(tmp_path):
    result = write_tops_spec_audit(tmp_path)

    assert result["rows"] > 0
    assert (tmp_path / "tops_spec_audit.json").exists()
    assert (tmp_path / "tops_spec_audit.csv").exists()


def test_discover_hist_files_filters_tops_entries():
    payload = [
        {
            "name": "20250102_IEXTP1_TOPS1.6.pcap.gz",
            "url": "https://example.test/20250102_IEXTP1_TOPS1.6.pcap.gz",
            "size": "123",
        },
        {
            "name": "20250102_IEXTP1_DEEP1.0.pcap.gz",
            "url": "https://example.test/20250102_IEXTP1_DEEP1.0.pcap.gz",
            "size": "456",
        },
    ]

    discovered = discover_hist_files(_Session(payload))

    assert list(discovered) == ["20250102"]
    assert discovered["20250102"]["size_bytes"] == 123


def test_is_gzip_url_handles_signed_google_storage_url():
    url = (
        "https://www.googleapis.com/download/storage/v1/b/iex/o/"
        "data%2Ffeeds%2F20250102%2F20250102_IEXTP1_TOPS1.6.pcap.gz"
        "?generation=1735874992388600&alt=media"
    )

    assert _is_gzip_url(url)


def test_is_pcapng_detects_magic(tmp_path):
    pcapng = tmp_path / "sample.pcap"
    pcapng.write_bytes(b"\x0a\x0d\x0d\x0a" + b"extra")

    assert _is_pcapng(pcapng)


def test_convert_csv_to_daily_parquet(tmp_path):
    day = "20250102"
    csv_path = tmp_path / f"{day}_IEXTP1_TOPS1.6_trd.csv"
    rows = [
        {
            "Packet Capture Time": "1",
            "Send Time": "2",
            "Raw Timestamp": "1735828200000000000",
            "Tick Type": "T",
            "Symbol": "AAPL",
            "Size": "100",
            "Price": "10.0",
            "Trade ID": "1",
            "Sale Condition": "REGULAR_HOURS",
        }
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    result = _convert_csv_to_parquet(csv_path, tmp_path / "parquet", day)

    parquet_path = tmp_path / "parquet" / "raw" / "tops" / "2025" / "01" / f"{day}_IEXTP1_TOPS1.6_trd.parquet"
    assert result["path"] == str(parquet_path)
    assert result["row_count"] == 1
    assert pl.read_parquet(parquet_path).height == 1


def test_dry_run_validation_writes_discovery_metrics(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "src.usecases.tops_ingest.discover_hist_files",
        lambda: {
            "20250102": {
                "day": "20250102",
                "name": "20250102_IEXTP1_TOPS1.6.pcap.gz",
                "url": "https://example.test/file.pcap.gz",
                "size_bytes": 123,
            }
        },
    )
    settings = Settings(
        iex_csv_root=str(tmp_path / "csv"),
        iex_parquet_root=str(tmp_path / "parquet"),
        display_tz="America/New_York",
        log_jsonl_path=str(tmp_path / "app.jsonl"),
        database_url=None,
    )

    code = run_tops_ingest_validation(
        settings=settings,
        work_root=str(tmp_path / "work"),
        report_root=str(tmp_path / "reports"),
        days=["20250102"],
        all_available=False,
        start_day="20250101",
        end_day=None,
        dry_run=True,
        keep_raw=False,
        parser_bin="unused",
    )

    assert code == 0
    assert "20250102" in (tmp_path / "reports" / "tops_ingest_metrics.jsonl").read_text()
