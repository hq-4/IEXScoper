from __future__ import annotations

import csv
import gzip
import json

import polars as pl

from src.usecases.tops_ingest import (
    DOWNLOAD_MAX_ATTEMPTS,
    CHECKPOINT_DIRNAME,
    StageMetric,
    discover_hist_files,
    run_tops_ingest_validation,
    write_tops_profile_report,
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


def test_convert_csv_to_daily_parquet_tolerates_nonessential_packet_capture_field(tmp_path):
    day = "20260422"
    csv_path = tmp_path / f"{day}_IEXTP1_TOPS1.6_trd.csv"
    rows = [
        {
            "Packet Capture Time": "177v886560128697088",
            "Send Time": "2",
            "Raw Timestamp": "177886560128697088",
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

    assert result["row_count"] == 1


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
        iex_work_root=str(tmp_path / "work"),
        iex_report_root=str(tmp_path / "reports"),
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
        max_workers=1,
        worker_scratch_budget_gb=180,
        scratch_reserve_gb=200,
        scratch_hard_floor_gb=150,
        limit_days=None,
        fail_threshold=5,
    )

    assert code == 0
    assert "20250102" in (tmp_path / "reports" / "tops_ingest_metrics.jsonl").read_text()


def test_validation_retries_crc_failures_and_records_state(tmp_path, monkeypatch):
    attempts = {"count": 0}

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

    def fake_download(url, target):
        attempts["count"] += 1
        target.parent.mkdir(parents=True, exist_ok=True)
        if attempts["count"] < DOWNLOAD_MAX_ATTEMPTS:
            target.write_bytes(b"not-a-gzip")
            return 123
        with gzip.open(target, "wb") as handle:
            handle.write(b"pcap")
        return 123

    monkeypatch.setattr("src.usecases.tops_ingest._download", fake_download)
    monkeypatch.setattr("src.usecases.tops_ingest._ensure_classic_pcap", lambda path: (path, {"pcap_format": "pcap"}))
    settings = Settings(
        iex_csv_root=str(tmp_path / "csv"),
        iex_parquet_root=str(tmp_path / "parquet"),
        iex_work_root=str(tmp_path / "work"),
        iex_report_root=str(tmp_path / "reports"),
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
        dry_run=False,
        keep_raw=True,
        parser_bin="unused",
        max_workers=1,
        worker_scratch_budget_gb=180,
        scratch_reserve_gb=200,
        scratch_hard_floor_gb=150,
        limit_days=None,
        fail_threshold=5,
    )

    assert code == 1
    assert attempts["count"] == DOWNLOAD_MAX_ATTEMPTS
    state = json.loads((tmp_path / "reports" / CHECKPOINT_DIRNAME / "20250102.json").read_text())
    assert state["stages"]["download"]["status"] == "succeeded"
    assert state["stages"]["download"]["detail"]["attempt"] == DOWNLOAD_MAX_ATTEMPTS
    assert state["stages"]["parse_pcap_to_csv"]["status"] == "failed"


def test_validation_resumes_completed_download_and_parse(tmp_path, monkeypatch):
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
    work = tmp_path / "work"
    reports = tmp_path / "reports"
    csv_root = tmp_path / "csv"
    parquet_root = tmp_path / "parquet"
    pcap_path = work / "pcap" / "20250102_IEXTP1_TOPS1.6.classic.pcap"
    pcap_path.parent.mkdir(parents=True, exist_ok=True)
    pcap_path.write_bytes(b"pcap")
    csv_path = csv_root / "2025" / "01" / "20250102_IEXTP1_TOPS1.6_trd.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
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

    checkpoint_dir = reports / CHECKPOINT_DIRNAME
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = {
        "day": "20250102",
        "stages": {
            "download": {"stage": "download", "status": "succeeded", "path": str(pcap_path)},
            "parse_pcap_to_csv": {"stage": "parse_pcap_to_csv", "status": "succeeded", "path": str(csv_path)},
        },
    }
    (checkpoint_dir / "20250102.json").write_text(json.dumps(checkpoint), encoding="utf-8")

    def forbidden(*args, **kwargs):
        raise AssertionError("stage should have been resumed")

    monkeypatch.setattr("src.usecases.tops_ingest._download", forbidden)
    monkeypatch.setattr("src.usecases.tops_ingest.subprocess.run", forbidden)
    settings = Settings(
        iex_csv_root=str(csv_root),
        iex_parquet_root=str(parquet_root),
        iex_work_root=str(work),
        iex_report_root=str(reports),
        display_tz="America/New_York",
        log_jsonl_path=str(tmp_path / "app.jsonl"),
        database_url=None,
    )

    code = run_tops_ingest_validation(
        settings=settings,
        work_root=str(work),
        report_root=str(reports),
        days=["20250102"],
        all_available=False,
        start_day="20250101",
        end_day=None,
        dry_run=False,
        keep_raw=True,
        parser_bin="unused",
        max_workers=1,
        worker_scratch_budget_gb=180,
        scratch_reserve_gb=200,
        scratch_hard_floor_gb=150,
        limit_days=None,
        fail_threshold=5,
    )

    assert code == 0
    assert (parquet_root / "raw" / "tops" / "2025" / "01" / "20250102_IEXTP1_TOPS1.6_trd.parquet").exists()


def test_profile_report_writes_stage_throughput_summary(tmp_path):
    report_root = tmp_path / "reports"
    checkpoint_dir = report_root / CHECKPOINT_DIRNAME
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    state = {
        "day": "20250102",
        "stages": {
            "download": {
                "stage": "download",
                "status": "succeeded",
                "elapsed_seconds": 200.0,
                "size_bytes": 40 * 1024 * 1024,
                "detail": {"downloaded_size_bytes": 10 * 1024 * 1024, "attempt": 2},
            },
            "parse_pcap_to_csv": {
                "stage": "parse_pcap_to_csv",
                "status": "succeeded",
                "elapsed_seconds": 100.0,
                "size_bytes": 5 * 1024 * 1024,
                "row_count": 2500,
                "detail": {},
            },
            "convert_csv_to_parquet": {
                "stage": "convert_csv_to_parquet",
                "status": "succeeded",
                "elapsed_seconds": 25.0,
                "size_bytes": 1024 * 1024,
                "row_count": 2500,
                "detail": {},
            },
            "aggregate_per_second": {
                "stage": "aggregate_per_second",
                "status": "succeeded",
                "elapsed_seconds": 2.0,
                "row_count": 1200,
                "detail": {"symbols": 25, "sessions": {"regular": 1200}},
            },
        },
    }
    (checkpoint_dir / "20250102.json").write_text(json.dumps(state), encoding="utf-8")

    result = write_tops_profile_report(report_root, ["20250102"])

    assert result["rows"] == 1
    payload = json.loads((report_root / "tops_ingest_profile.json").read_text(encoding="utf-8"))
    row = payload["days"][0]
    assert row["day"] == "20250102"
    assert row["download_attempt"] == 2
    assert row["download_input_mbps"] == 0.05
    assert row["parse_rows_per_second"] == 25.0
    assert row["parquet_output_mbps"] == 0.04


def test_validation_limit_days_and_summary(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "src.usecases.tops_ingest.discover_hist_files",
        lambda: {
            "20250102": {"day": "20250102", "name": "a", "url": "https://example.test/a", "size_bytes": 1},
            "20250103": {"day": "20250103", "name": "b", "url": "https://example.test/b", "size_bytes": 1},
            "20250104": {"day": "20250104", "name": "c", "url": "https://example.test/c", "size_bytes": 1},
        },
    )
    called: list[str] = []

    def fake_run_day(day, info, settings, work, metrics, state, dry_run, keep_raw, parser_bin):
        called.append(day)
        state.record(
            day,
            StageMetric(
                day=day,
                stage="aggregate_per_second",
                status="succeeded",
                started_at="now",
                finished_at="now",
                elapsed_seconds=0.1,
                row_count=1,
            ),
        )
        return 0

    monkeypatch.setattr("src.usecases.tops_ingest._run_day_pipeline", fake_run_day)
    monkeypatch.setattr("src.usecases.tops_ingest._free_space_gb", lambda path: 1000.0)
    settings = Settings(
        iex_csv_root=str(tmp_path / "csv"),
        iex_parquet_root=str(tmp_path / "parquet"),
        iex_work_root=str(tmp_path / "work"),
        iex_report_root=str(tmp_path / "reports"),
        display_tz="America/New_York",
        log_jsonl_path=str(tmp_path / "app.jsonl"),
        database_url=None,
    )

    code = run_tops_ingest_validation(
        settings=settings,
        work_root=str(tmp_path / "work"),
        report_root=str(tmp_path / "reports"),
        days=None,
        all_available=True,
        start_day="20250101",
        end_day=None,
        dry_run=False,
        keep_raw=False,
        parser_bin="unused",
        max_workers=1,
        worker_scratch_budget_gb=180,
        scratch_reserve_gb=200,
        scratch_hard_floor_gb=150,
        limit_days=2,
        fail_threshold=5,
    )

    assert code == 0
    assert called == ["20250102", "20250103"]
    summary = json.loads((tmp_path / "reports" / "tops_backfill_run_summary.json").read_text(encoding="utf-8"))
    assert summary["selected_day_count"] == 2
    assert summary["attempted_day_count"] == 2
    assert summary["succeeded_day_count"] == 2


def test_validation_skips_completed_days(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "src.usecases.tops_ingest.discover_hist_files",
        lambda: {
            "20250102": {"day": "20250102", "name": "a", "url": "https://example.test/a", "size_bytes": 1},
            "20250103": {"day": "20250103", "name": "b", "url": "https://example.test/b", "size_bytes": 1},
        },
    )
    reports = tmp_path / "reports"
    checkpoint_dir = reports / CHECKPOINT_DIRNAME
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = {
        "day": "20250102",
        "stages": {"aggregate_per_second": {"stage": "aggregate_per_second", "status": "succeeded"}},
    }
    (checkpoint_dir / "20250102.json").write_text(json.dumps(checkpoint), encoding="utf-8")
    called: list[str] = []

    def fake_run_day(day, info, settings, work, metrics, state, dry_run, keep_raw, parser_bin):
        called.append(day)
        state.record(
            day,
            StageMetric(
                day=day,
                stage="aggregate_per_second",
                status="succeeded",
                started_at="now",
                finished_at="now",
                elapsed_seconds=0.1,
                row_count=1,
            ),
        )
        return 0

    monkeypatch.setattr("src.usecases.tops_ingest._run_day_pipeline", fake_run_day)
    monkeypatch.setattr("src.usecases.tops_ingest._free_space_gb", lambda path: 1000.0)
    settings = Settings(
        iex_csv_root=str(tmp_path / "csv"),
        iex_parquet_root=str(tmp_path / "parquet"),
        iex_work_root=str(tmp_path / "work"),
        iex_report_root=str(reports),
        display_tz="America/New_York",
        log_jsonl_path=str(tmp_path / "app.jsonl"),
        database_url=None,
    )

    code = run_tops_ingest_validation(
        settings=settings,
        work_root=str(tmp_path / "work"),
        report_root=str(reports),
        days=["20250102", "20250103"],
        all_available=False,
        start_day="20250101",
        end_day=None,
        dry_run=False,
        keep_raw=False,
        parser_bin="unused",
        max_workers=1,
        worker_scratch_budget_gb=180,
        scratch_reserve_gb=200,
        scratch_hard_floor_gb=150,
        limit_days=None,
        fail_threshold=5,
    )

    assert code == 0
    assert called == ["20250103"]
    summary = json.loads((reports / "tops_backfill_run_summary.json").read_text(encoding="utf-8"))
    assert summary["skipped_day_count"] == 1
    assert summary["attempted_day_count"] == 1


def test_validation_blocks_on_scratch_floor(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "src.usecases.tops_ingest.discover_hist_files",
        lambda: {
            "20250102": {"day": "20250102", "name": "a", "url": "https://example.test/a", "size_bytes": 1},
        },
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("worker should not launch when scratch floor is breached")

    monkeypatch.setattr("src.usecases.tops_ingest._run_day_pipeline", forbidden)
    monkeypatch.setattr("src.usecases.tops_ingest._free_space_gb", lambda path: 100.0)
    settings = Settings(
        iex_csv_root=str(tmp_path / "csv"),
        iex_parquet_root=str(tmp_path / "parquet"),
        iex_work_root=str(tmp_path / "work"),
        iex_report_root=str(tmp_path / "reports"),
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
        dry_run=False,
        keep_raw=False,
        parser_bin="unused",
        max_workers=1,
        worker_scratch_budget_gb=180,
        scratch_reserve_gb=200,
        scratch_hard_floor_gb=150,
        limit_days=None,
        fail_threshold=5,
    )

    assert code == 1
    summary = json.loads((tmp_path / "reports" / "tops_backfill_run_summary.json").read_text(encoding="utf-8"))
    assert summary["stop_reason"] == "blocked_on_scratch"
    assert summary["attempted_day_count"] == 0


def test_validation_stops_launching_on_fail_threshold(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "src.usecases.tops_ingest.discover_hist_files",
        lambda: {
            "20250102": {"day": "20250102", "name": "a", "url": "https://example.test/a", "size_bytes": 1},
            "20250103": {"day": "20250103", "name": "b", "url": "https://example.test/b", "size_bytes": 1},
        },
    )
    called: list[str] = []

    def fake_run_day(day, info, settings, work, metrics, state, dry_run, keep_raw, parser_bin):
        called.append(day)
        return 1

    monkeypatch.setattr("src.usecases.tops_ingest._run_day_pipeline", fake_run_day)
    monkeypatch.setattr("src.usecases.tops_ingest._free_space_gb", lambda path: 1000.0)
    settings = Settings(
        iex_csv_root=str(tmp_path / "csv"),
        iex_parquet_root=str(tmp_path / "parquet"),
        iex_work_root=str(tmp_path / "work"),
        iex_report_root=str(tmp_path / "reports"),
        display_tz="America/New_York",
        log_jsonl_path=str(tmp_path / "app.jsonl"),
        database_url=None,
    )

    code = run_tops_ingest_validation(
        settings=settings,
        work_root=str(tmp_path / "work"),
        report_root=str(tmp_path / "reports"),
        days=["20250102", "20250103"],
        all_available=False,
        start_day="20250101",
        end_day=None,
        dry_run=False,
        keep_raw=False,
        parser_bin="unused",
        max_workers=1,
        worker_scratch_budget_gb=180,
        scratch_reserve_gb=200,
        scratch_hard_floor_gb=150,
        limit_days=None,
        fail_threshold=1,
    )

    assert code == 1
    assert called == ["20250102"]
    summary = json.loads((tmp_path / "reports" / "tops_backfill_run_summary.json").read_text(encoding="utf-8"))
    assert summary["stop_reason"] == "fail_threshold_reached"
    assert summary["attempted_day_count"] == 1
