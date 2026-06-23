from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import utils.backfill_tops_iextools as backfill
from utils.backfill_tops_iextools import (
    DEFAULT_UNKNOWN_MESSAGE_CONSECUTIVE_THRESHOLD,
    DEFAULT_UNKNOWN_MESSAGE_THRESHOLD,
    _cleanup,
    _cleanup_terminal_failure,
    _local_tops_input_path,
    _refresh_hist_records,
)


class CaptureLogger:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def info(self, _message: str, *, extra: dict[str, object]) -> None:
        self.events.append(extra)

    def warning(self, _message: str, *, extra: dict[str, object]) -> None:
        self.events.append(extra)


def test_backfill_threshold_defaults_match_hardened_runner_defaults() -> None:
    assert DEFAULT_UNKNOWN_MESSAGE_THRESHOLD == 10_000
    assert DEFAULT_UNKNOWN_MESSAGE_CONSECUTIVE_THRESHOLD == 10_000


def test_cleanup_removes_worker_scratch_and_logs_event(tmp_path: Path) -> None:
    worker_root = tmp_path / "worker-1"
    worker_root.mkdir()
    (worker_root / "raw.pcap.gz").write_bytes(b"raw")
    nested = worker_root / "nested"
    nested.mkdir()
    (nested / "artifact.txt").write_text("artifact", encoding="utf-8")
    logger = CaptureLogger()

    deleted = _cleanup(
        worker_root,
        logger=logger,
        day="20260105",
        attempt=2,
        reason="retryable_failure",
    )

    assert sorted(Path(path).name for path in deleted) == ["nested", "raw.pcap.gz"]
    assert not any(worker_root.iterdir())
    assert logger.events[-1]["event"] == "iextools_backfill_scratch_cleanup"


def test_terminal_failure_deletes_failed_gz_by_default(tmp_path: Path) -> None:
    local_gz, local_main, local_quote = _write_failure_artifacts(tmp_path)
    logger = CaptureLogger()

    _cleanup_terminal_failure(
        local_gz=local_gz,
        local_main=local_main,
        local_quote=local_quote,
        keep_failed_gz=False,
        logger=logger,
        day="20260105",
        attempt=1,
        reason="terminal_runner_failure",
        error="runner exited with code 139",
    )

    assert not local_gz.exists()
    assert not local_main.exists()
    assert not local_quote.exists()
    events = [event["event"] for event in logger.events]
    assert "iextools_backfill_scratch_cleanup" in events
    assert "iextools_backfill_failed_gz_deleted" in events


def test_terminal_failure_can_retain_failed_gz(tmp_path: Path) -> None:
    local_gz, local_main, local_quote = _write_failure_artifacts(tmp_path)
    logger = CaptureLogger()

    _cleanup_terminal_failure(
        local_gz=local_gz,
        local_main=local_main,
        local_quote=local_quote,
        keep_failed_gz=True,
        logger=logger,
        day="20260105",
        attempt=1,
        reason="terminal_runner_failure",
        error="ProtocolException: Unknown message type",
    )

    assert local_gz.exists()
    assert not local_main.exists()
    assert not local_quote.exists()
    events = [event["event"] for event in logger.events]
    assert "iextools_backfill_failed_gz_retained" in events
    assert "iextools_backfill_failed_gz_deleted" not in events


def test_local_tops_input_path_preserves_hist_version(tmp_path: Path) -> None:
    path = _local_tops_input_path(tmp_path, "20170103", "1.5")

    assert path == tmp_path / "20170103_IEXTP1_TOPS1.5.pcap.gz"


def test_refresh_hist_records_serializes_shared_index_writes(monkeypatch, tmp_path: Path) -> None:
    hist_path = tmp_path / "hist.json"
    sample = {
        "20170103": [
            {
                "link": "https://example.test/tops",
                "date": "20170103",
                "feed": "TOPS",
                "version": "1.5",
                "protocol": "IEXTP1",
                "size": "100",
            }
        ]
    }

    def slow_direct_download(_url: str, output_path: Path) -> Path:
        output_path.write_text('{"20170103": [', encoding="utf-8")
        time.sleep(0.01)
        output_path.write_text(json.dumps(sample), encoding="utf-8")
        return output_path

    monkeypatch.setattr(backfill, "download_hist_index", slow_direct_download)
    errors: list[Exception] = []

    def worker() -> None:
        try:
            records = _refresh_hist_records("https://example.test/hist", hist_path)
            assert records["20170103"][0].version == "1.5"
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []


def _write_failure_artifacts(tmp_path: Path) -> tuple[Path, Path, Path]:
    local_gz = tmp_path / "20260105_IEXTP1_TOPS1.6.pcap.gz"
    local_main = tmp_path / "20260105_IEXTP1_TOPS1.6.parquet"
    local_quote = tmp_path / "20260105_IEXTP1_TOPS1.6_QuoteUpdate.parquet"
    local_gz.write_bytes(b"raw")
    local_main.write_bytes(b"main")
    local_quote.write_bytes(b"quote")
    return local_gz, local_main, local_quote
