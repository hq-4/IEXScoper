from __future__ import annotations

import argparse
import concurrent.futures
import shutil
import subprocess
import threading
from collections import deque
from collections.abc import MutableSequence
from datetime import UTC, datetime
from pathlib import Path
from queue import Empty, Queue

import requests

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.iex_benchmark_core import ensure_repo_checkout
from utils.iextools_backfill_core import (
    append_jsonl,
    choose_tops_record,
    publish_parquet_pair,
    select_missing_tops_days,
)
from utils.iextools_backfill_recovery import (
    TAIL_LINE_COUNT,
    is_retryable_runner_failure,
    load_runner_payload,
    summarize_runner_failure,
)
from utils.iextools_backfill_reporting import classify_failure
from utils.parse_iex_hist_index import DEFAULT_HIST_URL, download_hist_index, load_hist_index


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hist-url", default=DEFAULT_HIST_URL)
    parser.add_argument("--hist-index-path", default="utils/benchmark_results/iex_hist_index.json")
    parser.add_argument("--scratch-root", default="/tmp/iex-tops-backfill")
    parser.add_argument("--parquet-root", default="/media/tn/pq")
    parser.add_argument("--report-root", default="reports/iextools-backfill")
    parser.add_argument("--repo-cache-root", default="/tmp/iex-benchmark-repos")
    parser.add_argument("--start-day", default="20250501")
    parser.add_argument("--end-day")
    parser.add_argument("--limit-days", type=int)
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--max-day-attempts", type=int, default=3)
    parser.add_argument("--compression", default="snappy")
    parser.add_argument("--unknown-message-threshold-count", type=int, default=100)
    parser.add_argument("--unknown-message-threshold-consecutive", type=int, default=10)
    parser.add_argument("--keep-local-parquet", action="store_true")
    parser.add_argument("--download-index", action="store_true")
    args = parser.parse_args()

    scratch_root = Path(args.scratch_root)
    parquet_root = Path(args.parquet_root)
    report_root = Path(args.report_root)
    hist_index_path = Path(args.hist_index_path)
    setup_logging(str(report_root / "iextools_backfill.jsonl"))
    logger = get_logger(__name__)

    if args.download_index or not hist_index_path.exists():
        download_hist_index(args.hist_url, hist_index_path)
    records_by_day = _refresh_hist_records(args.hist_url, hist_index_path)
    repo_root = ensure_repo_checkout("hq-4", Path(args.repo_cache_root))
    days = select_missing_tops_days(
        records_by_day,
        parquet_root,
        start_day=args.start_day,
        end_day=args.end_day,
        limit_days=args.limit_days,
    )
    logger.info(
        "backfill start",
        extra={
            "event": "iextools_backfill_start",
            "detail": {
                "day_count": len(days),
                "start_day": args.start_day,
                "end_day": args.end_day,
                "scratch_root": str(scratch_root),
                "parquet_root": str(parquet_root),
                "max_workers": args.max_workers,
            },
        },
    )
    results_path = report_root / "iextools_backfill_results.jsonl"
    queue: Queue[str] = Queue()
    for day in days:
        queue.put(day)
    failures = 0
    failures_lock = threading.Lock()

    def worker(worker_id: int) -> None:
        nonlocal failures
        worker_root = scratch_root / f"worker-{worker_id}"
        worker_root.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                day = queue.get_nowait()
            except Empty:
                return
            started_at = _utc_now()
            try:
                latest_records = _refresh_hist_records(args.hist_url, hist_index_path)
                record = choose_tops_record(latest_records, day)
                logger.info(
                    "day claimed",
                    extra={
                        "event": "iextools_backfill_day_claimed",
                        "day": day,
                        "detail": {
                            "worker_id": worker_id,
                            "link": record.link,
                            "size_bytes": record.size_bytes,
                        },
                    },
                )
                result = _process_day(
                    day=day,
                    record=record,
                    worker_root=worker_root,
                    parquet_root=parquet_root,
                    repo_root=repo_root,
                    report_root=report_root,
                    compression=args.compression,
                    max_day_attempts=args.max_day_attempts,
                    unknown_message_threshold_count=args.unknown_message_threshold_count,
                    unknown_message_threshold_consecutive=args.unknown_message_threshold_consecutive,
                    keep_local_parquet=args.keep_local_parquet,
                    hist_url=args.hist_url,
                    hist_index_path=hist_index_path,
                    logger=logger,
                )
                payload = {
                    "day": day,
                    "started_at": started_at,
                    "finished_at": _utc_now(),
                    **result,
                }
                append_jsonl(results_path, payload)
                logger.info(
                    "day complete",
                    extra={
                        "event": "iextools_backfill_day_complete",
                        "day": day,
                        "detail": payload,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                with failures_lock:
                    failures += 1
                payload = {
                    "day": day,
                    "started_at": started_at,
                    "finished_at": _utc_now(),
                    "status": "failed",
                    "error": f"{exc.__class__.__name__}: {exc}",
                }
                payload["failure_class"] = classify_failure(payload)
                append_jsonl(results_path, payload)
                logger.exception(
                    "day failed", extra={"event": "iextools_backfill_day_failed", "day": day}
                )
            finally:
                queue.task_done()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
        futures = [
            executor.submit(worker, worker_id) for worker_id in range(1, args.max_workers + 1)
        ]
        for future in futures:
            future.result()
    return 1 if failures else 0


def _process_day(
    *,
    day: str,
    record,
    worker_root: Path,
    parquet_root: Path,
    repo_root: Path,
    report_root: Path,
    compression: str,
    max_day_attempts: int,
    unknown_message_threshold_count: int,
    unknown_message_threshold_consecutive: int,
    keep_local_parquet: bool,
    hist_url: str,
    hist_index_path: Path,
    logger,
) -> dict[str, object]:
    local_gz = worker_root / f"{day}_IEXTP1_TOPS1.6.pcap.gz"
    local_main = worker_root / f"{day}_IEXTP1_TOPS1.6.parquet"
    local_quote = worker_root / f"{day}_IEXTP1_TOPS1.6_QuoteUpdate.parquet"
    runner_result = worker_root / f"{day}_hq-4_result.json"
    runner_log = report_root / "runner-logs" / f"{day}_hq-4_runner.jsonl"
    attempts = max(1, max_day_attempts)
    for attempt in range(1, attempts + 1):
        _cleanup(worker_root)
        latest_records = _refresh_hist_records(hist_url, hist_index_path)
        fresh_record = choose_tops_record(latest_records, day)
        _download_with_refresh(
            fresh_record,
            local_gz,
            hist_url=hist_url,
            hist_index_path=hist_index_path,
        )
        logger.info(
            "day start",
            extra={
                "event": "iextools_backfill_day_start",
                "day": day,
                "detail": {
                    "attempt": attempt,
                    "max_day_attempts": attempts,
                    "input_path": str(local_gz),
                    "main_output": str(local_main),
                    "quote_output": str(local_quote),
                    "runner_log": str(runner_log),
                },
            },
        )
        command = [
            "uv",
            "run",
            "python",
            "utils/iex_parser_repo_runner.py",
            "--repo",
            "hq-4",
            "--repo-path",
            str(repo_root),
            "--input-path",
            str(local_gz),
            "--main-output",
            str(local_main),
            "--quote-output",
            str(local_quote),
            "--compression",
            compression,
            "--result-path",
            str(runner_result),
            "--log-jsonl",
            str(runner_log),
            "--unknown-message-threshold-count",
            str(unknown_message_threshold_count),
            "--unknown-message-threshold-consecutive",
            str(unknown_message_threshold_consecutive),
        ]
        completed = subprocess.Popen(
            command,
            cwd=Path(__file__).resolve().parents[1],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        stdout_lines: deque[str] = deque(maxlen=TAIL_LINE_COUNT)
        stderr_lines: deque[str] = deque(maxlen=TAIL_LINE_COUNT)
        threads = [
            threading.Thread(
                target=_pump_stream,
                args=(completed.stdout, stdout_lines, logger, day, "stdout"),
                daemon=True,
            ),
            threading.Thread(
                target=_pump_stream,
                args=(completed.stderr, stderr_lines, logger, day, "stderr"),
                daemon=True,
            ),
        ]
        for thread in threads:
            thread.start()
        return_code = completed.wait()
        for thread in threads:
            thread.join()
        runner_payload = load_runner_payload(runner_result)
        if return_code != 0:
            error_text = summarize_runner_failure(
                runner_payload=runner_payload,
                stdout_lines=stdout_lines,
                stderr_lines=stderr_lines,
                return_code=return_code,
            )
            retryable = attempt < attempts and is_retryable_runner_failure(error_text)
            logger.warning(
                "day attempt failed",
                extra={
                    "event": "iextools_backfill_day_attempt_failed",
                    "day": day,
                    "detail": {
                        "attempt": attempt,
                        "max_day_attempts": attempts,
                        "retryable": retryable,
                        "error": error_text,
                    },
                },
            )
            if retryable:
                continue
            raise RuntimeError(error_text)
        if runner_payload is None:
            raise RuntimeError("runner result payload missing after successful exit")
        publish = publish_parquet_pair(
            local_main, local_quote, parquet_root, day, publish_token=f"worker-{worker_root.name}"
        )
        local_gz.unlink(missing_ok=True)
        if not keep_local_parquet:
            local_main.unlink(missing_ok=True)
            local_quote.unlink(missing_ok=True)
        runner_result.unlink(missing_ok=True)
        return {
            "status": "succeeded",
            "attempt_count": attempt,
            "download_size_bytes": (
                local_gz.stat().st_size if local_gz.exists() else fresh_record.size_bytes
            ),
            "message_counts": runner_payload.get("message_counts"),
            "processed_messages": runner_payload.get("processed_messages"),
            "parse_seconds": runner_payload.get("parse_seconds"),
            "normalize_seconds": runner_payload.get("normalize_seconds"),
            "write_seconds": runner_payload.get("write_seconds"),
            "publish": publish,
        }
    raise RuntimeError(f"exhausted day attempts for {day}")


def _download(url: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with target.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)


def _cleanup(worker_root: Path) -> None:
    for path in worker_root.iterdir():
        if path.is_file():
            path.unlink(missing_ok=True)
        elif path.is_dir():
            shutil.rmtree(path, ignore_errors=True)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _refresh_hist_records(hist_url: str, hist_index_path: Path):
    download_hist_index(hist_url, hist_index_path)
    return load_hist_index(hist_index_path)


def _download_with_refresh(
    record,
    target: Path,
    *,
    hist_url: str,
    hist_index_path: Path,
) -> None:
    try:
        _download(record.link, target)
        return
    except requests.RequestException:
        target.unlink(missing_ok=True)
        latest_records = _refresh_hist_records(hist_url, hist_index_path)
        fresh_record = choose_tops_record(latest_records, record.date)
        _download(fresh_record.link, target)


def _pump_stream(
    stream,
    sink: MutableSequence[str],
    logger,
    day: str,
    stream_name: str,
) -> None:
    if stream is None:
        return
    for line in stream:
        sink.append(line)
        text = line.rstrip()
        if not text:
            continue
        logger.info(
            "%s",
            text,
            extra={
                "event": f"iextools_backfill_child_{stream_name}",
                "day": day,
                "detail": {"stream": stream_name},
            },
        )


if __name__ == "__main__":
    raise SystemExit(main())
