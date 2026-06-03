from __future__ import annotations

import argparse
import faulthandler
import json
import sys
import time
import traceback
from collections import Counter
from contextlib import ExitStack
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.backfill_tops_iextools import _download_with_refresh
from utils.iex_benchmark_adapters import normalize_hq4_message
from utils.iex_benchmark_core import ensure_repo_checkout
from utils.iex_runner_unknowns import detect_unknown_message_type, unknown_message_detail
from utils.iextools_backfill_core import choose_tops_record
from utils.parse_iex_hist_index import DEFAULT_HIST_URL, download_hist_index, load_hist_index

DEFAULT_PROGRESS_MESSAGES = 1_000_000
DEFAULT_PROGRESS_SECONDS = 30.0


def main() -> int:
    args = _parse_args()
    faulthandler.enable(all_threads=True)
    report_root = Path(args.report_root)
    setup_logging(str(report_root / f"debug_{args.day}_iextools.jsonl"))
    logger = get_logger(__name__)
    result_path = report_root / f"debug_{args.day}_iextools_{args.mode}.json"
    payload = _run_debug(args, logger)
    result_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if payload["status"] == "succeeded" else 1


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True)
    parser.add_argument("--mode", choices=("parse", "normalize"), default="parse")
    parser.add_argument("--hist-url", default=DEFAULT_HIST_URL)
    parser.add_argument("--hist-index-path", default="utils/benchmark_results/iex_hist_index.json")
    parser.add_argument("--scratch-root", default="/tmp/iex-debug-iextools")
    parser.add_argument("--repo-cache-root", default="/tmp/iex-benchmark-repos")
    parser.add_argument("--report-root", default="reports/iextools-backfill")
    parser.add_argument("--limit-messages", type=int)
    parser.add_argument("--progress-every-messages", type=int, default=DEFAULT_PROGRESS_MESSAGES)
    parser.add_argument("--progress-every-seconds", type=float, default=DEFAULT_PROGRESS_SECONDS)
    parser.add_argument("--keep-local-gz", action="store_true")
    return parser.parse_args()


def _run_debug(args: argparse.Namespace, logger) -> dict[str, Any]:
    started = time.perf_counter()
    day_root = Path(args.scratch_root) / args.day
    input_path = day_root / f"{args.day}_IEXTP1_TOPS1.6.pcap.gz"
    hist_index_path = Path(args.hist_index_path)
    repo_root = ensure_repo_checkout("hq-4", Path(args.repo_cache_root))
    _stage_input(args, input_path, hist_index_path, logger)
    sys.path.insert(0, str(repo_root))
    payload = _initial_payload(args, input_path, repo_root)
    logger.info(
        "debug start",
        extra={"event": "iextools_debug_start", "day": args.day, "detail": payload},
    )
    try:
        _scan_messages(args, input_path, payload, started, logger)
        payload["status"] = "succeeded"
    except Exception as exc:  # noqa: BLE001
        payload["status"] = "failed"
        payload["error_class"] = exc.__class__.__name__
        payload["error_message"] = str(exc)
        payload["error_traceback"] = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
        logger.exception(
            "debug failed",
            extra={"event": "iextools_debug_failed", "day": args.day, "detail": payload},
        )
    finally:
        payload["elapsed_seconds"] = round(time.perf_counter() - started, 6)
        if not args.keep_local_gz:
            input_path.unlink(missing_ok=True)
    logger.info(
        "debug complete",
        extra={"event": "iextools_debug_complete", "day": args.day, "detail": payload},
    )
    return payload


def _stage_input(args: argparse.Namespace, input_path: Path, hist_index_path: Path, logger) -> None:
    input_path.parent.mkdir(parents=True, exist_ok=True)
    if input_path.exists():
        return
    download_hist_index(args.hist_url, hist_index_path)
    record = choose_tops_record(load_hist_index(hist_index_path), args.day)
    logger.info(
        "debug download start",
        extra={
            "event": "iextools_debug_download_start",
            "day": args.day,
            "detail": {"target": str(input_path), "size_bytes": record.size_bytes},
        },
    )
    _download_with_refresh(
        record,
        input_path,
        hist_url=args.hist_url,
        hist_index_path=hist_index_path,
    )
    logger.info(
        "debug download complete",
        extra={
            "event": "iextools_debug_download_complete",
            "day": args.day,
            "detail": {"target": str(input_path), "size_bytes": input_path.stat().st_size},
        },
    )


def _initial_payload(args: argparse.Namespace, input_path: Path, repo_root: Path) -> dict[str, Any]:
    return {
        "status": "running",
        "day": args.day,
        "mode": args.mode,
        "input_path": str(input_path),
        "input_size_bytes": input_path.stat().st_size,
        "repo_root": str(repo_root),
        "limit_messages": args.limit_messages,
        "processed_messages": 0,
        "message_counts": {},
        "unknown_message_count": 0,
        "unknown_message_types": {},
        "parse_seconds": 0.0,
        "normalize_seconds": 0.0,
    }


def _scan_messages(
    args: argparse.Namespace,
    input_path: Path,
    payload: dict[str, Any],
    started: float,
    logger,
) -> None:
    from IEXTools import Parser

    counts: Counter[str] = Counter()
    unknown_types: Counter[int] = Counter()
    last_messages = 0
    last_progress = time.perf_counter()
    with ExitStack() as stack:
        iterator = iter(stack.enter_context(Parser(str(input_path))))
        while args.limit_messages is None or payload["processed_messages"] < args.limit_messages:
            parse_start = time.perf_counter()
            try:
                message = next(iterator)
            except StopIteration:
                break
            except Exception as exc:  # noqa: BLE001
                payload["parse_seconds"] += time.perf_counter() - parse_start
                _record_unknown(exc, iterator, unknown_types, payload, logger)
                raise
            payload["parse_seconds"] += time.perf_counter() - parse_start
            counts[message.__class__.__name__] += 1
            if args.mode == "normalize":
                norm_start = time.perf_counter()
                normalize_hq4_message(message)
                payload["normalize_seconds"] += time.perf_counter() - norm_start
            payload["processed_messages"] += 1
            now = time.perf_counter()
            if _should_log(payload["processed_messages"], last_messages, now - last_progress, args):
                _log_progress(payload, counts, unknown_types, now - started, logger)
                last_messages = payload["processed_messages"]
                last_progress = now
    payload["message_counts"] = dict(counts)
    payload["unknown_message_types"] = {str(k): v for k, v in sorted(unknown_types.items())}


def _record_unknown(
    exc: Exception,
    iterator: Any,
    unknown_types: Counter[int],
    payload: dict[str, Any],
    logger,
) -> None:
    unknown_type = detect_unknown_message_type(exc)
    if unknown_type is None:
        return
    detail = unknown_message_detail(
        iterator,
        unknown_type=unknown_type,
        processed_messages=payload["processed_messages"],
        parse_seconds=payload["parse_seconds"],
    )
    detail["error"] = str(exc)
    unknown_types[unknown_type] += 1
    payload["unknown_message_count"] = sum(unknown_types.values())
    payload["unknown_message_types"] = {str(k): v for k, v in sorted(unknown_types.items())}
    payload["last_unknown_message"] = detail
    logger.warning(
        "debug unknown message",
        extra={"event": "iextools_debug_unknown_message", "day": payload["day"], "detail": detail},
    )


def _should_log(processed: int, last_processed: int, elapsed: float, args: argparse.Namespace) -> bool:
    return (
        processed - last_processed >= args.progress_every_messages
        or elapsed >= args.progress_every_seconds
    )


def _log_progress(
    payload: dict[str, Any],
    counts: Counter[str],
    unknown_types: Counter[int],
    elapsed: float,
    logger,
) -> None:
    payload["message_counts"] = dict(counts)
    payload["unknown_message_types"] = {str(k): v for k, v in sorted(unknown_types.items())}
    logger.info(
        "debug progress",
        extra={
            "event": "iextools_debug_progress",
            "day": payload["day"],
            "detail": {
                "mode": payload["mode"],
                "processed_messages": payload["processed_messages"],
                "parse_seconds": round(payload["parse_seconds"], 3),
                "normalize_seconds": round(payload["normalize_seconds"], 3),
                "elapsed_seconds": round(elapsed, 3),
                "message_counts": payload["message_counts"],
                "unknown_message_types": payload["unknown_message_types"],
            },
        },
    )


if __name__ == "__main__":
    raise SystemExit(main())
