from __future__ import annotations

import argparse
import faulthandler
import json
import logging
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
from utils.iex_benchmark_adapters import normalize_hq4_message, normalize_rob_message
from utils.iex_runner_unknowns import (
    UnknownMessageTracker,
    detect_unknown_message_type,
    unknown_message_detail,
)
from utils.iex_runner_outputs import StreamWriters, artifact

DEFAULT_PROGRESS_MESSAGES = 1_000_000
DEFAULT_PROGRESS_SECONDS = 30.0
DEFAULT_UNKNOWN_MESSAGE_THRESHOLD_COUNT = 10_000
DEFAULT_UNKNOWN_MESSAGE_THRESHOLD_CONSECUTIVE = 10_000


def main() -> int:
    faulthandler.enable(all_threads=True)
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, choices=("rob-blackbourn", "hq-4"))
    parser.add_argument("--repo-path", required=True)
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--main-output", required=True)
    parser.add_argument("--quote-output", required=True)
    parser.add_argument("--compression", required=True)
    parser.add_argument("--result-path", required=True)
    parser.add_argument("--log-jsonl", required=True)
    parser.add_argument("--progress-every-messages", type=int, default=DEFAULT_PROGRESS_MESSAGES)
    parser.add_argument("--progress-every-seconds", type=float, default=DEFAULT_PROGRESS_SECONDS)
    parser.add_argument(
        "--unknown-message-threshold-count",
        type=int,
        default=DEFAULT_UNKNOWN_MESSAGE_THRESHOLD_COUNT,
    )
    parser.add_argument(
        "--unknown-message-threshold-consecutive",
        type=int,
        default=DEFAULT_UNKNOWN_MESSAGE_THRESHOLD_CONSECUTIVE,
    )
    args = parser.parse_args()

    repo_path = Path(args.repo_path)
    sys.path.insert(0, str(repo_path))
    setup_logging(args.log_jsonl)
    logger = get_logger(__name__)
    result = {
        "status": "succeeded",
        "repo_key": args.repo,
        "input_path": args.input_path,
        "compression": args.compression,
        "parse_seconds": 0.0,
        "normalize_seconds": 0.0,
        "write_seconds": 0.0,
        "message_counts": {},
        "unknown_message_count": 0,
        "unknown_message_types": {},
        "unknown_message_threshold_count": args.unknown_message_threshold_count,
        "unknown_message_threshold_consecutive": args.unknown_message_threshold_consecutive,
        "preprocessing": {"input_mode": "pcap.gz_direct", "steps": []},
        "runner_log_jsonl": args.log_jsonl,
    }
    quarantine_path = Path(args.result_path).with_name(
        f"{Path(args.result_path).stem}_unknown_messages.jsonl"
    )
    result["unknown_message_quarantine_path"] = str(quarantine_path)
    writers = StreamWriters(
        Path(args.main_output), Path(args.quote_output), args.compression, logger
    )
    unknowns = UnknownMessageTracker(
        quarantine_path=quarantine_path,
        threshold_count=args.unknown_message_threshold_count,
        threshold_consecutive=args.unknown_message_threshold_consecutive,
    )
    counts: Counter[str] = Counter()
    processed_messages = 0
    last_progress_messages = 0
    last_progress_time = time.perf_counter()
    started_at = time.perf_counter()
    logger.info(
        "runner start",
        extra={
            "event": "iex_benchmark_runner_start",
            "detail": {
                "repo_key": args.repo,
                "input_path": args.input_path,
                "main_output": args.main_output,
                "quote_output": args.quote_output,
                "compression": args.compression,
            },
        },
    )
    try:
        with ExitStack() as stack:
            iterator = _open_iterator(args.repo, args.input_path, stack)
            while True:
                parse_start = time.perf_counter()
                try:
                    message = next(iterator)
                except StopIteration:
                    break
                except Exception as exc:  # noqa: BLE001
                    result["parse_seconds"] += time.perf_counter() - parse_start
                    if not _quarantine_unknown_message(
                        exc, iterator, unknowns, processed_messages, result["parse_seconds"], logger
                    ):
                        raise
                    result["unknown_message_count"] = unknowns.count
                    result["unknown_message_types"] = {
                        str(msg_type): count for msg_type, count in sorted(unknowns.types.items())
                    }
                    threshold_error = unknowns.threshold_error()
                    if threshold_error is not None:
                        raise RuntimeError(threshold_error) from exc
                    continue
                result["parse_seconds"] += time.perf_counter() - parse_start
                norm_start = time.perf_counter()
                target, row = _normalize(args.repo, message)
                result["normalize_seconds"] += time.perf_counter() - norm_start
                writers.add(target, row)
                counts[row["type"]] += 1
                processed_messages += 1
                unknowns.reset_consecutive()
                now = time.perf_counter()
                if _should_log_progress(
                    processed_messages,
                    last_progress_messages,
                    now - last_progress_time,
                    args.progress_every_messages,
                    args.progress_every_seconds,
                ):
                    logger.info(
                        "progress",
                        extra={
                            "event": "iex_benchmark_progress",
                            "detail": {
                                "repo_key": args.repo,
                                "processed_messages": processed_messages,
                                "main_rows": writers.main_count,
                                "quote_rows": writers.quote_count,
                                "main_buffer": len(writers.main_rows),
                                "quote_buffer": len(writers.quote_rows),
                                "parse_seconds": round(result["parse_seconds"], 3),
                                "normalize_seconds": round(result["normalize_seconds"], 3),
                                "write_seconds": round(writers.write_seconds, 3),
                                "elapsed_seconds": round(now - started_at, 3),
                            },
                        },
                    )
                    last_progress_messages = processed_messages
                    last_progress_time = now
    except Exception as exc:  # noqa: BLE001
        result["status"] = "failed"
        result["error_class"] = exc.__class__.__name__
        result["error_message"] = str(exc)
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["error_traceback"] = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
        logger.exception(
            "runner failed",
            extra={
                "event": "iex_benchmark_runner_failed",
                "detail": {
                    "error_class": result["error_class"],
                    "error_message": result["error_message"],
                    "processed_messages": processed_messages,
                    "main_rows": writers.main_count,
                    "quote_rows": writers.quote_count,
                    "traceback": result["error_traceback"],
                },
            },
        )
    finally:
        writers.close()

    result["message_counts"] = dict(counts)
    result["processed_messages"] = processed_messages
    result["main_rows"] = writers.main_count
    result["quote_rows"] = writers.quote_count
    result["unknown_message_count"] = unknowns.count
    result["unknown_message_types"] = {
        str(msg_type): count for msg_type, count in sorted(unknowns.types.items())
    }
    result["write_seconds"] = round(writers.write_seconds, 6)
    result["main_output"] = artifact(Path(args.main_output))
    result["quote_output"] = artifact(Path(args.quote_output))
    logger.info(
        "runner complete",
        extra={
            "event": "iex_benchmark_runner_complete",
            "detail": {
                "repo_key": args.repo,
                "status": result["status"],
                "processed_messages": processed_messages,
                "main_rows": result["main_rows"],
                "quote_rows": result["quote_rows"],
                "parse_seconds": round(result["parse_seconds"], 3),
                "normalize_seconds": round(result["normalize_seconds"], 3),
                "write_seconds": round(result["write_seconds"], 3),
            },
        },
    )
    Path(args.result_path).write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0 if result["status"] == "succeeded" else 1


def _open_iterator(repo: str, input_path: str, stack: ExitStack):
    if repo == "rob-blackbourn":
        from iex_parser import Parser, TOPS_1_6

        return iter(stack.enter_context(Parser(input_path, TOPS_1_6)))
    from IEXTools import Parser

    return iter(stack.enter_context(Parser(input_path)))


def _normalize(repo: str, message: Any) -> tuple[str, dict[str, Any]]:
    if repo == "rob-blackbourn":
        return normalize_rob_message(message)
    return normalize_hq4_message(message)


def _quarantine_unknown_message(
    exc: Exception,
    iterator: Any,
    unknowns: UnknownMessageTracker,
    processed_messages: int,
    parse_seconds: float,
    logger: logging.Logger,
) -> bool:
    unknown_type = detect_unknown_message_type(exc)
    if unknown_type is None:
        return False
    payload = unknown_message_detail(
        iterator,
        unknown_type=unknown_type,
        processed_messages=processed_messages,
        parse_seconds=parse_seconds,
    )
    payload["error"] = str(exc)
    payload["quarantine_index"] = unknowns.count + 1
    unknowns.record(payload)
    logger.warning(
        "unknown message quarantined",
        extra={"event": "iex_benchmark_unknown_message", "detail": payload},
    )
    return True


def _should_log_progress(
    processed_messages: int,
    last_progress_messages: int,
    elapsed_since_progress: float,
    progress_every_messages: int,
    progress_every_seconds: float,
) -> bool:
    return (
        processed_messages - last_progress_messages >= progress_every_messages
        or elapsed_since_progress >= progress_every_seconds
    )


if __name__ == "__main__":
    raise SystemExit(main())
