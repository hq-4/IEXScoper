from __future__ import annotations

from collections import deque

from utils.iextools_backfill_recovery import (
    is_retryable_runner_failure,
    summarize_runner_failure,
)
from utils.iextools_backfill_reporting import classify_failure


def test_retryable_runner_failure_detects_crc_and_negative_length() -> None:
    assert is_retryable_runner_failure("BadGzipFile: CRC check failed 0x1 != 0x2")
    assert is_retryable_runner_failure(
        "ValueError: read length must be non-negative or -1"
    )
    assert is_retryable_runner_failure(
        "error: Error -3 while decompressing data: invalid code lengths set"
    )
    assert is_retryable_runner_failure("error: unpack requires a buffer of 41 bytes")


def test_retryable_runner_failure_ignores_unknown_message_thresholds() -> None:
    assert not is_retryable_runner_failure(
        "RuntimeError: unknown message consecutive threshold exceeded"
    )


def test_summarize_runner_failure_prefers_runner_payload_error() -> None:
    error = summarize_runner_failure(
        runner_payload={"error": "BadGzipFile: CRC check failed"},
        stdout_lines=["stdout noise"],
        stderr_lines=["stderr noise"],
    )
    assert error == "BadGzipFile: CRC check failed"


def test_summarize_runner_failure_uses_bounded_tails() -> None:
    error = summarize_runner_failure(
        runner_payload=None,
        stdout_lines=deque((f"stdout {index}\n" for index in range(100)), maxlen=80),
        stderr_lines=deque(["stderr detail\n"], maxlen=80),
        return_code=1,
    )
    assert "runner exited with code 1" in error
    assert "stderr tail:\nstderr detail" in error
    assert "stdout 20" in error
    assert "stdout 19" not in error


def test_classify_failure_distinguishes_corruption_signatures() -> None:
    assert classify_failure({"error": "BadGzipFile: CRC check failed"}) == "gzip_crc_failed"
    assert (
        classify_failure({"error": "error: Error -3 while decompressing data"})
        == "gzip_decompress_failed"
    )
    assert (
        classify_failure({"error": "ValueError: read length must be non-negative or -1"})
        == "parser_negative_message_length"
    )
    assert (
        classify_failure({"error": "error: unpack requires a buffer of 41 bytes"})
        == "parser_short_buffer"
    )
