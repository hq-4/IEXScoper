from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

RETRYABLE_RUNNER_ERROR_SNIPPETS = (
    "BadGzipFile:",
    "CRC check failed",
    "invalid code lengths set",
    "Error -3 while decompressing data",
    "read length must be non-negative or -1",
    "Compressed file ended before the end-of-stream marker was reached",
    "unpack requires a buffer",
)

MAX_FAILURE_TEXT_CHARS = 12_000
TAIL_LINE_COUNT = 80


def load_runner_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def summarize_runner_failure(
    *,
    runner_payload: dict[str, Any] | None,
    stdout_lines: Sequence[str],
    stderr_lines: Sequence[str],
    return_code: int | None = None,
) -> str:
    if runner_payload and runner_payload.get("error"):
        return _truncate(str(runner_payload["error"]))
    parts = [f"runner exited with code {return_code}"]
    stderr_text = _tail_text(stderr_lines)
    stdout_text = _tail_text(stdout_lines)
    if stderr_text:
        parts.append(f"stderr tail:\n{stderr_text}")
    if stdout_text:
        parts.append(f"stdout tail:\n{stdout_text}")
    return _truncate("\n\n".join(parts))


def is_retryable_runner_failure(error_text: str) -> bool:
    return any(snippet in error_text for snippet in RETRYABLE_RUNNER_ERROR_SNIPPETS)


def _tail_text(lines: Sequence[str]) -> str:
    return "".join(list(lines)[-TAIL_LINE_COUNT:]).strip()


def _truncate(text: str) -> str:
    if len(text) <= MAX_FAILURE_TEXT_CHARS:
        return text
    omitted = len(text) - MAX_FAILURE_TEXT_CHARS
    return f"{text[:MAX_FAILURE_TEXT_CHARS]}...(+{omitted} chars)"
