from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

UNKNOWN_MESSAGE_RE = re.compile(r"Unknown message type: \((\d+),\)")


def detect_unknown_message_type(exc: Exception) -> int | None:
    match = UNKNOWN_MESSAGE_RE.search(str(exc))
    if not match:
        return None
    return int(match.group(1))


def unknown_message_detail(
    iterator: Any,
    *,
    unknown_type: int,
    processed_messages: int,
    parse_seconds: float,
) -> dict[str, Any]:
    raw_body = getattr(iterator, "message_binary", None)
    send_time = getattr(iterator, "cur_send_time", None)
    return {
        "message_type": unknown_type,
        "message_type_hex": hex(unknown_type),
        "message_len_bytes": len(raw_body) + 1 if isinstance(raw_body, bytes) else None,
        "message_body_prefix_hex": raw_body[:32].hex() if isinstance(raw_body, bytes) else None,
        "messages_left_in_segment": getattr(iterator, "messages_left", None),
        "bytes_read": getattr(iterator, "bytes_read", None),
        "current_stream_offset": getattr(iterator, "cur_stream_offset", None),
        "first_sequence_number": getattr(iterator, "first_sequence_number", None),
        "segment_send_time": send_time.isoformat() if isinstance(send_time, datetime) else None,
        "processed_messages": processed_messages,
        "parse_seconds": round(parse_seconds, 6),
    }


class UnknownMessageTracker:
    def __init__(
        self,
        *,
        quarantine_path: Path,
        threshold_count: int,
        threshold_consecutive: int,
    ) -> None:
        self.quarantine_path = quarantine_path
        self.threshold_count = threshold_count
        self.threshold_consecutive = threshold_consecutive
        self.count = 0
        self.consecutive = 0
        self.types: Counter[int] = Counter()

    def record(self, payload: dict[str, Any]) -> None:
        self.count += 1
        self.consecutive += 1
        self.types[payload["message_type"]] += 1
        self.quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        with self.quarantine_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")

    def reset_consecutive(self) -> None:
        self.consecutive = 0

    def threshold_error(self) -> str | None:
        if self.count > self.threshold_count:
            return (
                "unknown message count threshold exceeded "
                f"({self.count}>{self.threshold_count})"
            )
        if self.consecutive > self.threshold_consecutive:
            return (
                "unknown message consecutive threshold exceeded "
                f"({self.consecutive}>{self.threshold_consecutive})"
            )
        return None
