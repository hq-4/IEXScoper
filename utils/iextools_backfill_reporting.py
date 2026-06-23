from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any

from utils.iextools_backfill_core import existing_tops_days
from utils.parse_iex_hist_index import HistFileRecord

UNKNOWN_MESSAGE_RE = re.compile(r"Unknown message type: \((\d+),\)")


def load_backfill_results(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def classify_failure(payload: dict[str, Any]) -> str:
    error = payload.get("error", "")
    if "KeyboardInterrupt" in error:
        return "manual_interrupt"
    if "BadGzipFile:" in error or "CRC check failed" in error:
        return "gzip_crc_failed"
    if "invalid code lengths set" in error or "Error -3 while decompressing data" in error:
        return "gzip_decompress_failed"
    if "read length must be non-negative or -1" in error:
        return "parser_negative_message_length"
    if "unpack requires a buffer" in error:
        return "parser_short_buffer"
    if "IndexError: index out of range" in error:
        return "parser_header_not_found"
    if "unknown message count threshold exceeded" in error:
        return "unknown_message_threshold_count_exceeded"
    if "unknown message consecutive threshold exceeded" in error:
        return "unknown_message_threshold_consecutive_exceeded"
    if "Unknown message type:" in error:
        return "runner_unknown_message"
    if "refusing to overwrite existing parquet outputs" in error:
        return "publish_conflict"
    if "requests.exceptions" in error or "HTTPError" in error or "ConnectionError" in error:
        return "download_failed"
    if "runner failed" in error:
        return "runner_failed"
    if "FileExistsError" in error:
        return "publish_failed"
    return "other_failure"


def extract_unknown_types(payload: dict[str, Any]) -> list[int]:
    types: list[int] = []
    for match in UNKNOWN_MESSAGE_RE.finditer(payload.get("error", "")):
        types.append(int(match.group(1)))
    for msg_type, count in payload.get("unknown_message_types", {}).items():
        types.extend([int(msg_type)] * int(count))
    return types


def effective_results_by_day(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        latest[row["day"]] = row
    return latest


def tops_days_in_range(
    records_by_day: dict[str, list[HistFileRecord]], *, start_day: str, end_day: str | None
) -> list[str]:
    days: list[str] = []
    for day in sorted(records_by_day):
        if day < start_day:
            continue
        if end_day is not None and day > end_day:
            continue
        if any(record.feed == "TOPS" for record in records_by_day[day]):
            days.append(day)
    return days


def build_resume_state(
    records_by_day: dict[str, list[HistFileRecord]],
    parquet_root: Path,
    effective_rows: dict[str, dict[str, Any]],
    *,
    start_day: str,
    end_day: str | None,
) -> dict[str, Any]:
    candidate_days = tops_days_in_range(records_by_day, start_day=start_day, end_day=end_day)
    published = existing_tops_days(parquet_root)
    remaining_missing_days = [day for day in candidate_days if day not in published]
    retry_only_failed_days = [
        day
        for day in remaining_missing_days
        if effective_rows.get(day, {}).get("status") == "failed"
    ]
    unattempted_missing_days = [
        day for day in remaining_missing_days if day not in effective_rows
    ]
    last_contiguous_published_day = None
    for day in candidate_days:
        if day in published:
            last_contiguous_published_day = day
            continue
        break
    successful_days = sorted(day for day, row in effective_rows.items() if row.get("status") == "succeeded")
    return {
        "last_contiguous_published_day": last_contiguous_published_day,
        "last_successful_day": successful_days[-1] if successful_days else None,
        "suggested_resume_day": remaining_missing_days[0] if remaining_missing_days else None,
        "remaining_missing_days": remaining_missing_days,
        "retry_only_failed_days": retry_only_failed_days,
        "unattempted_missing_days": unattempted_missing_days,
    }


def summarize_backfill(
    rows: list[dict[str, Any]],
    records_by_day: dict[str, list[HistFileRecord]],
    parquet_root: Path,
    *,
    start_day: str,
    end_day: str | None,
) -> dict[str, Any]:
    effective = effective_results_by_day(rows)
    successes = [row for row in effective.values() if row.get("status") == "succeeded"]
    failures = [row for row in effective.values() if row.get("status") == "failed"]
    failure_by_day = {
        row["day"]: classify_failure(row)
        for row in sorted(failures, key=lambda item: item["day"])
    }
    failure_counts: dict[str, int] = {}
    for reason in failure_by_day.values():
        failure_counts[reason] = failure_counts.get(reason, 0) + 1
    unknown_counts: dict[str, int] = {}
    for row in effective.values():
        for unknown_type in extract_unknown_types(row):
            key = str(unknown_type)
            unknown_counts[key] = unknown_counts.get(key, 0) + 1
    durations = [
        _duration_seconds(row["started_at"], row["finished_at"])
        for row in successes
        if row.get("started_at") and row.get("finished_at")
    ]
    main_sizes = [row["publish"]["main_size_bytes"] for row in successes if row.get("publish")]
    quote_sizes = [row["publish"]["quote_size_bytes"] for row in successes if row.get("publish")]
    summary = {
        "effective_day_count": len(effective),
        "succeeded_count": len(successes),
        "failed_count": len(failures),
        "failure_reason_counts": dict(sorted(failure_counts.items())),
        "failure_reason_by_day": failure_by_day,
        "unknown_message_type_frequencies": dict(
            sorted(unknown_counts.items(), key=lambda item: (int(item[0]), item[0]))
        ),
        "average_runtime_seconds": round(mean(durations), 3) if durations else None,
        "average_main_size_bytes": round(mean(main_sizes), 3) if main_sizes else None,
        "average_quote_size_bytes": round(mean(quote_sizes), 3) if quote_sizes else None,
        "resume": build_resume_state(
            records_by_day,
            parquet_root,
            effective,
            start_day=start_day,
            end_day=end_day,
        ),
    }
    return summary


def render_markdown_summary(summary: dict[str, Any]) -> str:
    resume = summary["resume"]
    lines = [
        "# IEXTools Backfill Summary",
        "",
        "## Totals",
        "",
        f"- Effective days: `{summary['effective_day_count']}`",
        f"- Succeeded: `{summary['succeeded_count']}`",
        f"- Failed: `{summary['failed_count']}`",
        f"- Average runtime seconds: `{summary['average_runtime_seconds']}`",
        f"- Average main parquet size bytes: `{summary['average_main_size_bytes']}`",
        f"- Average quote parquet size bytes: `{summary['average_quote_size_bytes']}`",
        "",
        "## Resume",
        "",
        f"- Last contiguous published day: `{resume['last_contiguous_published_day']}`",
        f"- Last successful day: `{resume['last_successful_day']}`",
        f"- Suggested resume day: `{resume['suggested_resume_day']}`",
        f"- Remaining missing days: `{len(resume['remaining_missing_days'])}`",
        f"- Retry-only failed days: `{len(resume['retry_only_failed_days'])}`",
        f"- Unattempted missing days: `{len(resume['unattempted_missing_days'])}`",
        "",
        "## Failure Reasons",
        "",
        "| Reason | Count |",
        "| --- | ---: |",
    ]
    for reason, count in summary["failure_reason_counts"].items():
        lines.append(f"| {reason} | {count} |")
    lines.extend(
        [
            "",
            "## Unknown Message Types",
            "",
            "| Type | Count |",
            "| --- | ---: |",
        ]
    )
    for msg_type, count in summary["unknown_message_type_frequencies"].items():
        lines.append(f"| {msg_type} | {count} |")
    lines.extend(
        [
            "",
            "## Failed Days",
            "",
            "| Day | Reason |",
            "| --- | --- |",
        ]
    )
    for day, reason in summary["failure_reason_by_day"].items():
        lines.append(f"| {day} | {reason} |")
    return "\n".join(lines) + "\n"


def write_day_list(path: Path, days: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(days) + ("\n" if days else ""), encoding="utf-8")


def _duration_seconds(started_at: str, finished_at: str) -> float:
    start = datetime.fromisoformat(started_at)
    finish = datetime.fromisoformat(finished_at)
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if finish.tzinfo is None:
        finish = finish.replace(tzinfo=UTC)
    return (finish - start).total_seconds()
