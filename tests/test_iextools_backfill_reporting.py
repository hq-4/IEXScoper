from __future__ import annotations

import json
from pathlib import Path

from utils.iextools_backfill_core import tops_output_paths
from utils.iextools_backfill_reporting import (
    classify_failure,
    effective_results_by_day,
    load_backfill_results,
    render_markdown_summary,
    summarize_backfill,
    write_day_list,
)
from utils.parse_iex_hist_index import HistFileRecord


def test_classify_failure_variants() -> None:
    assert classify_failure({"error": "KeyboardInterrupt"}) == "manual_interrupt"
    assert (
        classify_failure({"error": "RuntimeError: unknown message count threshold exceeded (2>1)"})
        == "unknown_message_threshold_count_exceeded"
    )
    assert (
        classify_failure(
            {"error": "RuntimeError: unknown message consecutive threshold exceeded (2>1)"}
        )
        == "unknown_message_threshold_consecutive_exceeded"
    )
    assert (
        classify_failure({"error": "ProtocolException: Unknown message type: (0,)"})
        == "runner_unknown_message"
    )
    assert (
        classify_failure({"error": "FileExistsError: refusing to overwrite existing parquet outputs"})
        == "publish_conflict"
    )
    assert (
        classify_failure({"error": "error: unpack requires a buffer of 41 bytes"})
        == "parser_short_buffer"
    )


def test_load_results_and_effective_deduplicates_latest(tmp_path: Path) -> None:
    path = tmp_path / "results.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"day": "20250501", "status": "failed", "error": "KeyboardInterrupt"}),
                json.dumps({"day": "20250501", "status": "succeeded"}),
                json.dumps({"day": "20250502", "status": "failed", "error": "runner failed"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    rows = load_backfill_results(path)
    effective = effective_results_by_day(rows)
    assert len(rows) == 3
    assert effective["20250501"]["status"] == "succeeded"
    assert effective["20250502"]["status"] == "failed"


def test_summarize_backfill_builds_resume_and_failure_summary(tmp_path: Path) -> None:
    parquet_root = tmp_path / "pq"
    main_path, quote_path = tops_output_paths(parquet_root, "20250501")
    main_path.parent.mkdir(parents=True, exist_ok=True)
    main_path.write_bytes(b"x")
    quote_path.write_bytes(b"y")
    records = {
        "20250501": [HistFileRecord("20250501", "TOPS", "IEXTP1", "1.6", 1, "u1")],
        "20250502": [HistFileRecord("20250502", "TOPS", "IEXTP1", "1.6", 1, "u2")],
        "20250505": [HistFileRecord("20250505", "TOPS", "IEXTP1", "1.6", 1, "u3")],
    }
    rows = [
        {
            "day": "20250501",
            "status": "succeeded",
            "started_at": "2026-05-02T00:00:00+00:00",
            "finished_at": "2026-05-02T01:00:00+00:00",
            "publish": {"main_size_bytes": 10, "quote_size_bytes": 20},
        },
        {
            "day": "20250502",
            "status": "failed",
            "error": "ProtocolException: Unknown message type: (0,)",
            "started_at": "2026-05-02T01:00:00+00:00",
            "finished_at": "2026-05-02T01:30:00+00:00",
        },
    ]
    summary = summarize_backfill(
        rows,
        records,
        parquet_root,
        start_day="20250501",
        end_day=None,
    )
    assert summary["succeeded_count"] == 1
    assert summary["failed_count"] == 1
    assert summary["failure_reason_by_day"] == {"20250502": "runner_unknown_message"}
    assert summary["unknown_message_type_frequencies"] == {"0": 1}
    assert summary["resume"]["last_contiguous_published_day"] == "20250501"
    assert summary["resume"]["suggested_resume_day"] == "20250502"
    assert summary["resume"]["retry_only_failed_days"] == ["20250502"]
    assert summary["resume"]["unattempted_missing_days"] == ["20250505"]


def test_render_markdown_and_write_day_list(tmp_path: Path) -> None:
    summary = {
        "effective_day_count": 2,
        "succeeded_count": 1,
        "failed_count": 1,
        "failure_reason_counts": {"runner_unknown_message": 1},
        "failure_reason_by_day": {"20250502": "runner_unknown_message"},
        "unknown_message_type_frequencies": {"0": 1},
        "average_runtime_seconds": 3600.0,
        "average_main_size_bytes": 10.0,
        "average_quote_size_bytes": 20.0,
        "resume": {
            "last_contiguous_published_day": "20250501",
            "last_successful_day": "20250501",
            "suggested_resume_day": "20250502",
            "remaining_missing_days": ["20250502", "20250505"],
            "retry_only_failed_days": ["20250502"],
            "unattempted_missing_days": ["20250505"],
        },
    }
    report = render_markdown_summary(summary)
    assert "# IEXTools Backfill Summary" in report
    assert "| 20250502 | runner_unknown_message |" in report

    path = tmp_path / "days.txt"
    write_day_list(path, ["20250502", "20250505"])
    assert path.read_text(encoding="utf-8") == "20250502\n20250505\n"
