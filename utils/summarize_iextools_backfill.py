from __future__ import annotations

import argparse
import json
from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.iextools_backfill_reporting import (
    load_backfill_results,
    render_markdown_summary,
    summarize_backfill,
    write_day_list,
)
from utils.parse_iex_hist_index import DEFAULT_HIST_URL, download_hist_index, load_hist_index


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hist-url", default=DEFAULT_HIST_URL)
    parser.add_argument("--hist-index-path", default="utils/benchmark_results/iex_hist_index.json")
    parser.add_argument("--results-path", default="reports/iextools-backfill/iextools_backfill_results.jsonl")
    parser.add_argument("--parquet-root", default="/media/tn/pq")
    parser.add_argument("--report-root", default="reports/iextools-backfill")
    parser.add_argument("--start-day", default="20250501")
    parser.add_argument("--end-day")
    parser.add_argument("--download-index", action="store_true")
    args = parser.parse_args()

    hist_index_path = Path(args.hist_index_path)
    if args.download_index or not hist_index_path.exists():
        download_hist_index(args.hist_url, hist_index_path)
    records_by_day = load_hist_index(hist_index_path)
    rows = load_backfill_results(Path(args.results_path))
    summary = summarize_backfill(
        rows,
        records_by_day,
        Path(args.parquet_root),
        start_day=args.start_day,
        end_day=args.end_day,
    )

    report_root = Path(args.report_root)
    report_root.mkdir(parents=True, exist_ok=True)
    summary_json = report_root / "iextools_backfill_summary.json"
    summary_md = report_root / "iextools_backfill_summary.md"
    retry_list = report_root / "retry_failed_days.txt"
    remaining_list = report_root / "remaining_missing_days.txt"
    unattempted_list = report_root / "unattempted_missing_days.txt"

    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_md.write_text(render_markdown_summary(summary), encoding="utf-8")
    write_day_list(retry_list, summary["resume"]["retry_only_failed_days"])
    write_day_list(remaining_list, summary["resume"]["remaining_missing_days"])
    write_day_list(unattempted_list, summary["resume"]["unattempted_missing_days"])

    print(
        json.dumps(
            {
                "summary_json": str(summary_json),
                "summary_md": str(summary_md),
                "retry_list": str(retry_list),
                "remaining_list": str(remaining_list),
                "unattempted_list": str(unattempted_list),
                "suggested_resume_day": summary["resume"]["suggested_resume_day"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
