from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT as DEFAULT_REVIEW_ROOT

DEFAULT_INPUT_PATH = DEFAULT_REVIEW_ROOT / "unresolved_priority_queue.csv"
DEFAULT_BATCH_ROOT = DEFAULT_REVIEW_ROOT / "sec-terminal-batch"


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root)
    setup_logging(str(output_root / "sec_terminal_batch.jsonl"))
    try:
        run_batch(args, output_root)
    except Exception as exc:
        get_logger(__name__).exception(
            "SEC terminal resolution batch failed",
            extra={"event": "sec_terminal_batch_failed", "detail": {"error": repr(exc)}},
        )
        return 1
    get_logger(__name__).info(
        "SEC terminal resolution batch complete",
        extra={"event": "sec_terminal_batch_complete", "detail": {"output_root": str(output_root)}},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_BATCH_ROOT))
    parser.add_argument("--user-agent")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--lookback-days", type=int, default=180)
    parser.add_argument("--lookahead-days", type=int, default=60)
    parser.add_argument("--event-terms", default="merger,acquisition")
    parser.add_argument("--sleep-seconds", type=float, default=0.25)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--apply-import", action="store_true")
    return parser.parse_args()


def run_batch(args: argparse.Namespace, output_root: Path) -> None:
    user_agent = resolve_user_agent(args.user_agent)
    output_root.mkdir(parents=True, exist_ok=True)
    paths = batch_paths(output_root)
    run_steps(args, paths, user_agent)
    write_strong_needs_close(paths["strict_review"], paths["strong_needs_close"])
    run_close_and_text_steps(args, paths, user_agent)
    write_high_signal_missing_date(paths["terminal_text_review"], paths["high_signal_missing"])
    run_followup_and_import_steps(args, paths, user_agent)


def run_steps(args: argparse.Namespace, paths: dict[str, Path], user_agent: str) -> None:
    run_py(
        "utils/build_terminal_event_search_batch.py",
        "--input-path", args.input_path,
        "--output-path", paths["terminal_window"],
        "--summary-path", paths["terminal_window_summary"],
        "--limit", args.limit,
        "--lookback-days", args.lookback_days,
        "--lookahead-days", args.lookahead_days,
    )
    run_py(
        "utils/search_edgar_full_text.py",
        "--template-path", paths["terminal_window"],
        "--output-root", paths["search_root"],
        "--event-terms", args.event_terms,
        "--user-agent", user_agent,
        "--sleep-seconds", args.sleep_seconds,
        "--timeout-seconds", args.timeout_seconds,
        "--strict-date-bounds",
    )
    run_py(
        "utils/edgar_full_text_triage.py",
        "--leads-path", paths["search_root"] / "edgar_full_text_leads.parquet",
        "--output-csv", paths["triage_csv"],
        "--output-parquet", paths["triage_parquet"],
        "--summary-path", paths["triage_summary"],
    )
    run_py(
        "utils/build_dead_ticker_override_candidates.py",
        "--template-path", paths["terminal_window"],
        "--triage-path", paths["triage_parquet"],
        "--output-path", paths["candidates"],
        "--min-bucket", "medium_confidence_lead",
    )
    run_py(
        "utils/verify_sec_override_candidates.py",
        "--candidates-path", paths["candidates"],
        "--output-path", paths["verified_triage"],
        "--summary-path", paths["verified_summary"],
        "--user-agent", user_agent,
        "--sleep-seconds", args.sleep_seconds,
        "--timeout-seconds", args.timeout_seconds,
    )
    run_py(
        "utils/build_strict_terminal_review.py",
        "--verifier-path", paths["verified_triage"],
        "--output-path", paths["strict_review"],
        "--auto-verified-path", paths["strict_auto"],
        "--summary-path", paths["strict_summary"],
    )


def run_close_and_text_steps(args: argparse.Namespace, paths: dict[str, Path], user_agent: str) -> None:
    run_py(
        "utils/build_close_evidence_review.py",
        "--input-path", paths["strong_needs_close"],
        "--output-path", paths["close_review"],
        "--auto-verified-path", paths["close_auto"],
        "--summary-path", paths["close_summary"],
    )
    run_py(
        "utils/build_sec_terminal_text_evidence.py",
        "--verifier-path", paths["verified_triage"],
        "--output-dir", paths["search_root"],
        "--user-agent", user_agent,
        "--sleep-seconds", args.sleep_seconds,
        "--timeout-seconds", args.timeout_seconds,
    )


def run_followup_and_import_steps(args: argparse.Namespace, paths: dict[str, Path], user_agent: str) -> None:
    run_py(
        "utils/build_sec_terminal_followup_evidence.py",
        "--input-path", paths["high_signal_missing"],
        "--output-dir", paths["search_root"],
        "--user-agent", user_agent,
        "--sleep-seconds", args.sleep_seconds,
        "--timeout-seconds", args.timeout_seconds,
    )
    write_import_candidates(
        paths["search_root"] / "terminal_followup_auto_verified.csv",
        paths["import_candidates"],
    )
    import_args = [
        "utils/import_dead_ticker_manual_overrides.py",
        "--template-path", paths["import_candidates"],
        "--summary-path", paths["import_summary"],
    ]
    if not args.apply_import:
        import_args.append("--dry-run")
    run_py(*import_args)


def batch_paths(root: Path) -> dict[str, Path]:
    search_root = root / "edgar"
    return {
        "search_root": search_root,
        "terminal_window": root / "terminal_window.csv",
        "terminal_window_summary": root / "terminal_window_summary.json",
        "triage_csv": search_root / "edgar_full_text_triage.csv",
        "triage_parquet": search_root / "edgar_full_text_triage.parquet",
        "triage_summary": search_root / "edgar_full_text_triage_summary.json",
        "candidates": search_root / "sec_override_candidates.csv",
        "verified_triage": search_root / "sec_override_candidates_verified_triage.csv",
        "verified_summary": search_root / "sec_override_candidates_verified_triage_summary.json",
        "strict_review": search_root / "strict_terminal_review.csv",
        "strict_auto": search_root / "strict_terminal_auto_verified.csv",
        "strict_summary": search_root / "strict_terminal_review_summary.json",
        "strong_needs_close": search_root / "strong_needs_close_review.csv",
        "close_review": search_root / "close_evidence_review.csv",
        "close_auto": search_root / "close_evidence_auto_verified.csv",
        "close_summary": search_root / "close_evidence_review_summary.json",
        "terminal_text_review": search_root / "terminal_text_evidence_review.csv",
        "high_signal_missing": search_root / "terminal_text_high_signal_missing_date_review.csv",
        "import_candidates": root / "terminal_import_candidates.csv",
        "import_summary": root / "terminal_import_summary.json",
    }


def write_strong_needs_close(source: Path, output: Path) -> None:
    rows = read_csv_rows(source)
    selected = [r for r in rows if r.get("strict_terminal_bucket") == "strong_needs_close_or_completion_review"]
    selected.sort(key=lambda r: (abs(int(r.get("days_filed_to_original_last") or 999999)), -int(float(r.get("trade_rows") or 0))))
    write_csv_rows(output, selected, rows[0].keys() if rows else [])


def write_high_signal_missing_date(source: Path, output: Path) -> None:
    rows = read_csv_rows(source)
    selected = [r for r in rows if high_signal_missing_date(r)]
    selected.sort(key=lambda r: (-int(r.get("terminal_text_score") or 0), int(r.get("priority_rank") or 999999)))
    write_csv_rows(output, selected, rows[0].keys() if rows else [])


def high_signal_missing_date(row: dict[str, str]) -> bool:
    flags = set((row.get("terminal_text_flags") or "").split("|"))
    has_close = "completion_language" in flags or "delisting_language" in flags
    has_identity = "issuer_name_match" in flags or "direct_ticker_evidence" in flags
    return row.get("terminal_text_bucket") == "missing_terminal_date_review" and has_close and has_identity


def write_import_candidates(source: Path, output: Path) -> None:
    source_rows, fieldnames = read_csv_rows_and_fields(source)
    rows = [row for row in source_rows if safe_import_candidate(row)]
    rows.sort(key=lambda r: (r["symbol_era_id"], -int(r.get("followup_terminal_text_score") or 0)))
    deduped = list({row["symbol_era_id"]: import_row(row) for row in rows}.values())
    write_csv_rows(output, deduped, fieldnames)


def safe_import_candidate(row: dict[str, str]) -> bool:
    snippet = (row.get("followup_terminal_text_snippet") or "").upper()
    flags = set((row.get("followup_terminal_text_flags") or "").split("|"))
    return (
        row.get("followup_terminal_text_bucket") == "terminal_text_verified_ready"
        and "prospective_close_language" not in flags
        and not any(term in snippet for term in prospective_terms())
    )


def import_row(row: dict[str, str]) -> dict[str, str]:
    row = dict(row)
    row["research_status"] = "verified"
    row["primary_source_url"] = row.get("followup_terminal_text_document_url") or row.get("primary_source_url", "")
    row["proposed_historical_event_date"] = row.get("followup_terminal_text_date") or row.get("proposed_historical_event_date", "")
    row["research_note"] = (row.get("research_note", "") + " " + followup_note(row)).strip()
    return row


def followup_note(row: dict[str, str]) -> str:
    return (
        f"Terminal follow-up evidence: form={row.get('followup_form')}; "
        f"filed_at={row.get('followup_filed_at')}; "
        f"terminal_date={row.get('followup_terminal_text_date')}; "
        f"days_to_original_last={row.get('followup_days_terminal_text_to_original_last')}; "
        f"bucket={row.get('followup_terminal_text_bucket')}; "
        f"flags={row.get('followup_terminal_text_flags')}."
    )


def prospective_terms() -> tuple[str, ...]:
    return ("EXPECTED TO OCCUR", "EXPECT TO CLOSE", "EXPECTED TO CLOSE", "SUBJECT TO THE SATISFACTION")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    return read_csv_rows_and_fields(path)[0]


def read_csv_rows_and_fields(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not path.exists():
        return [], []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader), list(reader.fieldnames or [])


def write_csv_rows(path: Path, rows: list[dict[str, str]], fieldnames: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    field_list = list(fieldnames)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=field_list)
        writer.writeheader()
        writer.writerows(rows)


def run_py(*args: Any) -> None:
    command = [sys.executable, *(str(arg) for arg in args)]
    subprocess.run(command, cwd=Path(__file__).resolve().parents[1], check=True)


def resolve_user_agent(value: str | None) -> str:
    user_agent = value or os.getenv("SEC_USER_AGENT")
    if not user_agent:
        raise ValueError("provide --user-agent or set SEC_USER_AGENT before hitting SEC")
    return user_agent


if __name__ == "__main__":
    raise SystemExit(main())
