from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT as DEFAULT_REVIEW_ROOT

DEFAULT_PRIORITY_PATH = DEFAULT_REVIEW_ROOT / "unresolved_priority_queue.csv"
DEFAULT_OUTPUT_ROOT = DEFAULT_REVIEW_ROOT / "sec-terminal-iterations"


def main() -> int:
    args = parse_args()
    root = Path(args.output_root)
    setup_logging(str(root / "sec_terminal_iterations.jsonl"))
    try:
        summary = run_iterations(args, root)
    except Exception as exc:
        get_logger(__name__).exception(
            "SEC terminal iterations failed",
            extra={"event": "sec_terminal_iterations_failed", "detail": {"error": repr(exc)}},
        )
        return 1
    get_logger(__name__).info(
        "SEC terminal iterations complete",
        extra={"event": "sec_terminal_iterations_complete", "detail": summary},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--priority-path", default=str(DEFAULT_PRIORITY_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--user-agent")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-iterations", type=int, default=10)
    parser.add_argument("--max-attempted", type=int)
    parser.add_argument("--lookback-days", type=int, default=180)
    parser.add_argument("--lookahead-days", type=int, default=60)
    parser.add_argument("--event-terms", default="merger,acquisition")
    parser.add_argument("--sleep-seconds", type=float, default=0.25)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--apply-import", action="store_true")
    parser.add_argument("--stop-after-zero-candidate-batches", type=int, default=2)
    return parser.parse_args()


def run_iterations(args: argparse.Namespace, root: Path) -> dict[str, Any]:
    user_agent = resolve_user_agent(args.user_agent)
    root.mkdir(parents=True, exist_ok=True)
    summaries = existing_iterations(root)
    attempted = attempted_from_summaries(summaries)
    zero_candidate_batches = 0
    for iteration in range(len(summaries) + 1, args.max_iterations + 1):
        batch_input = write_iteration_input(args, root, iteration, attempted)
        batch_ids = symbol_era_ids(batch_input)
        if not batch_ids:
            break
        attempted.update(batch_ids)
        batch_root = root / f"iter_{iteration:03d}"
        run_batch(args, batch_input, batch_root, user_agent)
        summary = iteration_summary(iteration, batch_input, batch_root)
        summaries.append(summary)
        zero_candidate_batches = update_zero_count(zero_candidate_batches, summary)
        write_json(root / "sec_terminal_iterations_summary.json", build_summary(args, summaries))
        if args.apply_import and summary["verified_row_count"]:
            regenerate_queues()
        if should_stop(args, attempted, zero_candidate_batches):
            break
    final = build_summary(args, summaries)
    write_json(root / "sec_terminal_iterations_summary.json", final)
    return final


def existing_iterations(root: Path) -> list[dict[str, Any]]:
    summary = read_json(root / "sec_terminal_iterations_summary.json")
    iterations = summary.get("iterations") if isinstance(summary, dict) else None
    return list(iterations) if isinstance(iterations, list) else []


def attempted_from_summaries(summaries: list[dict[str, Any]]) -> set[str]:
    attempted = set()
    for summary in summaries:
        input_path = summary.get("input_path")
        if input_path:
            attempted.update(symbol_era_ids(Path(str(input_path))))
    return attempted


def write_iteration_input(
    args: argparse.Namespace, root: Path, iteration: int, attempted: set[str]
) -> Path:
    rows, fieldnames = read_csv(Path(args.priority_path))
    selected = [row for row in rows if row.get("symbol_era_id") not in attempted][: args.batch_size]
    path = root / f"iter_{iteration:03d}_input.csv"
    write_csv(path, selected, fieldnames)
    return path


def run_batch(args: argparse.Namespace, batch_input: Path, batch_root: Path, user_agent: str) -> None:
    command = [
        "utils/run_sec_terminal_resolution_batch.py",
        "--input-path", batch_input,
        "--output-root", batch_root,
        "--user-agent", user_agent,
        "--limit", args.batch_size,
        "--lookback-days", args.lookback_days,
        "--lookahead-days", args.lookahead_days,
        "--event-terms", args.event_terms,
        "--sleep-seconds", args.sleep_seconds,
        "--timeout-seconds", args.timeout_seconds,
    ]
    if args.apply_import:
        command.append("--apply-import")
    run_py(*command)


def iteration_summary(iteration: int, batch_input: Path, batch_root: Path) -> dict[str, Any]:
    candidates = read_csv(batch_root / "terminal_import_candidates.csv")[0]
    import_summary = read_json(batch_root / "terminal_import_summary.json")
    return {
        "iteration": iteration,
        "input_path": str(batch_input),
        "output_root": str(batch_root),
        "attempted_count": len(symbol_era_ids(batch_input)),
        "candidate_count": len(candidates),
        "verified_row_count": int(import_summary.get("verified_row_count") or 0),
        "verified_symbol_era_ids": import_summary.get("verified_symbol_era_ids") or [],
        "dry_run": bool(import_summary.get("dry_run", True)),
    }


def build_summary(args: argparse.Namespace, summaries: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "batch_size": args.batch_size,
        "max_iterations": args.max_iterations,
        "apply_import": args.apply_import,
        "iteration_count": len(summaries),
        "attempted_count": sum(item["attempted_count"] for item in summaries),
        "candidate_count": sum(item["candidate_count"] for item in summaries),
        "verified_row_count": sum(item["verified_row_count"] for item in summaries),
        "iterations": summaries,
    }


def should_stop(args: argparse.Namespace, attempted: set[str], zero_candidate_batches: int) -> bool:
    if args.max_attempted is not None and len(attempted) >= args.max_attempted:
        return True
    return zero_candidate_batches >= args.stop_after_zero_candidate_batches


def update_zero_count(current: int, summary: dict[str, Any]) -> int:
    return current + 1 if summary["candidate_count"] == 0 else 0


def regenerate_queues() -> None:
    run_py("utils/build_dead_ticker_review_queue.py")
    run_py("utils/build_dead_ticker_priority_queue.py")


def symbol_era_ids(path: Path) -> list[str]:
    return [row["symbol_era_id"] for row in read_csv(path)[0] if row.get("symbol_era_id")]


def read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not path.exists():
        return [], []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader), list(reader.fieldnames or [])


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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
