from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from rich.console import Console
from rich.panel import Panel

from src.framework.logging import get_logger, setup_logging
from utils.resolution_v2_program import ProgramConfig, run_resolution_program
from utils.resolution_v2_schema import DEFAULT_FACT_ROOT, DEFAULT_REPORT_ROOT


def main() -> int:
    args = parse_args()
    report_root = Path(args.report_root)
    setup_logging(str(report_root / "resolution_program.jsonl"))
    logger = get_logger(__name__)
    _log_start(logger, args)
    return _run_or_report(logger, args)


def _log_start(logger: object, args: argparse.Namespace) -> None:
    logger.info(
        "Dead ticker evidence-delta program starting",
        extra={
            "event": "resolution_v2_start",
            "detail": {
                "apply": args.apply,
                "local_only": args.local_only,
                "network_budget": args.network_budget,
                "fact_root": args.fact_root,
            },
        },
    )


def _run_or_report(logger: object, args: argparse.Namespace) -> int:
    try:
        summary = run_resolution_program(config_from_args(args))
    except Exception as exc:
        logger.exception(
            "Dead ticker evidence-delta program failed",
            extra={"event": "resolution_v2_failed", "detail": {"error": repr(exc)}},
        )
        _crash_banner(exc)
        return 1
    logger.info(
        "Dead ticker evidence-delta program complete",
        extra={"event": "resolution_v2_complete", "detail": summary},
    )
    return 0


def _crash_banner(error: Exception) -> None:
    message = (
        f"Root cause: {error}\n"
        "Remediation: correct the input/evidence state and rerun; staged work is resumable."
    )
    Console(stderr=True).print(
        Panel.fit(message, title="✖ Resolution V2 aborted", border_style="red")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the dry-run-first, evidence-delta dead ticker resolution program."
    )
    parser.add_argument("--apply", action="store_true", help="Apply the completed staged fact set.")
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Run migration, legacy harvesting, cached rescoring, queues, and reconciliation only.",
    )
    parser.add_argument("--fact-root", default=str(DEFAULT_FACT_ROOT))
    parser.add_argument("--report-root", default=str(DEFAULT_REPORT_ROOT))
    parser.add_argument("--user-agent")
    parser.add_argument("--network-budget", type=int, default=25_000)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--delay-seconds", type=float, default=0.25)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--retries", type=int, default=3)
    return parser.parse_args()


def config_from_args(args: argparse.Namespace) -> ProgramConfig:
    _validate_args(args)
    return ProgramConfig(
        apply=args.apply,
        local_only=args.local_only,
        fact_root=Path(args.fact_root),
        report_root=Path(args.report_root),
        user_agent=args.user_agent or os.getenv("SEC_USER_AGENT") or "",
        network_budget=args.network_budget,
        batch_size=args.batch_size,
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
    )


def _validate_args(args: argparse.Namespace) -> None:
    positive = (args.network_budget, args.batch_size, args.timeout_seconds, args.retries)
    if any(value <= 0 for value in positive):
        raise ValueError("network budget, batch size, timeout, and retries must be positive")
    if args.delay_seconds < 0:
        raise ValueError("--delay-seconds cannot be negative")
    if (
        not args.local_only
        and not args.apply
        and not (args.user_agent or os.getenv("SEC_USER_AGENT"))
    ):
        raise ValueError("SEC_USER_AGENT or --user-agent is required unless --local-only is used")


if __name__ == "__main__":
    raise SystemExit(main())
