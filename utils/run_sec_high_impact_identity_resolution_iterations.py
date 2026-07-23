from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.sec_high_impact_workflow import (
    DEFAULT_INPUT_PATH,
    DEFAULT_OUTPUT_ROOT,
    HighImpactConfig,
    run_high_impact_workflow,
)


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root)
    setup_logging(str(output_root / "sec_high_impact_identity_resolution.jsonl"))
    logger = get_logger(__name__)
    logger.info(
        "SEC high-impact identity resolution starting",
        extra={
            "event": "sec_identity_resolution_starting",
            "detail": {
                "input_path": args.input_path,
                "output_root": args.output_root,
                "batch_size": args.batch_size,
                "max_rows": args.max_rows,
                "apply_import": args.apply_import,
            },
        },
    )
    try:
        summary = run_high_impact_workflow(config_from_args(args))
    except Exception as exc:
        logger.exception(
            "SEC high-impact identity resolution failed; state is resumable",
            extra={"event": "sec_identity_resolution_failed", "detail": {"error": repr(exc)}},
        )
        return 1
    logger.info(
        "SEC high-impact identity resolution complete",
        extra={"event": "sec_identity_resolution_complete", "detail": summary},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve high-impact ticker eras with identity-anchored SEC evidence."
    )
    parser.add_argument("--input-path", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--user-agent")
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--max-rows", type=int)
    parser.add_argument("--sleep-seconds", type=float, default=0.25)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--max-row-attempts", type=int, default=2)
    parser.add_argument("--max-docs-per-row", type=int, default=12)
    parser.add_argument("--identity-days-before", type=int, default=45)
    parser.add_argument("--identity-days-after", type=int, default=90)
    parser.add_argument("--event-days-before", type=int, default=180)
    parser.add_argument("--event-days-after", type=int, default=90)
    parser.add_argument("--apply-import", action="store_true")
    parser.add_argument("--force-reprocess", action="store_true")
    return parser.parse_args()


def config_from_args(args: argparse.Namespace) -> HighImpactConfig:
    user_agent = args.user_agent or os.getenv("SEC_USER_AGENT") or ""
    return HighImpactConfig(
        input_path=Path(args.input_path),
        output_root=Path(args.output_root),
        user_agent=user_agent,
        batch_size=args.batch_size,
        max_rows=args.max_rows,
        sleep_seconds=args.sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
        max_row_attempts=args.max_row_attempts,
        max_docs_per_row=args.max_docs_per_row,
        identity_days_before=args.identity_days_before,
        identity_days_after=args.identity_days_after,
        event_days_before=args.event_days_before,
        event_days_after=args.event_days_after,
        apply_import=args.apply_import,
        force_reprocess=args.force_reprocess,
    )


if __name__ == "__main__":
    raise SystemExit(main())
