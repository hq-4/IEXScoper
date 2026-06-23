from __future__ import annotations

import argparse
import json
from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.iextools_price_repair import repair_day, select_days


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet-root", default="/media/tn/pq")
    parser.add_argument("--report-path", default="reports/iextools-price-repair/results.jsonl")
    parser.add_argument("--log-jsonl", default="reports/iextools-price-repair/repair.jsonl")
    parser.add_argument("--start-day", default="20170101")
    parser.add_argument("--end-day")
    parser.add_argument("--days", help="Comma-separated YYYYMMDD list. Overrides range selection.")
    parser.add_argument("--limit-days", type=int)
    parser.add_argument("--backup-root", help="Optional backup destination for originals before replace.")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    setup_logging(args.log_jsonl)
    logger = get_logger(__name__)
    parquet_root = Path(args.parquet_root)
    report_path = Path(args.report_path)
    backup_root = Path(args.backup_root) if args.backup_root else None
    days = select_days(
        parquet_root,
        start_day=args.start_day,
        end_day=args.end_day,
        days_arg=args.days,
        limit_days=args.limit_days,
    )
    logger.info(
        "price repair start",
        extra={
            "event": "iextools_price_repair_start",
            "detail": {
                "day_count": len(days),
                "parquet_root": str(parquet_root),
                "apply": args.apply,
                "backup_root": str(backup_root) if backup_root else None,
            },
        },
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    days_needing_repair = _process_days(
        days=days,
        parquet_root=parquet_root,
        report_path=report_path,
        apply=args.apply,
        backup_root=backup_root,
        logger=logger,
    )
    logger.info(
        "price repair complete",
        extra={
            "event": "iextools_price_repair_complete",
            "detail": {
                "day_count": len(days),
                "days_needing_repair": days_needing_repair,
                "apply": args.apply,
            },
        },
    )
    return 0


def _process_days(
    *,
    days: list[str],
    parquet_root: Path,
    report_path: Path,
    apply: bool,
    backup_root: Path | None,
    logger,
) -> int:
    days_needing_repair = 0
    for day in days:
        payload = repair_day(
            parquet_root=parquet_root,
            day=day,
            apply=apply,
            backup_root=backup_root,
        )
        if payload["needs_repair"]:
            days_needing_repair += 1
        with report_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
        logger.info(
            "price repair day complete",
            extra={"event": "iextools_price_repair_day_complete", "day": day, "detail": payload},
        )
    return days_needing_repair


if __name__ == "__main__":
    raise SystemExit(main())
