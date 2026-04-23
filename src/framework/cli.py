import argparse
from pathlib import Path

from src.framework.config import get_settings
from src.framework.logging import setup_logging, get_logger
from src.usecases.aggregate_per_second import run_aggregate_per_second, run_compact_master
from src.usecases.tops_ingest import SAMPLE_DAYS, run_tops_ingest_validation, write_tops_spec_audit

def main() -> None:
    p = argparse.ArgumentParser(prog="iexscoper")
    sub = p.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("aggregate-per-second")
    p1.add_argument("--year", type=int, required=True)
    p1.add_argument("--symbols", type=str, default="")
    p1.add_argument("--rebuild", action="store_true")
    p1.add_argument("--dry-run", action="store_true")
    p1.add_argument("--limit-days", type=int)

    p2 = sub.add_parser("compact-master")
    p2.add_argument("--year", type=int, required=True)

    p3 = sub.add_parser("tops-spec-audit")
    p3.add_argument("--report-root")

    p4 = sub.add_parser("validate-tops-ingest")
    p4.add_argument("--work-root")
    p4.add_argument("--report-root")
    p4.add_argument("--days", default=",".join(SAMPLE_DAYS))
    p4.add_argument("--all-available", action="store_true")
    p4.add_argument("--start-day", default="20250101")
    p4.add_argument("--end-day")
    p4.add_argument("--dry-run", action="store_true")
    p4.add_argument("--keep-raw", action="store_true")
    p4.add_argument(
        "--parser-bin",
        default="iex-parser/iex_cppparser/bin/iex_parser.out",
    )

    args = p.parse_args()
    settings = get_settings()
    setup_logging(settings.log_jsonl_path)
    logger = get_logger("cli")
    if args.cmd == "aggregate-per-second":
        symbols = [s for s in args.symbols.split(",") if s] if args.symbols else None
        code = run_aggregate_per_second(
            year=args.year,
            symbols=symbols,
            rebuild=args.rebuild,
            dry_run=args.dry_run,
            limit_days=args.limit_days,
            settings=settings,
        )
        logger.info("aggregate-per-second exit", extra={"event": "aggregate_per_second", "year": args.year})
        raise SystemExit(code)
    if args.cmd == "compact-master":
        code = run_compact_master(year=args.year, settings=settings)
        logger.info("compact-master exit", extra={"event": "compact_master", "year": args.year})
        raise SystemExit(code)
    if args.cmd == "tops-spec-audit":
        result = write_tops_spec_audit(Path(args.report_root or settings.iex_report_root))
        logger.info("tops-spec-audit exit", extra={"event": "tops_spec_audit", "detail": result})
        print(result)
        raise SystemExit(0)
    if args.cmd == "validate-tops-ingest":
        days = [day.strip() for day in args.days.split(",") if day.strip()]
        code = run_tops_ingest_validation(
            settings=settings,
            work_root=args.work_root or settings.iex_work_root,
            report_root=args.report_root or settings.iex_report_root,
            days=days,
            all_available=args.all_available,
            start_day=args.start_day,
            end_day=args.end_day,
            dry_run=args.dry_run,
            keep_raw=args.keep_raw,
            parser_bin=args.parser_bin,
        )
        logger.info("validate-tops-ingest exit", extra={"event": "validate_tops_ingest"})
        raise SystemExit(code)

if __name__ == "__main__":
    main()
