import argparse
from src.framework.config import get_settings
from src.framework.logging import setup_logging, get_logger
from src.usecases.aggregate_per_second import run_aggregate_per_second, run_compact_master

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

if __name__ == "__main__":
    main()
