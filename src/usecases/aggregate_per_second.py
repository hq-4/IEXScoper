from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable

from src.framework.logging import get_logger
from src.framework.config import Settings
from src.adapters.io.csv_trades_reader import scan_trades_csv_for_day
from src.adapters.io.derived_writer import (
    write_staging_parts,
    compact_master_from_staging,
    delete_old_staging_parts,
)

FILTER_VERSION = "v1"


def _iterate_dates(year: int) -> Iterable[date]:
    current = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    while current < end:
        yield current
        current += timedelta(days=1)


def run_aggregate_per_second(
    year: int,
    symbols: list[str] | None,
    rebuild: bool,
    dry_run: bool,
    limit_days: int | None,
    settings: Settings,
) -> int:
    logger = get_logger("usecases.aggregate_per_second")
    logger.info("start", extra={"event": "aggregate_per_second", "year": year})

    if not settings.iex_csv_root:
        logger.error("IEX_CSV_ROOT is not configured", extra={"event": "aggregate_per_second", "year": year})
        return 1
    if not settings.iex_parquet_root:
        logger.error(
            "IEX_PARQUET_ROOT is not configured",
            extra={"event": "aggregate_per_second", "year": year},
        )
        return 1

    processed = 0
    attempted = 0
    symbols_upper = [s.upper() for s in symbols] if symbols else None

    for current in _iterate_dates(year):
        yyyymmdd = current.strftime("%Y%m%d")
        if limit_days is not None and attempted >= limit_days:
            break
        attempted += 1
        try:
            df = scan_trades_csv_for_day(
                csv_root=settings.iex_csv_root,
                yyyymmdd=yyyymmdd,
                symbols=symbols_upper,
                display_tz=settings.display_tz,
            )
        except FileNotFoundError:
            logger.debug(
                "csv_missing",
                extra={
                    "event": "aggregate_per_second",
                    "year": year,
                    "day": current.isoformat(),
                    "detail": "csv_not_found",
                },
            )
            continue
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "csv_processing_failed",
                extra={
                    "event": "aggregate_per_second",
                    "year": year,
                    "day": current.isoformat(),
                    "detail": str(exc),
                },
            )
            return 1

        if df is None or df.height == 0:
            logger.debug(
                "no_trades_for_day",
                extra={
                    "event": "aggregate_per_second",
                    "year": year,
                    "day": current.isoformat(),
                },
            )
            continue

        if dry_run:
            logger.info(
                "dry_run_day",
                extra={
                    "event": "aggregate_per_second",
                    "year": year,
                    "day": current.isoformat(),
                    "rows": df.height,
                },
            )
            processed += 1
            continue

        summaries = write_staging_parts(
            parquet_root=settings.iex_parquet_root,
            year=year,
            day=current.strftime("%Y%m%d"),
            df=df,
            filter_version=FILTER_VERSION,
            rebuild=rebuild,
        )
        logger.info(
            "day_aggregated",
            extra={
                "event": "aggregate_per_second",
                "year": year,
                "day": current.isoformat(),
                "rows": df.height,
                "detail": {"parts": len(summaries)},
            },
        )
        processed += 1

    logger.info(
        "completed",
        extra={
            "event": "aggregate_per_second",
            "year": year,
            "detail": {"processed_days": processed, "attempted_days": attempted},
        },
    )
    return 0


def run_compact_master(year: int, settings: Settings) -> int:
    logger = get_logger("usecases.compact_master")
    logger.info("start", extra={"event": "compact_master", "year": year})
    if not settings.iex_parquet_root:
        logger.error("IEX_PARQUET_ROOT is not configured", extra={"event": "compact_master", "year": year})
        return 1

    summary = compact_master_from_staging(
        parquet_root=settings.iex_parquet_root,
        year=year,
        filter_version=FILTER_VERSION,
    )
    if summary is None:
        logger.warning(
            "no_staging_parts",
            extra={"event": "compact_master", "year": year, "detail": "nothing_to_compact"},
        )
        return 0

    logger.info(
        "master_written",
        extra={
            "event": "compact_master",
            "year": year,
            "detail": summary,
        },
    )

    removed = delete_old_staging_parts(
        parquet_root=settings.iex_parquet_root,
        year=year,
        keep_days=7,
    )
    if removed:
        logger.info(
            "staging_pruned",
            extra={
                "event": "compact_master",
                "year": year,
                "detail": {"removed": len(removed)},
            },
        )

    return 0
