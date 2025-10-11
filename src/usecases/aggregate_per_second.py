from src.framework.logging import get_logger
from src.framework.config import Settings

def run_aggregate_per_second(year: int, symbols: list[str] | None, rebuild: bool, dry_run: bool, limit_days: int | None, settings: Settings) -> int:
    logger = get_logger("usecases.aggregate_per_second")
    logger.info("start", extra={"event": "aggregate_per_second", "year": year})
    return 0

def run_compact_master(year: int, settings: Settings) -> int:
    logger = get_logger("usecases.compact_master")
    logger.info("start", extra={"event": "compact_master", "year": year})
    return 0
