from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

MIN_ROWS_BEFORE_STOP = 500
MIN_REQUESTS_BEFORE_STOP = 1_000
MIN_FACTS_PER_1K_REQUESTS = 1.0
MIN_IMPACT_PERCENT = 0.01
RECENT_END_DAYS = 180
RECENT_REFRESH_DAYS = 30
DIRECTORY_REFRESH_HOURS = 24


@dataclass
class BatchYield:
    rows: int
    requests: int
    verified_facts: int
    added_trade_rows: int
    lane_trade_rows: int

    @property
    def facts_per_1k_requests(self) -> float:
        return self.verified_facts * 1_000 / self.requests if self.requests else 0.0

    @property
    def impact_percent(self) -> float:
        return self.added_trade_rows * 100 / self.lane_trade_rows if self.lane_trade_rows else 0.0


@dataclass
class StoppingPolicy:
    batches: list[BatchYield] = field(default_factory=list)

    def add(self, batch: BatchYield) -> None:
        self.batches.append(batch)

    def should_stop(self) -> tuple[bool, str]:
        rows = sum(batch.rows for batch in self.batches)
        requests = sum(batch.requests for batch in self.batches)
        if rows < MIN_ROWS_BEFORE_STOP and requests < MIN_REQUESTS_BEFORE_STOP:
            return False, "minimum_observation_not_reached"
        if len(self.batches) < 2 or not all(_low_yield(batch) for batch in self.batches[-2:]):
            return False, "two_consecutive_low_yield_batches_not_observed"
        return True, "two_consecutive_low_fact_and_impact_yield_batches"


def cache_policy_for_era(last_day: date | None, today: date) -> dict[str, Any]:
    recent = bool(last_day and today - last_day <= timedelta(days=RECENT_END_DAYS))
    return {
        "immutable_negative": not recent,
        "max_age": timedelta(days=RECENT_REFRESH_DAYS) if recent else None,
    }


def directory_cache_policy() -> dict[str, Any]:
    return {
        "immutable_negative": False,
        "max_age": timedelta(hours=DIRECTORY_REFRESH_HOURS),
    }


def _low_yield(batch: BatchYield) -> bool:
    return (
        batch.facts_per_1k_requests < MIN_FACTS_PER_1K_REQUESTS
        and batch.impact_percent < MIN_IMPACT_PERCENT
    )
