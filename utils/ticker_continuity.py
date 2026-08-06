"""Detect whether a resolved CIK's security is still continuously trading under a
*different* symbol (a rename/successor ticker, e.g. `GPS` -> `GAP`) or genuinely
terminal, using the same SEC submissions payload `utils.sec_sic_client.fetch_sic`
already fetches for SIC. `tickers`/`exchanges` sit unused in that same response — this
reads them via the identical `sec_submissions` cache key, so any CIK the SIC pass
already covered in the same run is a free cache hit, never a second real request.

Without this, the pipeline has no way to distinguish three structurally different
outcomes that look identical in raw trading data (a ticker just stops appearing):

- genuinely delisted/no longer independently listed (`tickers: []`)
- renamed to a new symbol, same legal entity still trading (`tickers` non-empty, doesn't
  include the era's own symbol)
- still actively trading under the *same* symbol — the era's end date is a stale
  vendor-window artifact, not a real corporate event (era's own symbol is still in
  `tickers`)
[CA][REH][KBT]
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import polars as pl

from src.framework.logging import get_logger
from utils.resolution_v2_network import CachedPrimaryClient, PrimarySourceError
from utils.resolution_v2_registry import CachePolicy
from utils.sec_sic_client import SEC_SUBMISSIONS_SOURCE

DEFAULT_MAX_AGE_DAYS = 90
LOG_EVERY = 250

STATUS_OK = "ok"
STATUS_NOT_FOUND = "cik_not_found"
STATUS_FETCH_ERROR = "fetch_error"

CONTINUITY_TERMINAL = "terminal"
CONTINUITY_SAME_SYMBOL = "still_active_same_symbol"
CONTINUITY_RENAMED_OR_SUCCESSOR = "renamed_or_successor"

CONTINUITY_LOOKUP_SCHEMA = {
    "cik": pl.String,
    "current_tickers": pl.List(pl.String),
    "current_exchanges": pl.List(pl.String),
    "fetch_status": pl.String,
    "from_cache": pl.Boolean,
}


def fetch_current_tickers(
    client: CachedPrimaryClient, cik: str, *, max_age_days: int = DEFAULT_MAX_AGE_DAYS
) -> dict[str, Any]:
    """One CIK, one result row. Never raises for a 404 or exhausted retries — those
    become `fetch_status` values so a batch continues past individual bad CIKs."""
    url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
    policy = CachePolicy(max_age=timedelta(days=max_age_days))
    try:
        payload, from_cache = client.get_json(SEC_SUBMISSIONS_SOURCE, url, {}, policy)
    except PrimarySourceError as error:
        return _error_result(cik, error)
    return _payload_result(cik, payload, from_cache)


def fetch_many_current_tickers(
    client: CachedPrimaryClient, ciks: list[str], *, max_age_days: int = DEFAULT_MAX_AGE_DAYS
) -> list[dict[str, Any]]:
    """Best-effort batch, same shape as `sec_sic_client.fetch_many`: one bad CIK never
    aborts the run."""
    logger = get_logger(__name__)
    results = []
    for index, cik in enumerate(ciks, start=1):
        results.append(fetch_current_tickers(client, cik, max_age_days=max_age_days))
        if index % LOG_EVERY == 0 or index == len(ciks):
            logger.info(
                "Ticker continuity fetch progress",
                extra={
                    "event": "ticker_continuity_fetch_progress",
                    "detail": {"done": index, "total": len(ciks)},
                },
            )
    return results


def apply_continuity_status(
    enriched: pl.DataFrame, continuity_lookup: pl.DataFrame
) -> pl.DataFrame:
    """Joins the fetched current-tickers lookup onto an era-level frame (needs `symbol`
    and `resolved_cik` columns) and derives `continuity_status`. Rows with no resolved
    CIK, or whose fetch failed, get a null status rather than a guess."""
    # Cast defensively: a caller-built frame where every resolved_cik is null (e.g. an
    # empty batch) infers Null dtype, not String, and the join below would fail on it
    # even though real pipeline output never has this problem (mirrors the same guard
    # in `sector_cik_reconcile.reconcile_cik`).
    joined = enriched.cast({"resolved_cik": pl.String}).join(
        continuity_lookup.select("cik", "current_tickers"),
        left_on="resolved_cik",
        right_on="cik",
        how="left",
    )
    return joined.with_columns(
        _continuity_status_expr("symbol", "current_tickers").alias("continuity_status")
    ).drop("current_tickers")


def _continuity_status_expr(symbol_col: str, current_tickers_col: str) -> pl.Expr:
    tickers = pl.col(current_tickers_col)
    is_empty = (tickers.list.len() == 0).fill_null(False)
    has_same_symbol = tickers.list.contains(pl.col(symbol_col)).fill_null(False)
    return (
        pl.when(tickers.is_null())
        .then(None)
        .when(is_empty)
        .then(pl.lit(CONTINUITY_TERMINAL))
        .when(has_same_symbol)
        .then(pl.lit(CONTINUITY_SAME_SYMBOL))
        .otherwise(pl.lit(CONTINUITY_RENAMED_OR_SUCCESSOR))
    )


def _payload_result(cik: str, payload: Any, from_cache: bool) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return _base_result(cik, STATUS_FETCH_ERROR, from_cache)
    result = _base_result(cik, STATUS_OK, from_cache)
    result["current_tickers"] = _string_list(payload.get("tickers"))
    result["current_exchanges"] = _string_list(payload.get("exchanges"))
    return result


def _string_list(value: Any) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


def _error_result(cik: str, error: PrimarySourceError) -> dict[str, Any]:
    status_code = _http_status(error)
    status = STATUS_NOT_FOUND if status_code == 404 else STATUS_FETCH_ERROR
    return _base_result(cik, status, from_cache=False)


def _http_status(error: PrimarySourceError) -> int | None:
    response = getattr(error.__cause__, "response", None)
    return getattr(response, "status_code", None)


def _base_result(cik: str, fetch_status: str, from_cache: bool) -> dict[str, Any]:
    return {
        "cik": cik,
        "current_tickers": None,
        "current_exchanges": None,
        "fetch_status": fetch_status,
        "from_cache": from_cache,
    }
