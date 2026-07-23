from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from utils.dead_ticker_review_schema import DEFAULT_MANUAL_OVERRIDES_PATH
from utils import sec_high_impact_logging as progress_log
from utils.sec_high_impact_events import (
    active_current_evidence,
    empty_resolution,
    event_evidence,
    select_verified_event,
)
from utils.sec_high_impact_runtime import (
    default_raw_paths,
    finalize_import,
    load_ticker_directory,
    new_evidence_client,
    new_submissions_client,
    total_requests,
    update_import_summary,
    write_outputs,
)
from utils.sec_high_impact_state import (
    FINAL_STATUSES,
    initialize_state,
    read_csv,
    record_attempt,
    record_result,
    record_transport_error,
    write_state,
)
from utils.sec_identity_evidence import (
    build_identity_queries,
    identity_result,
    identity_window,
)
from utils.sec_identity_sources import (
    SecTransportError,
    deduplicate_evidence,
    evidence_from_payload,
    evidence_from_raw_paths,
)

DEFAULT_INPUT_PATH = Path(
    "reports/dead-ticker-review/resolution-workplan/workplan_high_impact_operating.csv"
)
DEFAULT_OUTPUT_ROOT = Path("reports/dead-ticker-review/sec-high-impact-identity-resolution")
DEFAULT_TICKER_DIRECTORY = Path(
    "reports/dead-ticker-review/edgar-lookup/company_tickers_exchange.json"
)


@dataclass(frozen=True)
class HighImpactConfig:
    input_path: Path = DEFAULT_INPUT_PATH
    output_root: Path = DEFAULT_OUTPUT_ROOT
    user_agent: str = ""
    batch_size: int = 250
    max_rows: int | None = None
    sleep_seconds: float = 0.25
    timeout_seconds: float = 10.0
    retries: int = 3
    max_row_attempts: int = 2
    max_docs_per_row: int = 12
    identity_days_before: int = 45
    identity_days_after: int = 90
    event_days_before: int = 180
    event_days_after: int = 90
    apply_import: bool = False
    force_reprocess: bool = False
    raw_evidence_paths: tuple[Path, ...] = ()
    overrides_path: Path = DEFAULT_MANUAL_OVERRIDES_PATH
    ticker_directory_path: Path = DEFAULT_TICKER_DIRECTORY


def run_high_impact_workflow(
    config: HighImpactConfig,
    evidence_client: Any | None = None,
    submissions_client: Any | None = None,
) -> dict[str, Any]:
    validate_config(config)
    rows, _ = read_csv(config.input_path)
    state = initialize_state(config.input_path, config.output_root, force=config.force_reprocess)
    raw_paths = config.raw_evidence_paths or tuple(default_raw_paths(config.output_root))
    local_evidence = evidence_from_raw_paths(raw_paths)
    evidence_by_ticker = index_evidence(local_evidence)
    evidence_client = evidence_client or new_evidence_client(config)
    submissions_client = submissions_client or new_submissions_client(config)
    ticker_directory = load_ticker_directory(config.ticker_directory_path)
    pending = pending_rows(rows, state, config.max_rows)
    progress_log.workflow_loaded(rows, pending, state, local_evidence, config.output_root)
    for start in range(0, len(pending), config.batch_size):
        batch = pending[start : start + config.batch_size]
        progress_log.batch_start(start, batch, pending, evidence_client, submissions_client)
        process_batch(
            batch,
            config,
            state,
            evidence_by_ticker,
            evidence_client,
            submissions_client,
            ticker_directory,
            rows,
        )
        write_state(config.output_root, state)
        summary = write_outputs(config, rows, state, evidence_client, submissions_client)
        progress_log.batch_complete(
            start, batch, pending, summary, evidence_client, submissions_client
        )
    summary = write_outputs(config, rows, state, evidence_client, submissions_client)
    progress_log.outputs_written(summary)
    imported_count = finalize_import(config, summary)
    progress_log.import_complete(config, summary, imported_count)
    return update_import_summary(config, summary, imported_count)


def validate_config(config: HighImpactConfig) -> None:
    if not config.input_path.exists():
        raise FileNotFoundError(f"high-impact input does not exist: {config.input_path}")
    positive = (
        config.batch_size,
        config.timeout_seconds,
        config.retries,
        config.max_row_attempts,
        config.max_docs_per_row,
    )
    if any(value <= 0 for value in positive):
        raise ValueError("batch, timeout, retry, attempt, and document limits must be positive")
    if config.sleep_seconds < 0:
        raise ValueError("--sleep-seconds cannot be negative")
    if not config.user_agent:
        raise ValueError("provide --user-agent or set SEC_USER_AGENT")


def process_batch(
    batch: list[dict[str, str]],
    config: HighImpactConfig,
    state: dict[str, Any],
    evidence_by_ticker: dict[str, list[Any]],
    evidence_client: Any,
    submissions_client: Any,
    ticker_directory: set[tuple[str, str]],
    all_rows: list[dict[str, str]],
) -> None:
    starting_requests = total_requests(evidence_client, submissions_client)
    transport_failures = 0
    for processed_count, row in enumerate(batch, start=1):
        failed = process_with_attempts(
            row,
            config,
            state,
            evidence_by_ticker,
            evidence_client,
            submissions_client,
            ticker_directory,
        )
        transport_failures += int(failed)
        write_state(config.output_root, state)
        progress_log.row_state(row["symbol_era_id"], state)
        requests_used = total_requests(evidence_client, submissions_client) - starting_requests
        if requests_used >= 20 and transport_failures / processed_count > 0.5:
            write_outputs(config, all_rows, state, evidence_client, submissions_client)
            progress_log.circuit_breaker(requests_used, processed_count, transport_failures)
            raise SecTransportError("SEC circuit breaker opened; state is resumable")


def process_with_attempts(
    row: dict[str, str],
    config: HighImpactConfig,
    state: dict[str, Any],
    evidence_by_ticker: dict[str, list[Any]],
    evidence_client: Any,
    submissions_client: Any,
    ticker_directory: set[tuple[str, str]],
) -> bool:
    symbol_era_id = row["symbol_era_id"]
    had_transport_error = False
    while (
        int(state["rows"].get(symbol_era_id, {}).get("attempt_count") or 0)
        < config.max_row_attempts
    ):
        record_attempt(state, symbol_era_id)
        try:
            result, events = resolve_row(
                row,
                config,
                evidence_by_ticker,
                evidence_client,
                submissions_client,
                ticker_directory,
            )
            record_result(state, symbol_era_id, result, events)
            return had_transport_error
        except (SecTransportError, requests.RequestException) as exc:
            had_transport_error = True
            record_transport_error(state, symbol_era_id, exc, config.max_row_attempts)
            progress_log.transport_error(symbol_era_id, state, exc)
    return had_transport_error


def resolve_row(
    row: dict[str, str],
    config: HighImpactConfig,
    evidence_by_ticker: dict[str, list[Any]],
    evidence_client: Any,
    submissions_client: Any,
    ticker_directory: set[tuple[str, str]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    symbol = row["symbol"].upper()
    evidence = list(evidence_by_ticker.get(symbol, []))
    identity = identity_result(
        row,
        evidence,
        days_before=config.identity_days_before,
        days_after=config.identity_days_after,
    )
    if identity["identity_bucket"] == "identity_no_evidence":
        evidence.extend(network_identity(row, config, evidence_client))
        identity = identity_result(
            row,
            deduplicate_evidence(evidence),
            days_before=config.identity_days_before,
            days_after=config.identity_days_after,
        )
    if identity["identity_bucket"] != "identity_verified_ready":
        return ({**identity, **empty_resolution("no_identity_found")}, [])
    anchored = {**row, **identity, "original_last_day": row.get("last_day", "")}
    event_rows, submissions = event_evidence(anchored, config, evidence_client, submissions_client)
    verified = select_verified_event(event_rows)
    if verified:
        return ({**identity, **verified}, event_rows)
    active = active_current_evidence(anchored, submissions, ticker_directory, submissions_client)
    bucket = "active_or_data_gap_hold" if active else "identity_only_hold"
    return ({**identity, **empty_resolution(bucket)}, event_rows)


def network_identity(row: dict[str, str], config: HighImpactConfig, client: Any) -> list[Any]:
    lower, upper = identity_window(row, config.identity_days_before, config.identity_days_after)
    if not lower or not upper:
        return []
    evidence = []
    for params in build_identity_queries(row["symbol"], lower, upper):
        evidence.extend(evidence_from_payload(client.search(params), source="network"))
    return evidence


def pending_rows(
    rows: list[dict[str, str]], state: dict[str, Any], max_rows: int | None
) -> list[dict[str, str]]:
    pending = [
        row
        for row in rows
        if state["rows"].get(row["symbol_era_id"], {}).get("status") not in FINAL_STATUSES
    ]
    return pending[:max_rows] if max_rows else pending


def index_evidence(evidence: list[Any]) -> dict[str, list[Any]]:
    output: dict[str, list[Any]] = {}
    for item in evidence:
        for ticker in item.tickers:
            output.setdefault(ticker, []).append(item)
    return output
