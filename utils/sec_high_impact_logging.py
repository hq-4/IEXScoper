from __future__ import annotations

from typing import Any

from src.framework.logging import get_logger
from utils.sec_high_impact_runtime import total_requests

LOGGER = get_logger("utils.sec_high_impact_workflow")


def workflow_loaded(
    rows: list[dict[str, str]],
    pending: list[dict[str, str]],
    state: dict[str, Any],
    local_evidence: list[Any],
    output_root: Any,
) -> None:
    LOGGER.info(
        "SEC high-impact identity workflow loaded",
        extra={
            "event": "sec_identity_workflow_loaded",
            "detail": {
                "input_rows": len(rows),
                "pending_rows": len(pending),
                "already_final_rows": len(rows) - len(pending),
                "state_rows": len(state.get("rows", {})),
                "local_evidence_rows": len(local_evidence),
                "workflow_version": state.get("workflow_version"),
                "output_root": str(output_root),
            },
        },
    )


def batch_start(
    start: int,
    batch: list[dict[str, str]],
    pending: list[dict[str, str]],
    evidence_client: Any,
    submissions_client: Any,
) -> None:
    LOGGER.info(
        "SEC high-impact identity batch starting",
        extra={
            "event": "sec_identity_batch_start",
            "detail": batch_detail(start, batch, pending, evidence_client, submissions_client),
        },
    )


def batch_complete(
    start: int,
    batch: list[dict[str, str]],
    pending: list[dict[str, str]],
    summary: dict[str, Any],
    evidence_client: Any,
    submissions_client: Any,
) -> None:
    detail = batch_detail(start, batch, pending, evidence_client, submissions_client)
    detail.update(
        {
            "completed_count": summary["completed_count"],
            "automation_exhausted_count": summary["automation_exhausted_count"],
            "retryable_count": summary["retryable_count"],
            "import_candidate_count": summary["import_candidate_count"],
        }
    )
    LOGGER.info(
        "SEC high-impact identity batch complete",
        extra={"event": "sec_identity_batch_complete", "detail": detail},
    )


def outputs_written(summary: dict[str, Any]) -> None:
    LOGGER.info(
        "SEC high-impact identity resolution outputs written",
        extra={"event": "sec_identity_outputs_written", "detail": summary},
    )


def import_complete(config: Any, summary: dict[str, Any], imported_count: int) -> None:
    LOGGER.info(
        "SEC high-impact identity resolution import step complete",
        extra={
            "event": "sec_identity_import_step_complete",
            "detail": {
                "apply_import": config.apply_import,
                "imported_count": imported_count,
                "retryable_count": summary["retryable_count"],
                "unattempted_count": summary["unattempted_count"],
            },
        },
    )


def circuit_breaker(requests_used: int, processed_count: int, transport_failures: int) -> None:
    LOGGER.error(
        "SEC circuit breaker opened",
        extra={
            "event": "sec_identity_circuit_breaker_open",
            "detail": {
                "requests_used": requests_used,
                "processed_count": processed_count,
                "transport_failures": transport_failures,
            },
        },
    )


def row_state(symbol_era_id: str, state: dict[str, Any]) -> None:
    entry = state["rows"].get(symbol_era_id, {})
    result = entry.get("result", {})
    LOGGER.info(
        "SEC high-impact identity row resolved",
        extra={
            "event": "sec_identity_row_resolved",
            "detail": {
                "symbol_era_id": symbol_era_id,
                "status": entry.get("status"),
                "attempt_count": entry.get("attempt_count"),
                "identity_bucket": result.get("identity_bucket"),
                "resolution_bucket": entry.get("bucket"),
                "importable": result.get("importable", False),
            },
        },
    )


def transport_error(symbol_era_id: str, state: dict[str, Any], error: Exception) -> None:
    entry = state["rows"].get(symbol_era_id, {})
    LOGGER.warning(
        "SEC high-impact identity row transport error",
        extra={
            "event": "sec_identity_row_transport_error",
            "detail": {
                "symbol_era_id": symbol_era_id,
                "status": entry.get("status"),
                "attempt_count": entry.get("attempt_count"),
                "error": repr(error),
            },
        },
    )


def batch_detail(
    start: int,
    batch: list[dict[str, str]],
    pending: list[dict[str, str]],
    evidence_client: Any,
    submissions_client: Any,
) -> dict[str, Any]:
    return {
        "batch_start": start,
        "batch_size": len(batch),
        "pending_rows": len(pending),
        "sec_request_count": total_requests(evidence_client, submissions_client),
    }
