from __future__ import annotations

import time

import requests

from src.framework.logging import get_logger
from utils.edgar_full_text_schema import RETRY_STATUS_CODES
from utils.search_edgar_full_text_types import EdgarFullTextConfig


def search_params(
    config: EdgarFullTextConfig, target: dict[str, object], query: str, *, include_forms: bool
) -> dict[str, str]:
    params = {
        "q": query,
        "entityName": str(target["symbol"]).upper(),
    }
    if include_forms:
        params["forms"] = ",".join(config.forms)
    startdt = date_arg(target.get("first_day"))
    enddt = date_arg(target.get("last_day"))
    if startdt:
        params["startdt"] = startdt
    if enddt:
        params["enddt"] = enddt
    if startdt or enddt:
        params["dateRange"] = "custom"
    return params


def date_arg(value: object) -> str | None:
    text = str(value or "")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return None


def request_with_retries(config: EdgarFullTextConfig, params: dict[str, str]) -> requests.Response:
    last_error = None
    for attempt in range(1, config.retries + 1):
        try:
            response = requests.get(
                config.endpoint,
                params=params,
                headers={"User-Agent": config.user_agent, "Accept": "application/json"},
                timeout=config.timeout_seconds,
            )
            response.raise_for_status()
            return response
        except requests.HTTPError as exc:
            last_error = exc
            status_code = response.status_code
            if status_code not in RETRY_STATUS_CODES or attempt == config.retries:
                raise
            get_logger(__name__).info(
                "EDGAR full text retryable response",
                extra={
                    "event": "edgar_full_text_retry",
                    "detail": {"status_code": status_code, "attempt": attempt, "params": params},
                },
            )
            time.sleep(config.sleep_seconds * attempt)
        except requests.RequestException as exc:
            last_error = exc
            if attempt == config.retries:
                raise
            get_logger(__name__).info(
                "EDGAR full text retryable transport error",
                extra={
                    "event": "edgar_full_text_retry",
                    "detail": {"error": repr(exc), "attempt": attempt, "params": params},
                },
            )
            time.sleep(config.sleep_seconds * attempt)
    assert last_error is not None
    raise last_error
