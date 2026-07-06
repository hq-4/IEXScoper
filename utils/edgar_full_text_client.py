from __future__ import annotations

import time

import requests

from src.framework.logging import get_logger
from utils.edgar_full_text_schema import RETRY_STATUS_CODES
from utils.search_edgar_full_text_types import EdgarFullTextConfig


def request_with_retries(config: EdgarFullTextConfig, params: dict[str, str]) -> requests.Response:
    last_error = None
    for attempt in range(1, config.retries + 1):
        response = requests.get(
            config.endpoint,
            params=params,
            headers={"User-Agent": config.user_agent, "Accept": "application/json"},
            timeout=config.timeout_seconds,
        )
        try:
            response.raise_for_status()
            return response
        except requests.HTTPError as exc:
            last_error = exc
            status_code = response.status_code
            if status_code not in RETRY_STATUS_CODES or attempt == config.retries:
                raise
            get_logger(__name__).warning(
                "EDGAR full text retryable response",
                extra={
                    "event": "edgar_full_text_retry",
                    "detail": {"status_code": status_code, "attempt": attempt, "params": params},
                },
            )
            time.sleep(config.sleep_seconds * attempt)
    assert last_error is not None
    raise last_error
