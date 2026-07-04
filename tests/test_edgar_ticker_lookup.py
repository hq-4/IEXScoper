from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from utils.lookup_edgar_tickers import (
    EdgarLookupConfig,
    lookup_edgar_tickers,
    resolve_user_agent,
)


def test_lookup_edgar_tickers_uses_custom_user_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = tmp_path / "edgar"
    seen_headers = []

    def fake_get(url: str, *, headers: dict[str, str], timeout: float) -> FakeResponse:
        seen_headers.append(headers)
        assert timeout == 3
        return FakeResponse(_sec_payload())

    monkeypatch.setattr("utils.lookup_edgar_tickers.requests.get", fake_get)

    result = lookup_edgar_tickers(
        EdgarLookupConfig(
            template_path=None,
            output_root=output_root,
            cache_path=output_root / "company_tickers_exchange.json",
            symbols=("AAPL", "MISS"),
            user_agent="IEXScoper test admin@example.test",
            timeout_seconds=3,
            sleep_seconds=0,
            fetch_submissions=False,
            refresh=True,
        )
    )

    rows = {
        row["symbol"]: row
        for row in pl.read_csv(
            output_root / "edgar_ticker_lookup.csv", infer_schema_length=0
        ).to_dicts()
    }
    assert seen_headers[0]["User-Agent"] == "IEXScoper test admin@example.test"
    assert rows["AAPL"]["edgar_lookup_status"] == "edgar_current_ticker_match"
    assert rows["MISS"]["edgar_lookup_status"] == "edgar_current_ticker_unmatched"
    assert result["summary"]["status_counts"]["edgar_current_ticker_match"] == 1


def test_resolve_user_agent_requires_explicit_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    with pytest.raises(ValueError, match="SEC_USER_AGENT"):
        resolve_user_agent(None)


class FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


def _sec_payload() -> dict[str, object]:
    return {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [320193, "Apple Inc.", "AAPL", "Nasdaq"],
        ],
    }
