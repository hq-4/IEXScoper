from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import requests

from utils.resolution_v2_network import CachedPrimaryClient, NetworkConfig
from utils.resolution_v2_registry import EvidenceRegistry
from utils.ticker_continuity import (
    CONTINUITY_RENAMED_OR_SUCCESSOR,
    CONTINUITY_SAME_SYMBOL,
    CONTINUITY_TERMINAL,
    STATUS_FETCH_ERROR,
    STATUS_NOT_FOUND,
    STATUS_OK,
    apply_continuity_status,
    fetch_current_tickers,
    fetch_many_current_tickers,
)


class FakeResponse:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            error = requests.HTTPError(f"status {self.status_code}")
            error.response = self  # type: ignore[attr-defined]
            raise error

    def json(self) -> dict[str, Any]:
        return self.payload


def _client(tmp_path: Path, *, retries: int = 3) -> CachedPrimaryClient:
    registry = EvidenceRegistry(tmp_path / "registry.sqlite")
    config = NetworkConfig(user_agent="test test@example.test", delay_seconds=0, retries=retries)
    return CachedPrimaryClient(config, registry)


def test_fetch_current_tickers_returns_tickers_and_exchanges(tmp_path: Path, monkeypatch: Any) -> None:
    def fake_get(url: str, **_: Any) -> FakeResponse:
        assert url == "https://data.sec.gov/submissions/CIK0001390777.json"
        return FakeResponse({"tickers": ["BNY", "BNY-PK"], "exchanges": ["NYSE", "NYSE"]})

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)
    result = fetch_current_tickers(_client(tmp_path), "1390777")

    assert result == {
        "cik": "1390777",
        "current_tickers": ["BNY", "BNY-PK"],
        "current_exchanges": ["NYSE", "NYSE"],
        "fetch_status": STATUS_OK,
        "from_cache": False,
    }


def test_fetch_current_tickers_empty_list_for_delisted(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda *_a, **_k: FakeResponse({"tickers": [], "exchanges": []}),
    )
    result = fetch_current_tickers(_client(tmp_path), "1740915")
    assert result["current_tickers"] == []
    assert result["fetch_status"] == STATUS_OK


def test_fetch_current_tickers_not_found(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda *_a, **_k: FakeResponse({}, status_code=404),
    )
    result = fetch_current_tickers(_client(tmp_path, retries=1), "9999999")
    assert result["fetch_status"] == STATUS_NOT_FOUND
    assert result["current_tickers"] is None


def test_fetch_current_tickers_fetch_error_does_not_raise(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda *_a, **_k: FakeResponse({}, status_code=503),
    )
    result = fetch_current_tickers(_client(tmp_path, retries=1), "1234567")
    assert result["fetch_status"] == STATUS_FETCH_ERROR
    assert result["current_tickers"] is None


def test_fetch_many_current_tickers_continues_past_one_bad_cik(
    tmp_path: Path, monkeypatch: Any
) -> None:
    def fake_get(url: str, **_: Any) -> FakeResponse:
        if "0000000002" in url:
            return FakeResponse({}, status_code=503)
        return FakeResponse({"tickers": ["X"], "exchanges": ["NYSE"]})

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)
    results = fetch_many_current_tickers(_client(tmp_path, retries=1), ["1", "2", "3"])

    statuses = [r["fetch_status"] for r in results]
    assert statuses == [STATUS_OK, STATUS_FETCH_ERROR, STATUS_OK]


CONTINUITY_LOOKUP_SCHEMA = {
    "cik": pl.String,
    "current_tickers": pl.List(pl.String),
}


def test_apply_continuity_status_terminal_when_tickers_empty() -> None:
    enriched = pl.DataFrame({"symbol": ["FTCH"], "resolved_cik": ["1740915"]})
    lookup = pl.DataFrame(
        {"cik": ["1740915"], "current_tickers": [[]]}, schema=CONTINUITY_LOOKUP_SCHEMA
    )
    result = apply_continuity_status(enriched, lookup)
    assert result["continuity_status"].to_list() == [CONTINUITY_TERMINAL]


def test_apply_continuity_status_same_symbol_when_still_trading() -> None:
    enriched = pl.DataFrame({"symbol": ["CORZ"], "resolved_cik": ["1839341"]})
    lookup = pl.DataFrame(
        {"cik": ["1839341"], "current_tickers": [["CORZ", "CORZW"]]},
        schema=CONTINUITY_LOOKUP_SCHEMA,
    )
    result = apply_continuity_status(enriched, lookup)
    assert result["continuity_status"].to_list() == [CONTINUITY_SAME_SYMBOL]


def test_apply_continuity_status_renamed_when_symbol_not_current() -> None:
    enriched = pl.DataFrame({"symbol": ["GPS"], "resolved_cik": ["39911"]})
    lookup = pl.DataFrame(
        {"cik": ["39911"], "current_tickers": [["GAP"]]}, schema=CONTINUITY_LOOKUP_SCHEMA
    )
    result = apply_continuity_status(enriched, lookup)
    assert result["continuity_status"].to_list() == [CONTINUITY_RENAMED_OR_SUCCESSOR]


def test_apply_continuity_status_null_when_no_cik_resolved() -> None:
    enriched = pl.DataFrame(
        {"symbol": ["ZZZZ"], "resolved_cik": [None]}, schema={"symbol": pl.String, "resolved_cik": pl.String}
    )
    lookup = pl.DataFrame(schema=CONTINUITY_LOOKUP_SCHEMA)
    result = apply_continuity_status(enriched, lookup)
    assert result["continuity_status"].to_list() == [None]


def test_apply_continuity_status_null_when_fetch_never_happened_for_resolved_cik() -> None:
    """A resolved CIK whose continuity fetch wasn't run (e.g. --skip-fetch) stays null,
    not a false 'terminal' guess."""
    enriched = pl.DataFrame({"symbol": ["AAA"], "resolved_cik": ["123"]})
    lookup = pl.DataFrame(schema=CONTINUITY_LOOKUP_SCHEMA)
    result = apply_continuity_status(enriched, lookup)
    assert result["continuity_status"].to_list() == [None]
