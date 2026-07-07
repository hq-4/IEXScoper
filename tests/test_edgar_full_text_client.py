import logging
from pathlib import Path

import pytest
import requests

from utils.edgar_full_text_client import (
    request_with_retries,
    search_param_variants,
    ticker_and_query,
)
from utils.search_edgar_full_text_types import EdgarFullTextConfig


def test_search_param_variants_drop_brittle_filters_and_dates() -> None:
    target = {
        "symbol": "SQ",
        "first_day": "20161212",
        "last_day": "20250117",
        "edgar_aliases": ("Block",),
    }

    variants = search_param_variants(_config(use_form_filter=True), target, "merger")

    labels = [label for label, params in variants]
    params = [params for label, params in variants]
    assert labels == [
        "primary",
        "without_forms",
        "without_dates",
        "alias_and_query",
        "ticker_and_query",
        "ticker_in_query",
    ]
    assert params[0]["forms"] == "8-K"
    assert "forms" not in params[1]
    assert "startdt" not in params[2]
    assert params[3] == {"q": "Block AND merger"}
    assert params[4] == {"q": "SQ AND merger"}
    assert params[5] == {"q": '"SQ" merger'}


def test_search_param_variants_are_deduplicated_without_date_bounds() -> None:
    target = {"symbol": "SQ", "first_day": None, "last_day": None}

    variants = search_param_variants(_config(use_form_filter=False), target, "merger")

    assert variants == [
        ("primary", {"q": "merger", "entityName": "SQ"}),
        ("ticker_and_query", {"q": "SQ AND merger"}),
        ("ticker_in_query", {"q": '"SQ" merger'}),
    ]


def test_ticker_and_query_wraps_or_terms() -> None:
    assert ticker_and_query("SQ", "merger OR acquisition") == "SQ AND (merger OR acquisition)"


def test_request_retries_do_not_emit_info_noise(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    calls = []
    caplog.set_level(logging.INFO)

    def fake_get(
        url: str, *, params: dict[str, str], headers: dict[str, str], timeout: float
    ) -> FakeResponse:
        calls.append(params)
        status = 500 if len(calls) == 1 else 200
        return FakeResponse(status)

    monkeypatch.setattr("utils.edgar_full_text_client.requests.get", fake_get)

    request_with_retries(_config(use_form_filter=False, retries=2), {"q": "merger"})

    assert len(calls) == 2
    assert not caplog.records


class FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")


def _config(*, use_form_filter: bool, retries: int = 1) -> EdgarFullTextConfig:
    return EdgarFullTextConfig(
        template_path=Path("unused.csv"),
        alias_path=Path("unused_aliases.csv"),
        output_root=Path("unused"),
        endpoint="https://efts.sec.gov/LATEST/search-index",
        symbols=("SQ",),
        user_agent="IEXScoper test admin@example.test",
        forms=("8-K",),
        use_form_filter=use_form_filter,
        event_terms=("merger",),
        size=5,
        max_symbols=None,
        timeout_seconds=2,
        sleep_seconds=0,
        retries=retries,
    )
