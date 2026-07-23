from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl
import pytest
import requests

from utils.search_edgar_full_text import (
    EdgarFullTextConfig,
    DEFAULT_EVENT_TERMS,
    query_for_symbol,
    search_edgar_full_text,
)


def test_default_query_terms_are_narrow_but_composable() -> None:
    assert DEFAULT_EVENT_TERMS == ("merger",)
    assert query_for_symbol("SQ", ("merger", "delisted")) == "merger OR delisted"


def test_search_edgar_full_text_writes_hit_and_no_hit_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    template_path = tmp_path / "template.csv"
    output_root = tmp_path / "out"
    seen = []
    _write_template(template_path)

    def fake_get(
        url: str, *, params: dict[str, str], headers: dict[str, str], timeout: float
    ) -> FakeResponse:
        seen.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        if params.get("entityName") == "AAA":
            return FakeResponse(_hit_payload())
        return FakeResponse(_empty_payload())

    monkeypatch.setattr("utils.edgar_full_text_client.requests.get", fake_get)

    result = search_edgar_full_text(
        _config(template_path=template_path, output_root=output_root, use_form_filter=True)
    )

    rows = pl.read_csv(output_root / "edgar_full_text_leads.csv", infer_schema_length=0).to_dicts()
    assert seen[0]["headers"]["User-Agent"] == "IEXScoper test admin@example.test"
    assert seen[0]["params"]["entityName"] == "AAA"
    assert seen[0]["params"]["forms"] == "8-K"
    assert seen[0]["params"]["dateRange"] == "custom"
    assert "size" not in seen[0]["params"]
    assert rows[0]["symbol"] == "AAA"
    assert rows[0]["search_status"] == "hit"
    assert rows[0]["entity"] == "AAA CORP"
    assert rows[1]["symbol"] == "BBB"
    assert rows[1]["search_status"] == "no_hits"
    assert result["summary"]["symbols_with_hits"] == 1


def test_search_edgar_full_text_continues_after_symbol_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    template_path = tmp_path / "template.csv"
    output_root = tmp_path / "out"
    _write_template(template_path)

    def fake_get(
        url: str, *, params: dict[str, str], headers: dict[str, str], timeout: float
    ) -> FakeResponse:
        if params.get("entityName") == "AAA":
            raise RuntimeError("SEC 500")
        return FakeResponse(_empty_payload())

    monkeypatch.setattr("utils.edgar_full_text_client.requests.get", fake_get)

    result = search_edgar_full_text(_config(template_path=template_path, output_root=output_root))

    rows = pl.read_csv(output_root / "edgar_full_text_leads.csv", infer_schema_length=0).to_dicts()
    assert [row["search_status"] for row in rows] == ["search_error", "no_hits"]
    assert "SEC 500" in rows[0]["document_url"]
    assert result["summary"]["status_counts"]["search_error"] == 1


def test_search_edgar_full_text_retries_transient_500(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    template_path = tmp_path / "template.csv"
    output_root = tmp_path / "out"
    calls = []
    _write_template(template_path)

    def fake_get(
        url: str, *, params: dict[str, str], headers: dict[str, str], timeout: float
    ) -> FakeResponse:
        calls.append(params)
        if len(calls) == 1:
            return FakeResponse({"message": "Internal server error"}, status_code=500)
        return FakeResponse(_empty_payload())

    monkeypatch.setattr("utils.edgar_full_text_client.requests.get", fake_get)

    result = search_edgar_full_text(
        _config(template_path=template_path, output_root=output_root, symbols=("AAA",), retries=2)
    )

    rows = pl.read_csv(output_root / "edgar_full_text_leads.csv", infer_schema_length=0).to_dicts()
    assert len(calls) == 4
    assert rows[0]["search_status"] == "no_hits"
    assert result["summary"]["status_counts"]["no_hits"] == 1


def test_search_edgar_full_text_retries_transport_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    template_path = tmp_path / "template.csv"
    output_root = tmp_path / "out"
    calls = []
    _write_template(template_path)

    def fake_get(
        url: str, *, params: dict[str, str], headers: dict[str, str], timeout: float
    ) -> FakeResponse:
        calls.append(params)
        if len(calls) == 1:
            raise requests.Timeout("SEC timeout")
        return FakeResponse(_empty_payload())

    monkeypatch.setattr("utils.edgar_full_text_client.requests.get", fake_get)

    result = search_edgar_full_text(
        _config(template_path=template_path, output_root=output_root, symbols=("AAA",), retries=2)
    )

    rows = pl.read_csv(output_root / "edgar_full_text_leads.csv", infer_schema_length=0).to_dicts()
    assert len(calls) == 4
    assert rows[0]["search_status"] == "no_hits"
    assert result["summary"]["status_counts"]["no_hits"] == 1


def test_search_edgar_full_text_falls_back_without_forms(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    template_path = tmp_path / "template.csv"
    output_root = tmp_path / "out"
    calls = []
    _write_template(template_path)

    def fake_get(
        url: str, *, params: dict[str, str], headers: dict[str, str], timeout: float
    ) -> FakeResponse:
        calls.append(params)
        if "forms" in params:
            return FakeResponse({"message": "Internal server error"}, status_code=500)
        return FakeResponse(_empty_payload())

    monkeypatch.setattr("utils.edgar_full_text_client.requests.get", fake_get)

    result = search_edgar_full_text(
        _config(
            template_path=template_path,
            output_root=output_root,
            symbols=("AAA",),
            use_form_filter=True,
        )
    )

    rows = pl.read_csv(output_root / "edgar_full_text_leads.csv", infer_schema_length=0).to_dicts()
    assert "forms" in calls[0]
    assert "forms" not in calls[1]
    assert rows[0]["search_status"] == "no_hits"
    assert result["summary"]["status_counts"]["no_hits"] == 1


def test_search_edgar_full_text_records_request_error_without_traceback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    template_path = tmp_path / "template.csv"
    output_root = tmp_path / "out"
    calls = []
    _write_template(template_path)
    caplog.set_level(logging.INFO)

    def fake_get(
        url: str, *, params: dict[str, str], headers: dict[str, str], timeout: float
    ) -> FakeResponse:
        calls.append(params)
        return FakeResponse({"message": "Internal server error"}, status_code=500)

    monkeypatch.setattr("utils.edgar_full_text_client.requests.get", fake_get)

    result = search_edgar_full_text(
        _config(
            template_path=template_path,
            output_root=output_root,
            symbols=("AAA",),
            use_form_filter=True,
        )
    )

    rows = pl.read_csv(output_root / "edgar_full_text_leads.csv", infer_schema_length=0).to_dicts()
    assert "forms" in calls[0]
    assert "forms" not in calls[1]
    assert rows[0]["search_status"] == "search_error"
    assert result["summary"]["status_counts"]["search_error"] == 1
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


class FakeResponse:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


def _write_template(path: Path) -> None:
    pl.DataFrame(
        {
            "priority_rank": [1, 2],
            "symbol": ["AAA", "BBB"],
            "symbol_era_id": ["AAA#001", "BBB#001"],
            "first_day": ["20200102", "20210104"],
            "last_day": ["20201231", "20211231"],
        }
    ).write_csv(path)


def _config(
    *,
    template_path: Path,
    output_root: Path,
    alias_path: Path | None = None,
    symbols: tuple[str, ...] = (),
    retries: int = 1,
    use_form_filter: bool = False,
    strict_date_bounds: bool = False,
) -> EdgarFullTextConfig:
    return EdgarFullTextConfig(
        template_path=template_path,
        alias_path=alias_path or template_path.with_name("missing_aliases.csv"),
        output_root=output_root,
        endpoint="https://efts.sec.gov/LATEST/search-index",
        symbols=symbols,
        user_agent="IEXScoper test admin@example.test",
        forms=("8-K",),
        use_form_filter=use_form_filter,
        event_terms=("merger",),
        size=5,
        max_symbols=None,
        timeout_seconds=2,
        sleep_seconds=0,
        retries=retries,
        strict_date_bounds=strict_date_bounds,
    )


def _hit_payload() -> dict[str, Any]:
    source = {
        "cik": "0000000001",
        "entity": "AAA CORP",
        "form": "8-K",
        "file_date": "2020-12-31",
        "adsh": "0000000001-20-000001",
        "documentUrl": "https://www.sec.gov/Archives/example.htm",
    }
    return {
        "hits": {
            "total": {"value": 1},
            "hits": [{"_source": source}],
        }
    }


def _empty_payload() -> dict[str, Any]:
    return {"hits": {"total": {"value": 0}, "hits": []}}
