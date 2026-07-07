from pathlib import Path
from typing import Any

import polars as pl
import pytest
import requests

from utils.edgar_full_text_targets import load_aliases
from utils.search_edgar_full_text_types import EdgarFullTextConfig
from utils.search_edgar_full_text import search_edgar_full_text


def test_search_edgar_full_text_tries_alias_after_zero_ticker_hits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    template_path = tmp_path / "template.csv"
    alias_path = tmp_path / "aliases.csv"
    output_root = tmp_path / "out"
    calls = []
    write_template(template_path)
    alias_path.write_text("symbol,alias,note\nAAA,Alpha Alias,test\n")

    def fake_get(
        url: str, *, params: dict[str, str], headers: dict[str, str], timeout: float
    ) -> FakeResponse:
        calls.append(params)
        if params["q"] == "Alpha Alias AND merger":
            return FakeResponse(_hit_payload())
        return FakeResponse(_empty_payload())

    monkeypatch.setattr("utils.edgar_full_text_client.requests.get", fake_get)

    result = search_edgar_full_text(
        _config(
            template_path=template_path,
            alias_path=alias_path,
            output_root=output_root,
            symbols=("AAA",),
        )
    )

    rows = pl.read_csv(output_root / "edgar_full_text_leads.csv", infer_schema_length=0).to_dicts()
    assert calls[-1]["q"] == "Alpha Alias AND merger"
    assert rows[0]["search_status"] == "hit"
    assert rows[0]["query"] == "Alpha Alias AND merger"
    assert result["summary"]["symbols_with_hits"] == 1


def test_search_edgar_full_text_omits_form_filter_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    template_path = tmp_path / "template.csv"
    output_root = tmp_path / "out"
    calls = []
    write_template(template_path)

    def fake_get(
        url: str, *, params: dict[str, str], headers: dict[str, str], timeout: float
    ) -> FakeResponse:
        calls.append(params)
        return FakeResponse(_empty_payload())

    monkeypatch.setattr("utils.edgar_full_text_client.requests.get", fake_get)

    search_edgar_full_text(
        _config(template_path=template_path, output_root=output_root, symbols=("AAA",))
    )

    assert "forms" not in calls[0]


def test_load_aliases_deduplicates_by_symbol(tmp_path: Path) -> None:
    path = tmp_path / "aliases.csv"
    path.write_text("symbol,alias,note\naaa,Alpha,test\nAAA,Alpha,duplicate\nAAA,Beta,test\n")

    assert load_aliases(path) == {"AAA": ("Alpha", "Beta")}


def write_template(path: Path) -> None:
    pl.DataFrame(
        {
            "priority_rank": [1],
            "symbol": ["AAA"],
            "symbol_era_id": ["AAA#001"],
            "first_day": ["20200102"],
            "last_day": ["20201231"],
        }
    ).write_csv(path)


class FakeResponse:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")

    def json(self) -> dict[str, Any]:
        return self._payload


def _config(
    *,
    template_path: Path,
    alias_path: Path | None = None,
    output_root: Path,
    symbols: tuple[str, ...],
) -> EdgarFullTextConfig:
    return EdgarFullTextConfig(
        template_path=template_path,
        alias_path=alias_path or template_path.with_name("missing_aliases.csv"),
        output_root=output_root,
        endpoint="https://efts.sec.gov/LATEST/search-index",
        symbols=symbols,
        user_agent="IEXScoper test admin@example.test",
        forms=("8-K",),
        use_form_filter=False,
        event_terms=("merger",),
        size=5,
        max_symbols=None,
        timeout_seconds=2,
        sleep_seconds=0,
        retries=1,
    )


def _hit_payload() -> dict[str, Any]:
    return {
        "hits": {
            "total": {"value": 1},
            "hits": [{"_source": {"entity": "AAA CORP", "form": "8-K"}}],
        }
    }


def _empty_payload() -> dict[str, Any]:
    return {"hits": {"total": {"value": 0}, "hits": []}}
