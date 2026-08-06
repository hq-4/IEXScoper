from __future__ import annotations

from pathlib import Path
from typing import Any

from utils.resolution_v2_network import CachedPrimaryClient, NetworkConfig
from utils.resolution_v2_registry import CachePolicy, EvidenceRegistry


class FakeResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        raise AssertionError("json() should not be called when parse_response is supplied")


def _client(tmp_path: Path) -> CachedPrimaryClient:
    registry = EvidenceRegistry(tmp_path / "registry.sqlite")
    config = NetworkConfig(user_agent="test test@example.test", delay_seconds=0)
    return CachedPrimaryClient(config, registry)


def test_get_json_default_behavior_unaffected_by_new_kwargs(tmp_path: Path, monkeypatch) -> None:
    """Existing callers (resolution_v2_sec.py, paged_sec_search) pass no parse_response/
    is_negative — this must still parse as JSON exactly as before."""

    class JsonResponse(FakeResponse):
        def json(self) -> Any:
            return {"hits": {"hits": [{"id": "1"}]}}

    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", lambda *a, **k: JsonResponse("{}")
    )
    client = _client(tmp_path)

    payload, from_cache = client.get_json("test_source", "https://example.test", {}, CachePolicy())

    assert payload == {"hits": {"hits": [{"id": "1"}]}}
    assert from_cache is False


def test_get_json_custom_parser_caches_non_json_response(tmp_path: Path, monkeypatch) -> None:
    calls = []

    def fake_get(url: str, **_: Any) -> FakeResponse:
        calls.append(url)
        return FakeResponse("<atom><cik>0000104599</cik></atom>")

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)
    client = _client(tmp_path)

    def parse_ciks(response: FakeResponse) -> dict[str, Any]:
        import re

        return {"ciks": re.findall(r"<cik>(\d+)</cik>", response.text)}

    first, from_cache = client.get_json(
        "test_xml_source",
        "https://example.test/search",
        {"company": "X"},
        CachePolicy(),
        parse_response=parse_ciks,
        is_negative=lambda p: not p["ciks"],
    )
    second, second_from_cache = client.get_json(
        "test_xml_source",
        "https://example.test/search",
        {"company": "X"},
        CachePolicy(),
        parse_response=parse_ciks,
        is_negative=lambda p: not p["ciks"],
    )

    assert first == {"ciks": ["0000104599"]}
    assert from_cache is False
    assert second == first
    assert second_from_cache is True
    assert len(calls) == 1  # second call was a cache hit, no new request


def test_get_json_custom_is_negative_marks_empty_result(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get", lambda *a, **k: FakeResponse("<atom></atom>")
    )
    client = _client(tmp_path)

    payload, _ = client.get_json(
        "test_xml_source",
        "https://example.test/search",
        {"company": "Nothing"},
        CachePolicy(),
        parse_response=lambda r: {"ciks": []},
        is_negative=lambda p: not p["ciks"],
    )

    assert payload == {"ciks": []}
    row = client.registry.connection.execute(
        "SELECT negative FROM requests WHERE source = ?", ("test_xml_source",)
    ).fetchone()
    assert row[0] == 1
