from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import requests

from utils.resolution_v2_network import CachedPrimaryClient, NetworkConfig
from utils.resolution_v2_registry import CachePolicy, EvidenceRegistry
from utils.sec_sic_client import (
    STATUS_FETCH_ERROR,
    STATUS_NO_SIC,
    STATUS_NOT_FOUND,
    STATUS_OK,
    SEC_SUBMISSIONS_SOURCE,
    fetch_many,
    fetch_sic,
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


def _client(tmp_path: Path) -> CachedPrimaryClient:
    registry = EvidenceRegistry(tmp_path / "registry.sqlite")
    config = NetworkConfig(user_agent="test test@example.test", delay_seconds=0, retries=3)
    return CachedPrimaryClient(config, registry)


def test_fetch_sic_returns_sic_and_description_on_success(tmp_path, monkeypatch) -> None:
    def fake_get(url: str, **_: Any) -> FakeResponse:
        assert url == "https://data.sec.gov/submissions/CIK0001512673.json"
        return FakeResponse(
            {
                "sic": "7372",
                "sicDescription": "Services-Prepackaged Software",
                "name": "Block, Inc.",
            }
        )

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)
    result = fetch_sic(_client(tmp_path), "1512673")

    assert result == {
        "cik": "1512673",
        "sic": "7372",
        "sic_description": "Services-Prepackaged Software",
        "entity_name": "Block, Inc.",
        "former_names": [],
        "fetch_status": STATUS_OK,
        "from_cache": False,
    }


def test_fetch_sic_extracts_former_names(tmp_path, monkeypatch) -> None:
    """SEC's submissions payload carries a `formerNames` array for any registrant that
    has renamed — real shape from a live payload (Cabot Oil & Gas Corp -> Coterra
    Energy Inc.), just the name strings extracted since callers compare by name."""
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda url, **_: FakeResponse(
            {
                "sic": "1311",
                "sicDescription": "Crude Petroleum & Natural Gas",
                "name": "Coterra Energy Inc.",
                "formerNames": [
                    {
                        "name": "CABOT OIL & GAS CORP",
                        "from": "1994-05-12T04:00:00.000Z",
                        "to": "2021-09-29T04:00:00.000Z",
                    }
                ],
            }
        ),
    )
    result = fetch_sic(_client(tmp_path), "858470")

    assert result["former_names"] == ["CABOT OIL & GAS CORP"]


def test_fetch_sic_missing_former_names_is_empty_list(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda url, **_: FakeResponse({"sic": "7372", "sicDescription": "S", "name": "N"}),
    )
    result = fetch_sic(_client(tmp_path), "1")

    assert result["former_names"] == []


def test_fetch_sic_blank_sic_is_no_sic_on_record(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda url, **_: FakeResponse({"sic": "", "sicDescription": "", "name": "Shell Co"}),
    )
    result = fetch_sic(_client(tmp_path), "1")

    assert result["fetch_status"] == STATUS_NO_SIC
    assert result["sic"] is None


def test_fetch_sic_second_call_is_a_cache_hit(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_get(url: str, **_: Any) -> FakeResponse:
        calls.append(url)
        return FakeResponse({"sic": "7372", "sicDescription": "Software", "name": "X"})

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)
    client = _client(tmp_path)

    first = fetch_sic(client, "1512673")
    second = fetch_sic(client, "1512673")

    assert len(calls) == 1
    assert first["from_cache"] is False
    assert second["from_cache"] is True
    assert second["sic"] == "7372"


def test_fetch_sic_cache_is_shared_across_separate_client_instances(tmp_path, monkeypatch) -> None:
    """The whole point of reusing EvidenceRegistry: a CIK the live SEC-lane resolver
    already fetched (a separate process, a separate CachedPrimaryClient instance) is a
    free cache hit here, as long as both point at the same registry path."""
    calls = []

    def fake_get(url: str, **_: Any) -> FakeResponse:
        calls.append(url)
        return FakeResponse({"sic": "6798", "sicDescription": "REIT", "name": "Y"})

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)
    registry_path = tmp_path / "shared_registry.sqlite"

    first_client = CachedPrimaryClient(
        NetworkConfig(user_agent="a b@c.test", delay_seconds=0), EvidenceRegistry(registry_path)
    )
    fetch_sic(first_client, "1287865")

    second_client = CachedPrimaryClient(
        NetworkConfig(user_agent="a b@c.test", delay_seconds=0), EvidenceRegistry(registry_path)
    )
    result = fetch_sic(second_client, "1287865")

    assert len(calls) == 1
    assert result["from_cache"] is True
    assert result["sic"] == "6798"


def test_request_shape_matches_resolution_v2_sec_call(tmp_path, monkeypatch) -> None:
    """utils.resolution_v2_sec.resolve_known_identity_event calls client.get_json exactly
    like this; if either module's shape drifts, cache sharing silently breaks."""
    calls = []
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda url, **_: (
            calls.append(url) or FakeResponse({"sic": "7372", "sicDescription": "S", "name": "N"})
        ),
    )
    client = _client(tmp_path)

    # Exactly the call resolution_v2_sec.py makes (source, url, params, policy).
    client.get_json(
        SEC_SUBMISSIONS_SOURCE,
        "https://data.sec.gov/submissions/CIK0000000042.json",
        {},
        CachePolicy(max_age=timedelta(days=30)),
    )

    result = fetch_sic(client, "42")

    assert len(calls) == 1  # fetch_sic reused the row resolution_v2_sec-style code wrote
    assert result["from_cache"] is True


def test_fetch_sic_404_is_cik_not_found_without_retry(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_get(url: str, **_: Any) -> FakeResponse:
        calls.append(url)
        return FakeResponse({}, status_code=404)

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)
    result = fetch_sic(_client(tmp_path), "999999999")

    assert result["fetch_status"] == STATUS_NOT_FOUND
    assert len(calls) == 1  # 404 is not in RETRYABLE, fails fast


def test_fetch_sic_429_retries_then_succeeds(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_get(url: str, **_: Any) -> FakeResponse:
        calls.append(url)
        if len(calls) == 1:
            return FakeResponse({}, status_code=429)
        return FakeResponse({"sic": "2836", "sicDescription": "Biological Products", "name": "Z"})

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)
    monkeypatch.setattr("utils.resolution_v2_network.time.sleep", lambda *_: None)
    result = fetch_sic(_client(tmp_path), "7")

    assert len(calls) == 2
    assert result["fetch_status"] == STATUS_OK
    assert result["sic"] == "2836"


def test_fetch_sic_retries_exhausted_is_fetch_error(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "utils.resolution_v2_network.requests.get",
        lambda url, **_: FakeResponse({}, status_code=503),
    )
    monkeypatch.setattr("utils.resolution_v2_network.time.sleep", lambda *_: None)
    result = fetch_sic(_client(tmp_path), "8")

    assert result["fetch_status"] == STATUS_FETCH_ERROR


def test_fetch_many_continues_past_individual_errors(tmp_path, monkeypatch) -> None:
    def fake_get(url: str, **_: Any) -> FakeResponse:
        if "0000000001" in url:
            return FakeResponse({}, status_code=404)
        return FakeResponse({"sic": "7372", "sicDescription": "Software", "name": "Good Co"})

    monkeypatch.setattr("utils.resolution_v2_network.requests.get", fake_get)
    results = fetch_many(_client(tmp_path), ["1", "2"])

    statuses = {row["cik"]: row["fetch_status"] for row in results}
    assert statuses == {"1": STATUS_NOT_FOUND, "2": STATUS_OK}
