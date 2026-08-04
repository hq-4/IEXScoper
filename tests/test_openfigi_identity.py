from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests

from utils.openfigi_identity_core import (
    IdentityConfig,
    build_openfigi_job,
    build_summary,
    build_symbol_map_rows,
    classify_instrument,
    collect_identity_data,
    map_with_fallback,
)


class FakeIdentityClient:
    def __init__(self) -> None:
        self.calls: list[list[dict[str, str]]] = []

    def map_jobs(self, jobs: list[dict[str, str]]) -> list[dict[str, Any]]:
        self.calls.append(jobs)
        return [_response_for(job["idValue"]) for job in jobs]


class FlakyBatchClient:
    """Fails multi-job batches; succeeds for single jobs."""

    def __init__(self) -> None:
        self.calls: list[list[dict[str, str]]] = []

    def map_jobs(self, jobs: list[dict[str, str]]) -> list[dict[str, Any]]:
        self.calls.append(jobs)
        if len(jobs) > 1:
            raise requests.HTTPError("batch poisoned")
        if jobs[0]["idValue"] == "DEAD":
            raise requests.HTTPError("permanent failure")
        return [_response_for(jobs[0]["idValue"])]


def test_build_openfigi_job_uses_ticker_us_equity() -> None:
    config = _config(Path("/tmp/in.jsonl"), Path("/tmp/out"))

    assert build_openfigi_job("AAAP", config) == {
        "idType": "TICKER",
        "idValue": "AAAP",
        "exchCode": "US",
        "marketSecDes": "Equity",
    }


def test_classify_instrument_maps_security_types() -> None:
    assert classify_instrument("ETP") == "fund_etf"
    assert classify_instrument("Mutual Fund") == "fund_etf"
    assert classify_instrument("Common Stock") == "equity_common"
    assert classify_instrument("ADR") == "adr"
    assert classify_instrument("Depositary Receipt") == "adr"
    assert classify_instrument("REIT") == "reit"
    assert classify_instrument("Preferred") == "preferred"
    assert classify_instrument("Unit") == "unit"
    assert classify_instrument("Right") == "right"
    assert classify_instrument("Warrant") == "warrant"
    assert classify_instrument(None, None, "Fund") == "fund_etf"
    assert classify_instrument("Partnership Shares") == "other_partnership shares"
    assert classify_instrument(None, "Common Stock") == "other_common stock"
    assert classify_instrument(None) == "other"


def test_symbol_map_rows_cover_single_multi_unmatched_error() -> None:
    responses = {
        "SINGLE": {"data": [_mapping("SINGLE")]},
        "MULTI": {"data": [_mapping("MULTI"), _mapping("MULTI", suffix="2")]},
        "MISS": {},
        "BADJOB": {"error": "Bad request"},
    }
    rows = build_symbol_map_rows(["SINGLE", "MULTI", "MISS", "BADJOB"], responses)

    assert [row["match_status"] for row in rows] == [
        "single",
        "multi",
        "multi",
        "unmatched",
        "error",
    ]
    single = rows[0]
    assert single["figi"] == "BBG-SINGLE"
    assert single["composite_figi"] == "BBG-COMP-SINGLE"
    assert single["figi_count"] == 1
    assert single["openfigi_class"] == "equity_common"
    multi = [row for row in rows if row["symbol"] == "MULTI"]
    assert {row["figi"] for row in multi} == {"BBG-MULTI", "BBG-MULTI2"}
    assert all(row["figi_count"] == 2 for row in multi)
    unmatched = rows[3]
    assert unmatched["figi"] is None and unmatched["figi_count"] == 0
    assert rows[4]["error"] == "Bad request"


def test_map_with_fallback_retries_singly_and_records_failures() -> None:
    client = FlakyBatchClient()
    jobs = [
        {"idType": "TICKER", "idValue": "SINGLE"},
        {"idType": "TICKER", "idValue": "DEAD"},
    ]

    responses = map_with_fallback(client, jobs)

    assert responses[0]["data"][0]["figi"] == "BBG-SINGLE"
    assert responses[1]["error"].startswith("request_failed")
    assert len(client.calls) == 3


def test_collect_identity_data_resumes_from_cache(tmp_path: Path) -> None:
    input_path = tmp_path / "observation_facts.jsonl"
    _write_facts(input_path, ["CACHED", "SINGLE", "MULTI", "MISS"])
    cache_path = tmp_path / "cache" / "identity_cache.jsonl"
    cache_path.parent.mkdir(parents=True)
    cache_path.write_text(
        json.dumps({"symbol": "CACHED", "response": {"data": [_mapping("CACHED")]}}) + "\n",
        encoding="utf-8",
    )
    config = _config(input_path, tmp_path / "out", cache_path=cache_path)
    client = FakeIdentityClient()

    result = collect_identity_data(config, client)

    requested = {job["idValue"] for call in client.calls for job in call}
    assert requested == {"SINGLE", "MULTI", "MISS"}
    summary = result["summary"]
    assert summary["total_symbols"] == 4
    assert summary["single_figi_symbols"] == 2
    assert summary["multi_figi_symbols"] == 1
    assert summary["unmatched_symbols"] == 1
    assert summary["total_figi_matches"] == 4

    second_client = FakeIdentityClient()
    collect_identity_data(config, second_client)
    assert second_client.calls == []


def test_era_join_and_summary_counts(tmp_path: Path) -> None:
    input_path = tmp_path / "observation_facts.jsonl"
    _write_facts(input_path, ["SINGLE", "MULTI", "MISS"], eras_per_symbol=2)
    config = _config(input_path, tmp_path / "out", cache_path=tmp_path / "cache.jsonl")

    result = collect_identity_data(config, FakeIdentityClient())

    era_rows = result["era_rows"]
    assert len(era_rows) == 6
    multi_era = next(row for row in era_rows if row["symbol"] == "MULTI")
    assert multi_era["match_status"] == "multi"
    assert multi_era["figi_count"] == 2
    assert multi_era["openfigi_class"] == "equity_common"
    assert multi_era["best_name"] == "MULTI Corp"
    miss_era = next(row for row in era_rows if row["symbol"] == "MISS")
    assert miss_era["match_status"] == "unmatched"
    assert miss_era["openfigi_class"] is None

    summary = build_summary(config, result["symbols"], result["map_rows"])
    assert summary["matched_symbols"] == 2
    assert summary["class_distribution"] == {"equity_common": 2, "unmatched": 1}
    assert summary["top_security_type2"] == {"Common Stock": 3}


def _config(input_path: Path, output_root: Path, cache_path: Path | None = None) -> IdentityConfig:
    return IdentityConfig(
        input_path=input_path,
        output_root=output_root,
        api_key=None,
        cache_path=cache_path or Path("/tmp/identity_cache.jsonl"),
        batch_size=2,
        sleep_seconds=0,
    )


def _mapping(symbol: str, suffix: str = "") -> dict[str, str]:
    return {
        "figi": f"BBG-{symbol}{suffix}",
        "compositeFIGI": f"BBG-COMP-{symbol}{suffix}",
        "ticker": symbol,
        "name": f"{symbol}{suffix} Corp",
        "exchCode": "US",
        "securityType": "Common Stock",
        "securityType2": "Common Stock",
        "marketSector": "Equity",
        "securityDescription": f"{symbol} description",
    }


def _response_for(symbol: str) -> dict[str, Any]:
    if symbol == "MISS":
        return {}
    if symbol == "BADJOB":
        return {"error": "Bad request"}
    if symbol == "MULTI":
        return {"data": [_mapping("MULTI"), _mapping("MULTI", suffix="2")]}
    return {"data": [_mapping(symbol)]}


def _write_facts(path: Path, symbols: list[str], eras_per_symbol: int = 1) -> None:
    lines = []
    for symbol in symbols:
        for era in range(1, eras_per_symbol + 1):
            lines.append(
                json.dumps(
                    {
                        "symbol": symbol,
                        "symbol_era_id": f"{symbol}#{era:03d}",
                        "first_day": "20170101",
                        "last_day": "20180101",
                        "gap_status": "delisted_or_acquired_candidate",
                    }
                )
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
