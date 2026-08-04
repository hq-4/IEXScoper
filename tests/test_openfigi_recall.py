from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from utils.openfigi_recall_experiment import (
    build_sample,
    build_variant_job,
    load_recall_cache,
    load_unmatched_symbols,
    run_mapping_variant,
)
from utils.openfigi_recall_metrics import (
    build_report,
    name_plausible,
    split_us_matches,
    variant_report,
)


class FakeRecallClient:
    def __init__(self) -> None:
        self.calls: list[list[dict[str, Any]]] = []

    def map_jobs(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        self.calls.append(jobs)
        return [
            {"data": [{"figi": f"BBG-{job['idValue']}", "exchCode": "UN", "name": "X Corp"}]}
            for job in jobs
        ]


def test_build_variant_job_constraints() -> None:
    assert build_variant_job("AAA", "no_market_sector") == {
        "idType": "TICKER",
        "idValue": "AAA",
        "exchCode": "US",
    }
    assert build_variant_job("AAA", "no_exch_code") == {
        "idType": "TICKER",
        "idValue": "AAA",
        "marketSecDes": "Equity",
    }
    assert build_variant_job("AAA", "bare_ticker") == {"idType": "TICKER", "idValue": "AAA"}
    assert build_variant_job("AAA", "unlisted") == {
        "idType": "TICKER",
        "idValue": "AAA",
        "includeUnlistedEquities": True,
    }


def test_load_unmatched_symbols_and_sample_priority(tmp_path: Path) -> None:
    cache_path = tmp_path / "cache.jsonl"
    lines = [
        {"symbol": "HIT", "response": {"data": [{"figi": "BBG1"}]}},
        {"symbol": "GT1", "response": {}},
        {"symbol": "ZZZ", "response": {"error": "x"}},
        {"symbol": "YYY", "response": {"data": []}},
    ]
    cache_path.write_text("\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8")
    unmatched = load_unmatched_symbols(cache_path)
    assert unmatched == ["GT1", "YYY", "ZZZ"]

    sample = build_sample(unmatched, {"GT1": "Gt One Inc"}, size=2, seed=7)
    assert sample[0] == "GT1"
    assert len(sample) == 2
    assert build_sample(unmatched, {"GT1": "Gt One Inc"}, 2, 7) == sample


def test_run_mapping_variant_caches_and_resumes(tmp_path: Path) -> None:
    cache_path = tmp_path / "recall.jsonl"
    cache: dict[str, dict[str, Any]] = {}
    client = FakeRecallClient()
    run_mapping_variant(["AAA", "BBB"], "bare_ticker", client, cache, cache_path, 0)
    assert len(client.calls) == 1
    assert cache["AAA|bare_ticker"]["data"][0]["figi"] == "BBG-AAA"

    reloaded = load_recall_cache(cache_path)
    second = FakeRecallClient()
    run_mapping_variant(["AAA", "BBB"], "bare_ticker", second, reloaded, cache_path, 0)
    assert second.calls == []


def test_split_us_matches_and_name_plausibility() -> None:
    response = {
        "data": [
            {"figi": "US1", "exchCode": "UN"},
            {"figi": "FR1", "exchCode": "FP"},
        ]
    }
    us, noise = split_us_matches(response, "bare_ticker")
    assert [m["figi"] for m in us] == ["US1"]
    assert [m["figi"] for m in noise] == ["FR1"]
    us_all, noise_none = split_us_matches(response, "no_market_sector")
    assert len(us_all) == 2 and noise_none == []

    assert name_plausible("WHWK BRINGS CLO MKT FLX", "Whitehawk Therapeutics, Inc.") is False
    assert name_plausible("WHITEHAWK THERAPEUTICS INC", "Whitehawk Therapeutics, Inc.") is True
    assert name_plausible(None, "Aaron's Company, Inc.") is False


def test_variant_report_and_recommendation() -> None:
    sample = ["GT1", "ZZZ"]
    cache = {
        "GT1|bare_ticker": {"data": [{"figi": "US1", "exchCode": "UN", "name": "GT ONE HOLDINGS"}]},
        "ZZZ|bare_ticker": {"data": [{"figi": "FR1", "exchCode": "FP", "name": "ZZZ SA"}]},
        "GT1|search_endpoint": {"data": []},
        "ZZZ|search_endpoint": {"data": []},
    }
    truth = {"GT1": "GT One Corp"}

    report = variant_report(sample, "bare_ticker", cache, truth)
    assert report["recalled_symbols"] == 1
    assert report["recall_rate"] == 0.5
    assert report["noise_only_symbols"] == 1
    assert report["ground_truth"] == {
        "subset_size": 1,
        "matched": 1,
        "name_plausible": 1,
        "name_plausible_rate": 1.0,
    }

    full = build_report(sample, cache, truth, ("bare_ticker", "search_endpoint"), 0.1)
    assert full["best_variant"] == "bare_ticker"
    assert full["lift_vs_baseline"] == 5.0
    assert "Run full pass" in full["recommendation"]
