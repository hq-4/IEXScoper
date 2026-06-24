from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from utils.openfigi_enrichment_core import (
    OpenFigiConfig,
    build_enriched_row,
    build_openfigi_job,
    enrich_symbol_stability,
)


class FakeOpenFigiClient:
    def __init__(self) -> None:
        self.calls: list[list[dict[str, str]]] = []

    def map_jobs(self, jobs: list[dict[str, str]]) -> list[dict[str, Any]]:
        self.calls.append(jobs)
        responses: list[dict[str, Any]] = []
        for job in jobs:
            symbol = job["idValue"]
            if symbol == "MISS":
                responses.append({})
            elif symbol == "MULTI":
                responses.append({"data": [_mapping(symbol), _mapping(symbol, suffix=" B")]})
            else:
                responses.append({"data": [_mapping(symbol)]})
        return responses


def test_build_openfigi_job_uses_ticker_us_equity() -> None:
    config = _config(Path("/tmp/in.csv"), Path("/tmp/out"))

    assert build_openfigi_job("C", config) == {
        "idType": "TICKER",
        "idValue": "C",
        "exchCode": "US",
        "marketSecDes": "Equity",
    }


def test_build_enriched_row_flags_match_and_multiple() -> None:
    row = {"symbol": "C", "classification": "stable_candidate"}

    enriched = build_enriched_row(row, {"data": [_mapping("C")]})
    assert enriched["openfigi_status"] == "matched"
    assert enriched["identity_risk"] == "stable_candidate_with_match"

    multiple = build_enriched_row(row, {"data": [_mapping("C"), _mapping("C", suffix=" B")]})
    assert multiple["openfigi_status"] == "multiple_matches"
    assert multiple["identity_risk"] == "multiple_openfigi_matches"


def test_enrich_symbol_stability_writes_outputs_and_uses_cache(tmp_path: Path) -> None:
    input_path = tmp_path / "symbol_stability_rows.csv"
    _write_audit_rows(input_path, ["C", "MISS", "MULTI"])
    config = _config(input_path, tmp_path / "out")
    client = FakeOpenFigiClient()

    result = enrich_symbol_stability(config, client)

    assert result["summary"]["symbol_count"] == 3
    assert result["summary"]["status_counts"] == {
        "matched": 1,
        "multiple_matches": 1,
        "unmatched": 1,
    }
    assert (config.output_root / "openfigi_enriched_symbols.csv").exists()
    assert (config.output_root / "openfigi_enrichment_report.md").exists()
    assert len(client.calls) == 2

    second_client = FakeOpenFigiClient()
    enrich_symbol_stability(config, second_client)
    assert second_client.calls == []


def _config(input_path: Path, output_root: Path) -> OpenFigiConfig:
    return OpenFigiConfig(
        input_path=input_path,
        output_root=output_root,
        api_key=None,
        batch_size=2,
        sleep_seconds=0,
    )


def _mapping(symbol: str, suffix: str = "") -> dict[str, str]:
    return {
        "figi": f"BBG-{symbol}{suffix}",
        "compositeFIGI": f"BBG-COMP-{symbol}{suffix}",
        "shareClassFIGI": f"BBG-SHARE-{symbol}{suffix}",
        "ticker": symbol,
        "name": f"{symbol}{suffix} Corp",
        "exchCode": "US",
        "securityType": "Common Stock",
        "securityType2": "Common Stock",
        "marketSector": "Equity",
    }


def _write_audit_rows(path: Path, symbols: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol",
                "classification",
                "first_day",
                "last_day",
                "coverage_ratio",
                "major_gap_count",
                "main_rows",
                "trade_rows",
            ],
        )
        writer.writeheader()
        for symbol in symbols:
            writer.writerow(
                {
                    "symbol": symbol,
                    "classification": "stable_candidate",
                    "first_day": "20250102",
                    "last_day": "20250103",
                    "coverage_ratio": "1.0",
                    "major_gap_count": "0",
                    "main_rows": "100",
                    "trade_rows": "10",
                }
            )
