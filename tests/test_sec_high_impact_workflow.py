from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

from utils.import_dead_ticker_manual_overrides import OVERRIDE_COLUMNS
from utils.sec_identity_sources import SecTransportError
from utils.sec_high_impact_workflow import HighImpactConfig, run_high_impact_workflow


def test_resume_performs_zero_network_work_for_completed_row(tmp_path: Path) -> None:
    input_path = tmp_path / "input.csv"
    overrides_path = tmp_path / "overrides.csv"
    raw_path = tmp_path / "edgar_full_text_raw.jsonl"
    write_csv(
        input_path,
        [
            {
                "symbol": "ABC",
                "symbol_era_id": "ABC#001",
                "first_day": "20200101",
                "last_day": "20201231",
                "trade_rows": "10",
            }
        ],
    )
    write_csv(overrides_path, [], fields=OVERRIDE_COLUMNS)
    raw_path.write_text(
        '{"payload":{"hits":{"hits":[{"_id":"0000000001-20-000001:a.htm",'
        '"_source":{"display_names":["ABC Corp (ABC) (CIK 1)"],'
        '"file_date":"2020-02-01","adsh":"0000000001-20-000001"}}]}}}\n',
        encoding="utf-8",
    )
    config = HighImpactConfig(
        input_path=input_path,
        output_root=tmp_path / "out",
        user_agent="test test@example.test",
        raw_evidence_paths=(raw_path,),
        overrides_path=overrides_path,
        sleep_seconds=0,
    )
    evidence_client = NoSearchClient()
    submissions_client = EmptySubmissionsClient()

    first = run_high_impact_workflow(config, evidence_client, submissions_client)
    second = run_high_impact_workflow(config, evidence_client, submissions_client)

    assert first["automation_exhausted_count"] == 1
    assert first["resolution_buckets"]["identity_only_hold"]["count"] == 1
    assert second["automation_exhausted_count"] == 1
    assert evidence_client.search_count == 0
    assert submissions_client.filing_calls == 1


def test_workflow_never_persists_full_filing_text(tmp_path: Path) -> None:
    input_path = tmp_path / "input.csv"
    overrides_path = tmp_path / "overrides.csv"
    raw_path = tmp_path / "edgar_full_text_raw.jsonl"
    write_csv(
        input_path,
        [
            {
                "symbol": "ABC",
                "symbol_era_id": "ABC#001",
                "first_day": "20200101",
                "last_day": "20201231",
                "trade_rows": "10",
            }
        ],
    )
    write_csv(overrides_path, [], fields=OVERRIDE_COLUMNS)
    raw_path.write_text(
        '{"payload":{"hits":{"hits":[{"_id":"0000000001-20-000001:a.htm",'
        '"_source":{"display_names":["ABC Corp (ABC) (CIK 1)"],'
        '"file_date":"2020-02-01","adsh":"0000000001-20-000001"}}]}}}\n',
        encoding="utf-8",
    )
    output_root = tmp_path / "out"
    config = HighImpactConfig(
        input_path=input_path,
        output_root=output_root,
        user_agent="test test@example.test",
        raw_evidence_paths=(raw_path,),
        overrides_path=overrides_path,
        sleep_seconds=0,
    )

    run_high_impact_workflow(config, DocumentClient(), EventSubmissionsClient())

    persisted = "".join(
        path.read_text(encoding="utf-8")
        for path in output_root.iterdir()
        if path.suffix in {".csv", ".json", ".txt"}
    )
    assert "FULL_DOCUMENT_SENTINEL" not in persisted
    assert "completed the merger" in persisted.lower()


def test_retryable_row_exhausts_configured_attempt_budget(tmp_path: Path) -> None:
    input_path = tmp_path / "input.csv"
    overrides_path = tmp_path / "overrides.csv"
    write_csv(
        input_path,
        [
            {
                "symbol": "MISS",
                "symbol_era_id": "MISS#001",
                "first_day": "20200101",
                "last_day": "20201231",
                "trade_rows": "1",
            }
        ],
    )
    write_csv(overrides_path, [], fields=OVERRIDE_COLUMNS)
    client = FailingSearchClient()
    config = HighImpactConfig(
        input_path=input_path,
        output_root=tmp_path / "out",
        user_agent="test test@example.test",
        raw_evidence_paths=(tmp_path / "missing.jsonl",),
        overrides_path=overrides_path,
        max_row_attempts=2,
        sleep_seconds=0,
    )

    summary = run_high_impact_workflow(config, client, EmptySubmissionsClient())

    assert summary["automation_exhausted_count"] == 1
    assert summary["resolution_buckets"]["fetch_error_hold"]["count"] == 1
    assert client.search_count == 2


def test_workflow_emits_structured_progress_logs(tmp_path: Path, caplog: Any) -> None:
    input_path = tmp_path / "input.csv"
    overrides_path = tmp_path / "overrides.csv"
    raw_path = tmp_path / "edgar_full_text_raw.jsonl"
    write_csv(
        input_path,
        [
            {
                "symbol": "ABC",
                "symbol_era_id": "ABC#001",
                "first_day": "20200101",
                "last_day": "20201231",
                "trade_rows": "10",
            }
        ],
    )
    write_csv(overrides_path, [], fields=OVERRIDE_COLUMNS)
    raw_path.write_text(
        '{"payload":{"hits":{"hits":[{"_id":"0000000001-20-000001:a.htm",'
        '"_source":{"display_names":["ABC Corp (ABC) (CIK 1)"],'
        '"file_date":"2020-02-01","adsh":"0000000001-20-000001"}}]}}}\n',
        encoding="utf-8",
    )
    config = HighImpactConfig(
        input_path=input_path,
        output_root=tmp_path / "out",
        user_agent="test test@example.test",
        raw_evidence_paths=(raw_path,),
        overrides_path=overrides_path,
        sleep_seconds=0,
    )

    caplog.set_level(logging.INFO, logger="utils.sec_high_impact_workflow")
    run_high_impact_workflow(config, NoSearchClient(), EmptySubmissionsClient())

    events = {getattr(record, "event", "") for record in caplog.records}
    assert "sec_identity_workflow_loaded" in events
    assert "sec_identity_batch_start" in events
    assert "sec_identity_row_resolved" in events
    assert "sec_identity_batch_complete" in events
    assert "sec_identity_outputs_written" in events
    assert "sec_identity_import_step_complete" in events


class NoSearchClient:
    request_count = 0
    retry_count = 0
    stats = type("Stats", (), {"request_count": 0, "retry_count": 0, "fetch_error_count": 0})()
    search_count = 0

    def search(self, params: dict[str, str]) -> dict[str, Any]:
        self.search_count += 1
        raise AssertionError(f"unexpected network identity query: {params}")

    def document_text(self, url: str) -> str:
        raise AssertionError(f"unexpected document fetch: {url}")


class FailingSearchClient(NoSearchClient):
    def search(self, params: dict[str, str]) -> dict[str, Any]:
        self.search_count += 1
        raise SecTransportError("simulated SEC outage")


class EmptySubmissionsClient:
    request_count = 0
    retry_count = 0
    filing_calls = 0

    def filings(self, cik: str, *, lower: str, upper: str) -> list[dict[str, str]]:
        self.filing_calls += 1
        return []

    def submissions(self, cik: str) -> dict[str, Any]:
        return {"cik": cik, "tickers": []}


class DocumentClient(NoSearchClient):
    def document_text(self, url: str) -> str:
        return (
            "FULL_DOCUMENT_SENTINEL "
            + "prefix " * 200
            + "On December 31, 2020, ABC Corp completed the merger and shares ceased trading."
            + " suffix" * 200
        )


class EventSubmissionsClient(EmptySubmissionsClient):
    def filings(self, cik: str, *, lower: str, upper: str) -> list[dict[str, str]]:
        self.filing_calls += 1
        return [
            {
                "form": "8-K",
                "filing_date": "2021-01-02",
                "accession_no": "0000000001-21-000001",
                "document_url": "https://example.test/event.htm",
            }
        ]


def write_csv(path: Path, rows: list[dict[str, str]], fields: list[str] | None = None) -> None:
    columns = fields or list(rows[0])
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
