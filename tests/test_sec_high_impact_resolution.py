from __future__ import annotations

from utils.sec_high_impact_resolution import resolve_event_text, symbol_change_result
from utils.sec_high_impact_events import active_current_evidence
from utils.sec_high_impact_outputs import import_candidates

ROW = {
    "symbol": "OLD",
    "original_last_day": "20240105",
    "identity_cik": "1",
    "identity_issuer_name": "Old Corp",
}


def test_near_date_same_cik_terminal_completion_passes() -> None:
    text = "On January 5, 2024, Old Corp completed the merger and its shares ceased trading."
    result = resolve_event_text(ROW, text, "https://example.test/event", filing_cik="1")
    assert result["resolution_bucket"] == "terminal_verified_ready"
    assert "cik_identity_match" in result["event_flags"]


def test_prospective_and_far_date_terminal_language_hold() -> None:
    prospective = "On January 5, 2024, Old Corp expects to close the merger."
    far = "On January 5, 2023, Old Corp completed the merger and shares ceased trading."
    assert (
        resolve_event_text(ROW, prospective, "url", filing_cik="1")["resolution_bucket"]
        == "identity_only_hold"
    )
    assert (
        resolve_event_text(ROW, far, "url", filing_cik="1")["resolution_bucket"]
        == "identity_only_hold"
    )


def test_exact_same_cik_symbol_change_passes_and_takes_precedence() -> None:
    text = (
        "Effective January 5, 2024, Old Corp changed its ticker symbol from OLD to NEW "
        "and shares under OLD ceased trading."
    )
    result = resolve_event_text(ROW, text, "https://example.test/change", filing_cik="1")
    assert result["resolution_bucket"] == "symbol_change_verified_ready"
    assert result["event_successor"] == "NEW"
    assert result["event_type"] == "symbol_change"


def test_symbol_change_rejects_different_cik_or_missing_successor() -> None:
    text = "Effective January 5, 2024, Old Corp changed its ticker symbol from OLD to NEW."
    assert symbol_change_result(ROW, text, "url", filing_cik="2")["symbol_change_bucket"] != (
        "symbol_change_verified_ready"
    )
    missing = "On January 5, 2024 Old Corp changed its ticker symbol."
    assert (
        symbol_change_result(ROW, missing, "url", filing_cik="1")["symbol_change_bucket"]
        != "symbol_change_verified_ready"
    )


def test_identity_only_evidence_is_not_importable() -> None:
    result = resolve_event_text(ROW, "Old Corp filed a quarterly report.", "url", filing_cik="1")
    assert result["resolution_bucket"] == "identity_only_hold"
    assert result["importable"] is False


def test_active_data_gap_requires_submissions_and_directory_match() -> None:
    row = {"symbol": "OLD", "identity_cik": "1", "last_day": "20240105"}
    submissions = {"cik": "0000000001", "tickers": ["OLD"]}
    client = type(
        "Client",
        (),
        {"filings": lambda self, cik, lower, upper: [{"filing_date": "2024-02-01"}]},
    )()

    assert active_current_evidence(row, submissions, {("OLD", "1")}, client) is True
    assert active_current_evidence(row, submissions, set(), client) is False


def test_existing_override_ids_are_skipped_idempotently() -> None:
    row = {
        "symbol": "OLD",
        "symbol_era_id": "OLD#001",
        "identity_issuer_name": "Old Corp",
        "event_type": "symbol_change",
        "event_date": "2024-01-05",
        "event_successor": "NEW",
        "event_document_url": "https://example.test/event",
    }
    candidates, already = import_candidates([row], {"OLD#001"})
    assert candidates == []
    assert already == ["OLD#001"]
