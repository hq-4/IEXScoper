from __future__ import annotations

from utils.derivative_identity_resolution import derivative_resolution, parent_root_symbol


def test_parent_syntax_alone_never_creates_terminal_disposition() -> None:
    row = {"symbol": "ABCW", "instrument_type": "probable_warrant", "last_day": "20240105"}
    result = derivative_resolution(row, {"identity_cik": "1"}, "", filing_cik="1")
    assert parent_root_symbol("ABCW", "probable_warrant") == "ABC"
    assert result["derivative_bucket"] == "derivative_evidence_hold"
    assert result["importable"] is False


def test_warrant_requires_explicit_child_action_and_near_date() -> None:
    row = {"symbol": "ABCW", "instrument_type": "probable_warrant", "last_day": "20240105"}
    snippet = "On January 5, 2024, the ABCW warrants expired and ceased trading."
    result = derivative_resolution(row, {"identity_cik": "1"}, snippet, filing_cik="1")
    assert result["derivative_bucket"] == "derivative_verified_ready"
    assert result["importable"] is True


def test_share_class_requires_same_cik_and_parent_action_date() -> None:
    row = {"symbol": "ABC.A", "instrument_type": "probable_share_class", "last_day": "20240105"}
    snippet = "On January 5, 2024, ABC.A common shares ceased trading after the merger."
    held = derivative_resolution(row, {"identity_cik": "1"}, snippet, filing_cik="2")
    ready = derivative_resolution(
        row, {"identity_cik": "1"}, snippet, filing_cik="1", parent_event_date="2024-01-05"
    )
    assert held["derivative_bucket"] == "derivative_evidence_hold"
    assert ready["derivative_bucket"] == "derivative_verified_ready"
