from __future__ import annotations

from datetime import date

from utils.resolution_v2_sec import _symbol_change, _terminal_gate, _terminal_type

ROW = {"symbol": "OLD"}
IDENTITY = {"entity_id": "1", "issuer": "Old Corp"}
FILING = {"filer_cik": "1"}


def test_terminal_gate_accepts_explicit_completed_merger() -> None:
    text = "Old Corp completed the merger on January 5, 2025."
    assert _terminal_gate(
        ROW,
        IDENTITY,
        FILING,
        text,
        text,
        date(2025, 1, 5),
        date(2025, 1, 6),
    )


def test_terminal_gate_rejects_generic_effective_and_prospective_delisting() -> None:
    effective = "Old Corp certificate of designations became effective on January 5, 2025."
    prospective = "Once OLD is delisted on January 5, 2025, trading may be limited."
    for text in (effective, prospective):
        assert not _terminal_gate(
            ROW,
            IDENTITY,
            FILING,
            text,
            text,
            date(2025, 1, 5),
            date(2025, 1, 6),
        )


def test_terminal_type_recognizes_acquisition_completion() -> None:
    assert _terminal_type("Old Corp completed the acquisition") == (
        "merger_or_acquisition_terminal"
    )


def test_unconfirmed_symbol_transition_cannot_fall_through_to_terminal() -> None:
    text = (
        "On August 25, 2025, the stock ceased trading under ticker symbol OLD and began "
        "trading under ticker symbol NEW."
    )
    identity = {
        "entity_id": "1",
        "related_symbols": ["OLD"],
        "evidence_date": "2025-08-26",
    }
    filing = {"filer_cik": "1", "form": "8-K", "accession_no": "a", "document_url": "u"}
    fact, detected = _symbol_change(ROW, identity, filing, text, date(2025, 8, 25))
    assert fact is None
    assert detected is True
