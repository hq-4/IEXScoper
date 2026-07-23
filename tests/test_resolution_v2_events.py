from __future__ import annotations

from datetime import date, timedelta

from utils.resolution_v2_events import (
    date_candidates,
    form15_event,
    form25_event,
    normalize_filing_text,
    select_stratified_filings,
    semantic_action_date,
    symbol_change_proof,
)


def test_entity_decoding_and_semantic_dates_remove_160_artifacts() -> None:
    encoded = "<p>On September&nbsp;18th, 2024, Acme completed the merger.</p>"
    legacy = "On SEPTEMBER 160 18 2024, Acme completed the merger."
    assert "160" not in normalize_filing_text(encoded)
    assert date_candidates(encoded)[0]["date"] == date(2024, 9, 18)
    assert semantic_action_date(legacy, "20240918")[0] == date(2024, 9, 18)


def test_all_supported_date_shapes_and_nearest_action_clause() -> None:
    text = (
        "The agreement was signed 01/02/2020. "
        "The transaction closed on Sept. 19th, 2024. "
        "The report was filed 2024-09-20."
    )
    values = {item["date"] for item in date_candidates(text)}
    assert {date(2020, 1, 2), date(2024, 9, 19), date(2024, 9, 20)} <= values
    chosen, snippet = semantic_action_date(text, "20240918")
    assert chosen == date(2024, 9, 19)
    assert "transaction closed" in snippet


def test_semantic_action_date_bounds_sentence_scans_for_large_filings() -> None:
    filler = "Risk factor text without action dates. " * 5_000
    text = (
        f"January 1, 1999. {filler}"
        "The transaction closed on September 19, 2024. "
        f"{filler} The report was filed on October 1, 2024."
    )
    chosen, snippet = semantic_action_date(text, "20240918")
    assert chosen == date(2024, 9, 19)
    assert "transaction closed" in snippet


def test_form25_preserves_subject_filer_and_derives_effective_date() -> None:
    filing = {"filing_date": "2024-01-05", "filer_cik": "999", "subject_cik": "123"}
    result = form25_event(
        filing, "Exact common stock of Acme.", anchored_subject_cik="123", exact_security=True
    )
    assert result["verification_state"] == "verified"
    assert result["event_date"] == "2024-01-15"
    assert result["date_basis"] == "regulatory_rule"
    assert result["filer_cik"] == "999" and result["subject_cik"] == "123"
    assert "exchange_filer_distinct" in result["flags"]


def test_form25_postponement_or_wrong_subject_blocks_verification() -> None:
    filing = {"filing_date": "2024-01-05", "filer_cik": "999", "subject_cik": "123"}
    postponed = form25_event(
        filing, "The delisting was postponed.", anchored_subject_cik="123", exact_security=True
    )
    collision = form25_event(
        filing, "Delisting notice.", anchored_subject_cik="456", exact_security=True
    )
    assert postponed["verification_state"] == "event_candidate"
    assert "withdrawal_or_postponement" in postponed["flags"]
    assert collision["verification_state"] == "event_candidate"


def test_form15_requires_explicit_effective_text_and_exact_subject() -> None:
    filing = {"filing_date": "2024-01-05", "filer_cik": "123", "subject_cik": "123"}
    explicit = "The termination of registration became effective on January 15, 2024."
    verified = form15_event(filing, explicit, anchored_subject_cik="123", exact_security=True)
    held = form15_event(
        filing, "This Form 15 was filed.", anchored_subject_cik="123", exact_security=True
    )
    assert verified["verification_state"] == "verified"
    assert verified["date_basis"] == "explicit_form_text"
    assert held["verification_state"] == "event_candidate"


def test_stratified_filing_selection_uses_quotas_exhibits_and_deduplication() -> None:
    boundary = date(2024, 1, 10)
    filings = [_filing(f"8-{i}", "8-K", boundary + timedelta(days=i)) for i in range(7)]
    filings += [_filing(f"25-{i}", "25-NSE", boundary + timedelta(days=i)) for i in range(6)]
    filings += [_filing(f"deal-{i}", "425", boundary + timedelta(days=i)) for i in range(4)]
    filings += [
        _filing("8-0", "8-K", boundary),
        _filing("8-0", "EX-99.1", boundary, role="EX-99.1"),
    ]
    selected = select_stratified_filings(filings, boundary)
    assert sum(row["form"] == "8-K" for row in selected) == 4
    assert sum(row["form"] == "25-NSE" for row in selected) == 4
    assert sum(row["form"] == "425" for row in selected) == 2
    assert any(row["role"] == "EX-99.1" for row in selected)
    assert len({(row["accession_no"], row["document_url"]) for row in selected}) == len(selected)


def test_prospective_symbol_change_needs_post_effective_same_cik_confirmation() -> None:
    text = "Effective January 5, 2024, the ticker symbol will change from OLD to NEW."
    announcement = {"subject_cik": "1", "boundary": "20240105"}
    confirmation = {"cik": "1", "tickers": ["NEW"], "filing_date": "2024-01-06"}
    held = symbol_change_proof("OLD", text, announcement, [])
    verified = symbol_change_proof("OLD", text, announcement, [confirmation])
    collision = symbol_change_proof("OTHER", text, announcement, [confirmation])
    assert held["verification_state"] == "event_candidate"
    assert verified["verification_state"] == "verified"
    assert verified["new_symbol"] == "NEW"
    assert collision["verification_state"] == "event_candidate"


def _filing(accession: str, form: str, filed: date, role: str = "primary") -> dict[str, str]:
    return {
        "accession_no": accession,
        "form": form,
        "filing_date": filed.isoformat(),
        "role": role,
        "document_url": f"https://example.test/{accession}/{role}",
    }
