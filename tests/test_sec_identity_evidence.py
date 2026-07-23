from __future__ import annotations

from datetime import date

from utils.sec_identity_evidence import (
    build_identity_queries,
    identity_result,
    parse_display_name,
)
from utils.sec_identity_sources import evidence_from_raw_paths


def test_parenthetical_tickers_parse_comma_separated_values() -> None:
    evidence = parse_display_name(
        "Example Holdings, Inc. (EX, EX.W, X) (CIK 0000123456)",
        filing_date="2024-01-05",
        accession_no="0000123456-24-000001",
    )

    assert evidence.issuer_name == "Example Holdings, Inc."
    assert evidence.cik == "123456"
    assert evidence.tickers == ("EX", "EX.W", "X")


def test_date_scoped_identity_accepts_aliases_on_one_cik() -> None:
    row = {"symbol": "OLD", "first_day": "20200101", "last_day": "20201231"}
    evidence = [
        parse_display_name("Old Name (OLD) (CIK 1)", filing_date="2020-02-01"),
        parse_display_name("New Name (OLD, NEW) (CIK 1)", filing_date="2020-11-01"),
        parse_display_name("Reuser (OLD) (CIK 2)", filing_date="2023-01-01"),
    ]

    result = identity_result(row, evidence)

    assert result["identity_bucket"] == "identity_verified_ready"
    assert result["identity_cik"] == "1"
    assert result["identity_candidate_cik_count"] == 1
    assert result["identity_issuer_aliases"] == "New Name|Old Name"


def test_multiple_date_scoped_ciks_are_always_held() -> None:
    row = {"symbol": "OLD", "first_day": "20200101", "last_day": "20201231"}
    evidence = [
        parse_display_name("First (OLD) (CIK 1)", filing_date="2020-02-01"),
        parse_display_name("Second (OLD) (CIK 2)", filing_date="2020-03-01"),
    ]

    result = identity_result(row, evidence)

    assert result["identity_bucket"] == "identity_collision_hold"
    assert result["identity_candidate_cik_count"] == 2


def test_one_character_ticker_requires_exact_filer_ticker_evidence() -> None:
    row = {"symbol": "C", "first_day": "20200101", "last_day": "20201231"}
    direct = parse_display_name("Citigroup Inc. (C, C-PN) (CIK 831001)", filing_date="2020-04-01")
    mention_only = parse_display_name("Acquirer Inc. (BUY) (CIK 2)", filing_date="2020-04-02")

    assert identity_result(row, [direct])["identity_bucket"] == "identity_verified_ready"
    assert identity_result(row, [mention_only])["identity_bucket"] == "identity_no_evidence"


def test_identity_queries_never_use_entity_name() -> None:
    variants = build_identity_queries("ABC", date(2020, 1, 1), date(2020, 12, 31))

    assert len(variants) == 3
    assert all("entityName" not in params for params in variants)
    assert all("ABC" in params["q"] for params in variants)


def test_existing_raw_evidence_is_loaded_without_network(tmp_path) -> None:
    raw = tmp_path / "edgar_full_text_raw.jsonl"
    raw.write_text(
        '{"symbol":"ABC","payload":{"hits":{"hits":[{"_id":'
        '"0000000001-20-000001:event.htm","_source":{"display_names":'
        '["ABC Corp (ABC) (CIK 0000000001)"],"file_date":"2020-02-01",'
        '"adsh":"0000000001-20-000001"}}]}}}\n',
        encoding="utf-8",
    )

    evidence = evidence_from_raw_paths([raw])

    assert len(evidence) == 1
    assert evidence[0].tickers == ("ABC",)
    assert evidence[0].source == "local_cache"
