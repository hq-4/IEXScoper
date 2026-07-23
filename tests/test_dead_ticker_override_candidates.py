from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_dead_ticker_override_candidates import (
    CANDIDATE_STATUS,
    CandidateConfig,
    build_override_candidates,
)


def test_build_override_candidates_prefills_sec_leads_but_not_verified(tmp_path: Path) -> None:
    template_path = tmp_path / "template.csv"
    triage_path = tmp_path / "triage.parquet"
    output_path = tmp_path / "candidates.csv"
    _template().write_csv(template_path)
    _triage().write_parquet(triage_path)

    result = build_override_candidates(
        CandidateConfig(
            template_path=template_path,
            triage_path=triage_path,
            output_path=output_path,
            min_bucket="high_confidence_lead",
            max_rows=None,
        )
    )

    rows = pl.read_csv(output_path, infer_schema_length=0).to_dicts()
    assert result["candidate_count"] == 1
    assert rows[0]["symbol"] == "AAA"
    assert rows[0]["research_status"] == CANDIDATE_STATUS
    assert rows[0]["proposed_historical_issuer_name"] == "AAA Corp"
    assert rows[0]["proposed_historical_event_type"] == "merger_or_acquisition_lead"
    assert rows[0]["primary_source_url"] == (
        "https://www.sec.gov/Archives/edgar/data/1234/000123424000001/"
    )
    assert "Verify issuer" in rows[0]["research_note"]


def test_build_override_candidates_can_include_medium_bucket(tmp_path: Path) -> None:
    template_path = tmp_path / "template.csv"
    triage_path = tmp_path / "triage.csv"
    output_path = tmp_path / "candidates.csv"
    _template().write_csv(template_path)
    _triage().write_csv(triage_path)

    build_override_candidates(
        CandidateConfig(
            template_path=template_path,
            triage_path=triage_path,
            output_path=output_path,
            min_bucket="medium_confidence_lead",
            max_rows=2,
        )
    )

    symbols = pl.read_csv(output_path, infer_schema_length=0)["symbol"].to_list()
    assert symbols == ["AAA", "BBB"]


def test_build_override_candidates_accepts_lane_batch_without_research_columns(
    tmp_path: Path,
) -> None:
    template_path = tmp_path / "lane.csv"
    triage_path = tmp_path / "triage.parquet"
    output_path = tmp_path / "candidates.csv"
    _template().drop(
        [
            "research_status",
            "proposed_historical_identity_status",
            "proposed_historical_issuer_name",
            "proposed_historical_event_type",
            "proposed_historical_event_date",
            "proposed_historical_successor",
            "primary_source_url",
            "secondary_source_url",
            "research_note",
        ]
    ).write_csv(template_path)
    _triage().write_parquet(triage_path)

    result = build_override_candidates(
        CandidateConfig(
            template_path=template_path,
            triage_path=triage_path,
            output_path=output_path,
            min_bucket="high_confidence_lead",
            max_rows=None,
        )
    )

    assert result["candidate_count"] == 1
    assert pl.read_csv(output_path, infer_schema_length=0)["research_status"].to_list() == [
        CANDIDATE_STATUS
    ]


def _template() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "priority_rank": ["1", "2"],
            "symbol": ["AAA", "BBB"],
            "symbol_era_id": ["AAA#001", "BBB#001"],
            "source_classification": ["delisted_or_acquired_candidate", "delisted_or_acquired_candidate"],
            "instrument_hint": ["probable_operating_or_other", "probable_operating_or_other"],
            "first_day": ["20200101", "20200101"],
            "last_day": ["20241231", "20241231"],
            "research_status": ["todo", "todo"],
            "proposed_historical_identity_status": [None, None],
            "proposed_historical_issuer_name": [None, None],
            "proposed_historical_event_type": [None, None],
            "proposed_historical_event_date": [None, None],
            "proposed_historical_successor": [None, None],
            "primary_source_url": [None, None],
            "secondary_source_url": [None, None],
            "research_note": [None, None],
        }
    )


def _triage() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["AAA", "BBB"],
            "symbol_era_id": ["AAA#001", "BBB#001"],
            "triage_rank": [1, 1],
            "triage_bucket": ["high_confidence_lead", "medium_confidence_lead"],
            "triage_score": [120, 90],
            "triage_reason": [
                "deal_form; inside_symbol_era; symbol_token_in_entity",
                "review_form; inside_symbol_era; symbol_token_in_entity",
            ],
            "cik": ["0000001234", "0000005678"],
            "entity": ["AAA Corp  (AAA)  (CIK 0000001234)", "BBB Corp  (BBB)  (CIK 0000005678)"],
            "form": ["425", "8-K"],
            "filed_at": ["2024-01-01", "2024-02-01"],
            "accession_no": ["0001234-24-000001", "0005678-24-000001"],
        }
    )
