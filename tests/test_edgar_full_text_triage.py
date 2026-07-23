from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.edgar_full_text_triage import (
    confidence_bucket,
    score_entity,
    triage_edgar_full_text_leads,
)
from utils.edgar_full_text_triage_schema import TriageConfig


def test_triage_ranks_deal_form_entity_match_before_broad_8k(tmp_path: Path) -> None:
    leads_path = tmp_path / "leads.csv"
    _lead_rows(
        [
            {
                "symbol": "SQ",
                "symbol_era_id": "SQ#001",
                "entity": "Block Inc SQ",
                "form": "425",
                "filed_at": "2022-01-31",
                "hit_rank": 3,
            },
            {
                "symbol": "SQ",
                "symbol_era_id": "SQ#001",
                "entity": "Unrelated Holdings",
                "form": "8-K",
                "filed_at": "2022-02-01",
                "hit_rank": 1,
            },
        ]
    ).write_csv(leads_path)

    result = triage_edgar_full_text_leads(_config(tmp_path, leads_path))

    rows = result["rows"]
    assert rows[0]["form"] == "425"
    assert rows[0]["triage_bucket"] == "high_confidence_lead"
    assert rows[0]["triage_rank"] == 1


def test_triage_penalizes_short_symbol_without_entity_match(tmp_path: Path) -> None:
    leads_path = tmp_path / "leads.csv"
    _lead_rows(
        [
            {
                "symbol": "K",
                "symbol_era_id": "K#001",
                "entity": "TonenGeneral Sekiyu K.K.",
                "query": "merger",
                "form": "425",
                "filed_at": "2024-09-10",
                "hit_rank": 1,
            },
            {
                "symbol": "K",
                "symbol_era_id": "K#001",
                "entity": "Kellanova",
                "query": "Kellanova AND merger",
                "form": "DEFM14A",
                "filed_at": "2024-09-10",
                "hit_rank": 2,
            },
        ]
    ).write_csv(leads_path)

    result = triage_edgar_full_text_leads(_config(tmp_path, leads_path))

    rows = result["rows"]
    assert rows[0]["entity"] == "Kellanova"
    assert rows[0]["entity_match"] == "query_alias_in_entity"
    assert rows[1]["entity_match"] == "short_symbol_no_entity_match"
    assert rows[1]["triage_bucket"] == "manual_review_lead"


def test_triage_writes_outputs_and_summary(tmp_path: Path) -> None:
    leads_path = tmp_path / "leads.parquet"
    _lead_rows(
        [
            {
                "symbol": "AAA",
                "symbol_era_id": "AAA#001",
                "entity": "AAA Corp",
                "form": "S-4",
                "filed_at": "2020-10-01",
                "hit_rank": 1,
            }
        ]
    ).write_parquet(leads_path)
    config = _config(tmp_path, leads_path)

    result = triage_edgar_full_text_leads(config)

    assert config.output_csv.exists()
    assert config.output_parquet.exists()
    assert config.summary_path.exists()
    assert result["summary"]["bucket_counts"] == {"high_confidence_lead": 1}


def test_score_entity_uses_alias_query_for_known_issuer_name() -> None:
    assert score_entity("XLNX", "Xilinx Inc", "Xilinx AND merger")[0] == "query_alias_in_entity"
    assert score_entity("AB", "Random Fund", "merger")[0] == "short_symbol_no_entity_match"


def test_confidence_bucket_keeps_weak_forms_in_manual_review() -> None:
    assert (
        confidence_bucket("weak_form", "inside_symbol_era", "symbol_token_in_entity")
        == "manual_review_lead"
    )


def _config(tmp_path: Path, leads_path: Path) -> TriageConfig:
    return TriageConfig(
        leads_path=leads_path,
        output_csv=tmp_path / "triage.csv",
        output_parquet=tmp_path / "triage.parquet",
        summary_path=tmp_path / "triage_summary.json",
        top_n=5,
    )


def _lead_rows(overrides: list[dict[str, object]]) -> pl.DataFrame:
    rows = []
    for override in overrides:
        row = {
            "priority_rank": "1",
            "symbol": "AAA",
            "symbol_era_id": "AAA#001",
            "first_day": "20200101",
            "last_day": "20241231",
            "query": f"{override.get('entity')} AND merger",
            "search_status": "hit",
            "total_hits": 2,
            "hit_rank": 1,
            "cik": "0000000001",
            "entity": "AAA Corp",
            "form": "8-K",
            "filed_at": "2020-01-01",
            "accession_no": "0000000001-20-000001",
            "document_url": "https://www.sec.gov/Archives/example.htm",
        }
        row.update(override)
        rows.append(row)
    return pl.DataFrame(rows)
