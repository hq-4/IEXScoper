from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_dead_ticker_resolution_template import (
    ResolutionTemplateConfig,
    build_resolution_template,
)


def test_build_resolution_template_adds_blank_research_columns(tmp_path: Path) -> None:
    priority_path = tmp_path / "priority.parquet"
    output_path = tmp_path / "template.csv"
    _write_priority(priority_path)

    result = build_resolution_template(
        ResolutionTemplateConfig(
            priority_queue_path=priority_path, output_path=output_path, limit=1
        )
    )

    rows = pl.read_csv(output_path, infer_schema_length=0).to_dicts()
    assert result["row_count"] == 1
    assert rows[0]["symbol"] == "AAA"
    assert rows[0]["research_route"] == "operating_company_sec_event"
    assert "8-K" in rows[0]["recommended_evidence"]
    assert rows[0]["research_status"] == "todo"
    assert rows[0]["proposed_historical_issuer_name"] is None
    assert rows[0]["primary_source_url"] is None


def _write_priority(path: Path) -> None:
    pl.DataFrame(
        {
            "priority_rank": [1, 2],
            "symbol": ["AAA", "BBB"],
            "symbol_era_id": ["AAA#001", "BBB#001"],
            "source_classification": [
                "delisted_or_acquired_candidate",
                "intermittent_or_reused_candidate",
            ],
            "instrument_hint": ["probable_operating_or_other", "probable_operating_or_other"],
            "instrument_type": ["probable_operating_company", "probable_operating_company"],
            "instrument_reason": ["common_stock_symbol", "common_stock_symbol"],
            "research_route": ["operating_company_sec_event", "operating_company_sec_event"],
            "recommended_evidence": [
                "8-K, merger proxy/S-4, 25-NSE, 15-12B, issuer/acquirer release",
                "8-K, merger proxy/S-4, 25-NSE, 15-12B, issuer/acquirer release",
            ],
            "routing_reason": [
                "instrument_type=probable_operating_company",
                "instrument_type=probable_operating_company",
            ],
            "is_probable_operating": [True, True],
            "is_delisted_candidate": [True, False],
            "first_day": ["20170103", "20180103"],
            "last_day": ["20191231", "20201231"],
            "observed_days": [10, 20],
            "trade_rows": [1000, 2000],
            "main_rows": [1100, 2100],
        }
    ).write_parquet(path)
