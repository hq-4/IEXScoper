from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utils.build_sector_manual_research_worklist import (
    SectorWorklistConfig,
    build_sector_worklist,
)


def _write_enriched(path: Path) -> None:
    pl.DataFrame(
        {
            "symbol": ["ZZZ", "YYY", "XXX", "DONE", "FUND"],
            "symbol_era_id": ["ZZZ#001", "YYY#001", "XXX#001", "DONE#001", "FUND#001"],
            "source_classification": [
                "delisted_or_acquired_candidate",
                "intermittent_or_reused_candidate",
                "delisted_or_acquired_candidate",
                "stable_candidate",
                "stable_candidate",
            ],
            "trade_rows": [100, 300, 200, 9999, 50000],
            "first_day": ["20170103", "20180103", "20190103", "20200103", "20161212"],
            "last_day": ["20171229", "20181231", "20191231", "20260101", "20260622"],
            "identity_tier": [None, "openfigi_asserted", None, "verified", None],
            "identity_issuer": [None, "Some Reused Co", None, "Done Corp", None],
            "identity_instrument": [
                None,
                "equity_common",
                None,
                "probable_operating_company",
                None,
            ],
            "instrument_class": [
                None,
                "equity_common",
                None,
                "probable_operating_company",
                "fund_etf",
            ],
            "cik_source": [
                "no_cik_available",
                "no_cik_available",
                "no_cik_available",
                "sec_current_ticker_match",
                "no_cik_available",
            ],
            "resolved_cik": [None, None, None, "123456", None],
            "sic_coverage_status": [
                "no_cik",
                "no_cik",
                "no_cik",
                "cik_no_sic",
                "fund_no_sic_needed",
            ],
            "identity_disproven": [False, True, False, False, False],
            "blank_sic_lead_cik": [None, None, "999", None, None],
            "blank_sic_lead_name": [None, None, "Some Blank-SIC Co", None, None],
            "blank_sic_lead_high_confidence": [False, False, True, False, False],
        }
    ).write_parquet(path)


def test_build_sector_worklist_ranks_by_trade_rows_and_excludes_resolved(tmp_path: Path) -> None:
    enriched_path = tmp_path / "eras_sector_enriched.parquet"
    output_root = tmp_path / "out"
    _write_enriched(enriched_path)

    result = build_sector_worklist(
        SectorWorklistConfig(
            eras_sector_enriched_path=enriched_path, output_root=output_root, top_n=2
        )
    )

    rows = pl.read_parquet(output_root / "sector_research_worklist.parquet").to_dicts()
    symbols = [row["symbol"] for row in rows]
    assert symbols == ["YYY", "XXX", "ZZZ"]
    assert "DONE" not in symbols  # already has a resolved CIK
    assert "FUND" not in symbols  # fund/ETF, excluded regardless of CIK status
    assert rows[0]["priority_rank"] == 1
    assert rows[0]["has_googleable_name"] is True  # YYY has an OpenFIGI-asserted issuer
    assert rows[1]["has_googleable_name"] is False  # XXX has nothing to google by
    assert rows[0]["identity_disproven"] is True  # YYY's issuer name is proven wrong
    assert rows[1]["identity_disproven"] is False
    assert rows[1]["blank_sic_lead_cik"] == "999"  # XXX has a blank-SIC research lead
    assert rows[1]["blank_sic_lead_high_confidence"] is True
    assert rows[0]["blank_sic_lead_cik"] is None
    for column in ("manual_cik", "manual_sic", "manual_notes"):
        assert rows[0][column] is None

    assert result["summary"]["worklist_era_count"] == 3
    assert result["summary"]["excluded_fund_count"] == 1
    assert result["summary"]["has_googleable_name_count"] == 1
    assert result["summary"]["identity_disproven_count"] == 1
    assert result["summary"]["blank_sic_lead_count"] == 1
    assert result["summary"]["blank_sic_lead_high_confidence_count"] == 1
    assert result["summary"]["top_n"] == 2
    assert (output_root / "sector_research_worklist_report.md").exists()
    assert (output_root / "sector_research_worklist_top.csv").exists()


def test_build_sector_worklist_raises_on_missing_input(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        build_sector_worklist(
            SectorWorklistConfig(
                eras_sector_enriched_path=tmp_path / "missing.parquet",
                output_root=tmp_path / "out",
            )
        )
