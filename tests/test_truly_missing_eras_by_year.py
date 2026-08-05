from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_truly_missing_eras_by_year import TrulyMissingConfig, build_truly_missing_by_year


def test_build_truly_missing_by_year(tmp_path: Path) -> None:
    review_path = tmp_path / "review.parquet"
    output_root = tmp_path / "out"
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC", "DDD"],
            "symbol_era_id": ["AAA#001", "BBB#001", "CCC#001", "DDD#001"],
            "source_classification": ["delisted_or_acquired_candidate"] * 4,
            "instrument_type": [
                "probable_operating_company",
                "probable_fund_or_trust",
                "probable_operating_company",
                "probable_operating_company",
            ],
            "research_route": [
                "operating_company_sec_event",
                "fund_or_trust_closure",
                "operating_company_sec_event",
                "operating_company_sec_event",
            ],
            "recommended_evidence": ["8-K/merger evidence"] * 4,
            "trade_rows": [100, 200, 300, 400],
            "first_day": ["20170101", "20170601", "20180101", "20190101"],
            "last_day": ["20170601", "20171201", "20180601", "20190601"],
            "canonical_identity_usable_default": [None, False, True, None],
        }
    ).write_parquet(review_path)

    result = build_truly_missing_by_year(
        TrulyMissingConfig(review_queue_path=review_path, output_root=output_root)
    )

    # CCC is usable (True) and must be excluded; AAA/BBB/DDD (null/False/null) remain.
    assert result["summary"]["total_truly_missing_eras"] == 3
    assert result["summary"]["total_trade_rows"] == 700
    years = {row["first_year"]: row for row in result["by_year"]}
    assert years["2017"]["eras"] == 2
    assert years["2017"]["trade_rows"] == 300
    assert years["2019"]["eras"] == 1
    assert "2018" not in years

    clusters = {(row["first_year"], row["research_route"]): row for row in result["clusters"]}
    assert clusters[("2017", "operating_company_sec_event")]["eras"] == 1
    assert clusters[("2017", "fund_or_trust_closure")]["eras"] == 1
    assert clusters[("2019", "operating_company_sec_event")]["trade_rows"] == 400

    route_totals = {
        row["research_route"]: row for row in result["summary"]["research_route_totals"]
    }
    assert route_totals["operating_company_sec_event"]["eras"] == 2

    detail = pl.read_csv(output_root / "truly_missing_eras_by_year.csv")
    assert set(detail["symbol_era_id"]) == {"AAA#001", "BBB#001", "DDD#001"}
    assert "research_route" in detail.columns
    cluster_csv = pl.read_csv(output_root / "truly_missing_eras_clusters.csv")
    assert cluster_csv.height == 3
    assert (output_root / "truly_missing_eras_by_year_report.md").exists()
    assert (output_root / "truly_missing_eras_by_year_summary.json").exists()
