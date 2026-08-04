from __future__ import annotations

import json

import polars as pl

from utils.build_era_identity_enriched import build_enriched

ERAS = pl.DataFrame(
    {
        "symbol": ["AAA", "BBB", "CCC", "DDD"],
        "symbol_era_id": ["AAA#001", "BBB#001", "CCC#001", "DDD#001"],
        "first_day": ["20170101", "20180101", "20190101", "20200101"],
        "last_day": ["20170601", "20180701", "20190601", "20200601"],
        "trade_rows": [100, 200, 300, 400],
        "source_classification": ["delisted_or_acquired_candidate"] * 4,
    }
)

IDENTITIES = [
    {
        "symbol_era_id": "AAA#001",
        "verification_state": "openfigi_asserted",
        "issuer": "Wrong Co",
        "entity_id": "F9",
        "evidence_method": "openfigi_symbol_identity",
        "instrument": "equity_common",
        "flags": ["contested"],
    },
    {
        "symbol_era_id": "AAA#001",
        "verification_state": "verified",
        "issuer": "Alpha Corp",
        "entity_id": "123",
        "evidence_method": "sec_date_scoped_display_names",
        "instrument": "probable_operating_company",
        "flags": [],
    },
    {
        "symbol_era_id": "BBB#001",
        "verification_state": "openfigi_asserted",
        "issuer": "Beta ETF",
        "entity_id": "F2",
        "evidence_method": "openfigi_symbol_identity",
        "instrument": "fund_etf",
        "flags": [],
    },
]

EVENTS = [
    {
        "symbol_era_id": "AAA#001",
        "event_type": "merger_or_acquisition_terminal",
        "event_date": "2017-05-30",
        "verification_state": "verified",
    },
    {
        "symbol_era_id": "BBB#001",
        "event_type": "delisting_form25",
        "event_date": "2018-07-01",
        "verification_state": "event_candidate",
    },
]


def test_build_enriched(tmp_path) -> None:
    eras_path = tmp_path / "eras.parquet"
    ERAS.write_parquet(eras_path)
    fact_root = tmp_path / "facts"
    fact_root.mkdir()
    (fact_root / "identity_facts.jsonl").write_text(
        "".join(json.dumps(f) + "\n" for f in IDENTITIES)
    )
    (fact_root / "event_facts.jsonl").write_text("".join(json.dumps(f) + "\n" for f in EVENTS))
    out = tmp_path / "enriched.parquet"
    summary = build_enriched(eras_path, fact_root, out)

    frame = pl.read_parquet(out).sort("symbol_era_id")
    rows = {row["symbol_era_id"]: row for row in frame.iter_rows(named=True)}
    assert rows["AAA#001"]["identity_tier"] == "verified"  # best tier wins over asserted
    assert rows["AAA#001"]["identity_issuer"] == "Alpha Corp"
    assert rows["AAA#001"]["identity_usable_default"] is True
    assert rows["BBB#001"]["identity_tier"] == "openfigi_asserted"
    assert rows["BBB#001"]["identity_usable_default"] is True
    assert rows["CCC#001"]["identity_tier"] is None
    assert rows["CCC#001"]["identity_usable_default"] is False
    assert rows["BBB#001"]["event_type"] == "delisting_form25"
    assert summary["total_eras"] == 4
    assert summary["usable_default_eras"] == 2
    assert summary["usable_default_trade_rows"] == 300
    assert summary["fund_etf_eras"] == 1
    assert summary["fund_etf_median_span_days"] is not None
