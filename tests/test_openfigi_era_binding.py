from __future__ import annotations

import json

import polars as pl

from utils.openfigi_era_binding import (
    build_event_candidates,
    build_identity_candidates,
    summarize,
    write_stage,
)

ERAS = pl.DataFrame(
    {
        "symbol": ["AAA", "AAA", "BBB", "CCC", "DDD"],
        "symbol_era_id": ["AAA#001", "AAA#002", "BBB#001", "CCC#001", "DDD#001"],
        "first_day": ["20170101", "20190101", "20180101", "20190101", "20200101"],
        "last_day": ["20171231", "20191231", "20181231", "20191231", "20201231"],
        "openfigi_class": ["equity_common"] * 4 + ["fund_etf"],
        "match_status": ["single", "single", "multi", "multi", "unmatched"],
        "figi_count": [1, 1, 2, 2, 0],
    }
)

FIGI_MAP = pl.DataFrame(
    {
        "symbol": ["AAA", "BBB", "BBB", "CCC", "CCC", "DDD"],
        "figi": ["F1", "F2", "F3", "F4", "F5", "F6"],
        "composite_figi": ["C1", "C2", "C3", "C4", "C5", "C6"],
        "name": ["Alpha Corp", "Beta One Inc", "Beta Two Inc", "Gamma A", "Gamma B", "Delta"],
        "security_type2": ["Common Stock"] * 6,
        "match_status": ["single", "multi", "multi", "multi", "multi", "unmatched"],
    }
)

MATCHES = pl.DataFrame(
    {
        "symbol_era_id": ["BBB#001", "CCC#001", "AAA#001"],
        "symbol": ["BBB", "CCC", "AAA"],
        "openfigi_class": ["equity_common"] * 3,
        "gap_status": ["delisted_or_acquired_candidate"] * 3,
        "first_day": ["20180101", "20190101", "20170101"],
        "last_day": ["20181231", "20191231", "20171231"],
        "source": ["sec_form25"] * 3,
        "catalog_ticker": ["BBB", "CCC", "AAA"],
        "catalog_name": ["Beta Two", None, "Alpha Corp"],
        "catalog_issuer": ["Beta Two Inc", None, "Alpha Corp"],
        "catalog_event_date": ["2018-12-30", "2019-12-30", "2017-12-30"],
        "catalog_inception_date": [None, None, None],
    }
)


def test_identity_candidates_single_and_multi() -> None:
    identity, stats = build_identity_candidates(ERAS, FIGI_MAP, MATCHES, exclude=set())
    by_era = {fact["symbol_era_id"]: fact for fact in identity}
    assert set(by_era) == {"AAA#001", "AAA#002", "BBB#001"}
    assert by_era["AAA#001"]["entity_id"] == "F1"
    assert by_era["AAA#001"]["verification_state"] == "candidate"
    assert by_era["AAA#001"]["evidence_method"] == "openfigi_symbol_identity"
    assert "form25_name_disambiguated" in by_era["BBB#001"]["flags"]
    assert by_era["BBB#001"]["entity_id"] == "F3"  # Beta Two bound via Form 25 name
    assert stats == {"single_figi_staged": 2, "multi_figi_bound": 1, "multi_figi_held": 1}


def test_identity_candidates_respect_exclusions() -> None:
    identity, stats = build_identity_candidates(ERAS, FIGI_MAP, MATCHES, exclude={"AAA#001"})
    assert {fact["symbol_era_id"] for fact in identity} == {"AAA#002", "BBB#001"}
    assert stats["single_figi_staged"] == 1


def test_event_candidates() -> None:
    events = build_event_candidates(MATCHES, exclude=set())
    assert len(events) == 3
    sample = events[0]
    assert sample["event_type"] == "delisting_form25"
    assert sample["verification_state"] == "event_candidate"
    excluded = build_event_candidates(MATCHES, exclude={"AAA#001"})
    assert {fact["symbol_era_id"] for fact in excluded} == {"BBB#001", "CCC#001"}


def test_write_stage_shape(tmp_path) -> None:
    identity, stats = build_identity_candidates(ERAS, FIGI_MAP, MATCHES, exclude=set())
    events = build_event_candidates(MATCHES, exclude=set())
    summary = summarize(identity, events, stats, 17_677)
    stage_dir = write_stage(tmp_path, identity, events, summary)
    manifest = json.loads((stage_dir / "stage_manifest.json").read_text())
    assert manifest["status"] == "complete"
    assert manifest["identity_candidates"] == 3
    lines = (stage_dir / "identity_facts.jsonl").read_text().splitlines()
    assert len(lines) == 3
    fact = json.loads(lines[0])
    assert fact["record_type"] == "identity"
    assert fact["fact_id"].startswith("identity:")
    assert summary["unique_eras_with_identity_candidate"] == 3


def test_apply_corroboration_labels() -> None:
    from utils.openfigi_era_binding import apply_corroboration

    identity, _ = build_identity_candidates(ERAS, FIGI_MAP, MATCHES, exclude=set())
    sec = pl.DataFrame({"symbol_era_id": ["AAA#001"], "sec_name": ["Alpha Corp"]})
    counts = apply_corroboration(identity, MATCHES, sec)
    by_era = {fact["symbol_era_id"]: fact for fact in identity}
    assert counts["form25_agree"] == 2  # AAA#001 Alpha Corp, BBB#001 Beta Two
    assert counts["uncorroborated"] == 1  # AAA#002
    assert any(f == "corroboration:form25_agree" for f in by_era["AAA#001"]["flags"])
    assert by_era["AAA#001"]["fact_id"].startswith("identity:")


def test_corroboration_sets_verification_states() -> None:
    from utils.openfigi_era_binding import apply_corroboration

    identity, _ = build_identity_candidates(ERAS, FIGI_MAP, MATCHES, exclude=set())
    apply_corroboration(identity, MATCHES, None)
    states = {fact["symbol_era_id"]: fact["verification_state"] for fact in identity}
    assert states["AAA#001"] == "corroborated"  # form25_agree
    assert states["BBB#001"] == "corroborated"
    assert states["AAA#002"] == "openfigi_asserted"  # uncorroborated
