from __future__ import annotations

from pathlib import Path

import polars as pl

from utils.build_symbol_change_candidates import (
    CandidateConfig,
    build_symbol_change_candidates,
    issuer_score,
    mutual_best_pairs,
    normalize_issuer,
    pair_eras,
    volume_score,
)


def _era(symbol: str, era_id: str, first: str, last: str, rows: int, **hints) -> dict:
    return {
        "symbol": symbol,
        "symbol_era_id": era_id,
        "first_day": first,
        "last_day": last,
        "trade_rows": rows,
        "iex_latest_issuer": hints.get("issuer"),
        "sec_cik": hints.get("cik"),
        "sec_name": hints.get("sec_name"),
    }


def test_pair_eras_recovers_rename_and_excludes_same_symbol() -> None:
    eras = [
        _era("FB", "FB#001", "20161212", "20220608", 1000, issuer="Meta Platforms", cik=1),
        _era("META", "META#001", "20220609", "20260622", 1200, issuer="Meta Platforms", cik=1),
        _era("FB", "FB#002", "20220609", "20260622", 5),  # same-symbol re-split: not a rename
        _era("ZZZ", "ZZZ#001", "20220701", "20260622", 10),  # gap too large (23 days)
    ]
    pairs = pair_eras(eras, max_gap_days=10)

    by_pair = {(p["a_symbol"], p["b_symbol"]): p for p in pairs}
    assert ("FB", "META") in by_pair
    assert ("FB", "FB") not in by_pair
    assert ("FB", "ZZZ") not in by_pair

    hit = by_pair[("FB", "META")]
    assert hit["gap_days"] == 1
    assert hit["boundary_score"] == 1.0
    assert hit["issuer_score"] == 1.0
    assert hit["cik_score"] == 1.0
    assert hit["research_status"] == "candidate_needs_review"


def test_boundary_score_decays_with_gap() -> None:
    eras = [
        _era("AAA", "AAA#001", "20200101", "20220101", 100),
        _era("BBB", "BBB#001", "20220102", "20220622", 100),
        _era("CCC", "CCC#001", "20220111", "20220622", 100),
    ]
    pairs = {(p["a_symbol"], p["b_symbol"]): p for p in pair_eras(eras, 10)}
    assert pairs[("AAA", "BBB")]["score"] > pairs[("AAA", "CCC")]["score"]


def test_volume_and_issuer_scoring() -> None:
    assert volume_score(100, 1000) == 0.1
    assert volume_score(0, 1000) == 0.25
    assert issuer_score("Meta Platforms, Inc.", "META PLATFORMS") == 1.0
    assert issuer_score("Apple Inc", "Microsoft Corp") == 0.0
    assert issuer_score(None, "Microsoft Corp") == 0.3
    assert normalize_issuer("The Walt Disney Company") == {"WALT", "DISNEY"}


def test_mutual_best_pairs_requires_reciprocity() -> None:
    eras = [
        _era("AAA", "AAA#001", "20200101", "20220101", 100),
        _era("CCC", "CCC#001", "20190101", "20220101", 9000),
        _era("BBB", "BBB#001", "20220102", "20220622", 500),
    ]
    pairs = pair_eras(eras, 10)
    # BBB's heaviest predecessor is CCC, so AAA->BBB is one-sided and drops out;
    # CCC->BBB is mutual and survives.
    kept = mutual_best_pairs(pairs)
    assert [(p["a_symbol"], p["b_symbol"]) for p in kept] == [("CCC", "BBB")]


def test_build_symbol_change_candidates_end_to_end(tmp_path: Path) -> None:
    eras_path = tmp_path / "eras.parquet"
    pl.DataFrame(
        {
            "symbol": ["FB", "META"],
            "symbol_era_id": ["FB#001", "META#001"],
            "first_day": ["20161212", "20220609"],
            "last_day": ["20220608", "20260622"],
            "trade_rows": [1000, 1100],
        }
    ).write_parquet(eras_path)

    iex_path = tmp_path / "iex.parquet"
    pl.DataFrame(
        {
            "symbol_era_id": ["FB#001", "META#001"],
            "iex_latest_issuer": ["Meta Platforms", "Meta Platforms"],
        }
    ).write_parquet(iex_path)
    sec_path = tmp_path / "sec.parquet"
    pl.DataFrame(
        {
            "symbol_era_id": ["FB#001", "META#001"],
            "sec_cik": [1, 1],
            "sec_name": ["Meta Platforms, Inc.", "Meta Platforms, Inc."],
        }
    ).write_parquet(sec_path)

    result = build_symbol_change_candidates(
        CandidateConfig(
            eras_path=eras_path,
            output_root=tmp_path / "out",
            iex_enriched_path=iex_path,
            sec_enriched_path=sec_path,
            max_gap_days=10,
        )
    )

    candidates = result["candidates"]
    assert len(candidates) == 1
    assert candidates[0]["a_symbol"] == "FB" and candidates[0]["b_symbol"] == "META"
    assert result["summary"]["seed_recovery"]["FB->META"]["recovered"] is True
    assert (tmp_path / "out" / "candidates.csv").exists()
    assert (tmp_path / "out" / "summary.json").exists()
