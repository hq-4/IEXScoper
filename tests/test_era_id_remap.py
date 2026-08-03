from __future__ import annotations

import csv
from pathlib import Path

import polars as pl
import pytest

from utils.era_id_remap import build_era_id_remap, load_era_id_remap, remap_era_ids, remap_frame


def _write_old_review(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["symbol", "symbol_era_id", "first_day", "last_day"]
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_new_eras(path: Path, rows: list[dict[str, str]]) -> None:
    pl.DataFrame(rows).write_parquet(path)


def _fixtures(tmp_path: Path) -> tuple[Path, Path]:
    old_path = tmp_path / "old.csv"
    _write_old_review(
        old_path,
        [
            {
                "symbol": "AAA",
                "symbol_era_id": "AAA#001",
                "first_day": "2016-12-12",
                "last_day": "2020-01-01",
            },
            {
                "symbol": "BBB",
                "symbol_era_id": "BBB#002",
                "first_day": "2020-01-02",
                "last_day": "2021-05-05",
            },
            {
                "symbol": "CCC",
                "symbol_era_id": "CCC#001",
                "first_day": "2016-12-12",
                "last_day": "2017-08-26",
            },
            {
                "symbol": "DDD",
                "symbol_era_id": "DDD#003",
                "first_day": "2018-01-02",
                "last_day": "2019-01-01",
            },
            {
                "symbol": "EEE",
                "symbol_era_id": "EEE#001",
                "first_day": "2016-12-12",
                "last_day": "2017-01-01",
            },
        ],
    )
    new_path = tmp_path / "new.parquet"
    _write_new_eras(
        new_path,
        [
            {
                "symbol": "AAA",
                "symbol_era_id": "AAA#001",
                "first_day": "20161212",
                "last_day": "20200101",
            },
            {
                "symbol": "BBB",
                "symbol_era_id": "BBB#001",
                "first_day": "20200102",
                "last_day": "20210505",
            },
            {
                "symbol": "CCC",
                "symbol_era_id": "CCC#001",
                "first_day": "20161212",
                "last_day": "20170825",
            },
            {
                "symbol": "DDD",
                "symbol_era_id": "DDD#002",
                "first_day": "20180103",
                "last_day": "20190101",
            },
        ],
    )
    return old_path, new_path


def test_build_era_id_remap_tiers(tmp_path: Path) -> None:
    old_path, new_path = _fixtures(tmp_path)
    out = tmp_path / "remap.csv"
    summary = build_era_id_remap(old_path, new_path, out)

    assert summary == {
        "unchanged": 1,
        "id_shift": 1,
        "last_day_shift": 1,
        "first_day_shift": 1,
        "vanished": 1,
        "total_old_eras": 5,
    }
    mapping = load_era_id_remap(out)
    assert mapping == {
        "AAA#001": "AAA#001",
        "BBB#002": "BBB#001",
        "CCC#001": "CCC#001",
        "DDD#003": "DDD#002",
    }
    assert out.with_suffix(".summary.json").exists()


def test_build_era_id_remap_aborts_on_ambiguity(tmp_path: Path) -> None:
    old_path = tmp_path / "old.csv"
    _write_old_review(
        old_path,
        [
            {
                "symbol": "AAA",
                "symbol_era_id": "AAA#002",
                "first_day": "2020-01-02",
                "last_day": "2021-05-05",
            }
        ],
    )
    new_path = tmp_path / "new.parquet"
    _write_new_eras(
        new_path,
        [
            {
                "symbol": "AAA",
                "symbol_era_id": "AAA#001",
                "first_day": "20200102",
                "last_day": "20210505",
            },
            {
                "symbol": "AAA",
                "symbol_era_id": "AAA#001X",
                "first_day": "20200102",
                "last_day": "20210505",
            },
        ],
    )
    with pytest.raises(ValueError, match="ambiguous era remap"):
        build_era_id_remap(old_path, new_path, tmp_path / "remap.csv")


def test_remap_era_ids_in_place() -> None:
    rows = [
        {"symbol_era_id": "BBB#002", "value": 1},
        {"symbol_era_id": "AAA#001", "value": 2},
        {"symbol_era_id": "EEE#001", "value": 3},
    ]
    remapped, vanished = remap_era_ids(rows, {"BBB#002": "BBB#001", "AAA#001": "AAA#001"})
    assert (remapped, vanished) == (1, 1)
    assert rows[0]["symbol_era_id"] == "BBB#001"
    assert rows[1]["symbol_era_id"] == "AAA#001"
    assert rows[2]["symbol_era_id"] == "EEE#001"


def test_remap_frame_translates_and_drops_uncovered() -> None:
    frame = pl.DataFrame({"symbol_era_id": ["AAA#009", "AAA#001", "ZZZ#001"], "value": [1, 2, 3]})
    out, stats = remap_frame(frame, {"AAA#009": "AAA#001", "AAA#001": "AAA#001"})
    assert stats == {"remapped": 1, "unmapped_dropped": 1}
    assert out["symbol_era_id"].to_list() == ["AAA#001", "AAA#001"]
    assert out["value"].to_list() == [1, 2]


def test_remap_frame_noop_without_mapping() -> None:
    frame = pl.DataFrame({"symbol_era_id": ["AAA#001"], "value": [1]})
    out, stats = remap_frame(frame, {})
    assert stats == {"remapped": 0, "unmapped_dropped": 0}
    assert out.equals(frame)
