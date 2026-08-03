"""Build and load the old-to-new `symbol_era_id` remap for the quarantined era rebuild.

The 2026-07 session-validity quarantine removed weekend test-capture eras and renumbered
positional `symbol_era_id`s. Legacy resolution inputs (manual overrides, resolution ledger,
V2 fact store) are keyed to the old IDs. This module derives a deterministic old→new map by
matching eras on `(symbol, first_day, last_day)`, then relaxing to a single date anchor, and
writes it as a review artifact. Ambiguous matches abort rather than guess. [CA][IV][KBT]
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging

DEFAULT_OLD_REVIEW_PATH = Path("reports/dead-ticker-review/dead_ticker_review_queue.csv")
DEFAULT_NEW_ERAS_PATH = Path("reports/symbol-stability/symbol_eras.parquet")
DEFAULT_REMAP_PATH = Path("reports/dead-ticker-review/era_id_remap.csv")

REMAP_COLUMNS = ("old_era_id", "new_era_id", "match_kind")
MATCH_TIERS = ("exact_dates", "first_day_anchor", "last_day_anchor")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-review-path", default=str(DEFAULT_OLD_REVIEW_PATH))
    parser.add_argument("--new-eras-path", default=str(DEFAULT_NEW_ERAS_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_REMAP_PATH))
    args = parser.parse_args()
    setup_logging(str(Path(args.output_path).parent / "era_id_remap.jsonl"))
    summary = build_era_id_remap(
        Path(args.old_review_path), Path(args.new_eras_path), Path(args.output_path)
    )
    get_logger(__name__).info(
        "era id remap complete", extra={"event": "era_id_remap_complete", "detail": summary}
    )
    return 0


def build_era_id_remap(
    old_review_path: Path, new_eras_path: Path, output_path: Path
) -> dict[str, Any]:
    old = _normalize_days(
        pl.read_csv(
            old_review_path, columns=["symbol", "symbol_era_id", "first_day", "last_day"]
        ).rename({"symbol_era_id": "old_era_id"})
    )
    new = _normalize_days(
        pl.read_parquet(
            new_eras_path, columns=["symbol", "symbol_era_id", "first_day", "last_day"]
        ).rename({"symbol_era_id": "new_era_id"})
    )
    rows, remaining = _match_tier(old, new, join_on=["symbol", "first_day", "last_day"], kind=None)
    for keys, kind in (
        (["symbol", "first_day"], "last_day_shift"),
        (["symbol", "last_day"], "first_day_shift"),
    ):
        tier_rows, remaining = _match_tier(remaining, new, join_on=keys, kind=kind)
        rows.extend(tier_rows)
    summary = _summary(rows, remaining)
    _write_outputs(output_path, rows, summary)
    return summary


def load_era_id_remap(path: Path) -> dict[str, str]:
    """Return old→new era id map; unchanged IDs are included for completeness."""
    with path.open(newline="", encoding="utf-8") as handle:
        return {row["old_era_id"]: row["new_era_id"] for row in csv.DictReader(handle)}


def remap_era_ids(rows: list[dict[str, Any]], mapping: dict[str, str]) -> tuple[int, int]:
    """Translate `symbol_era_id` in place; return (remapped, vanished) counts."""
    remapped = vanished = 0
    for row in rows:
        era_id = row.get("symbol_era_id", "")
        if era_id not in mapping:
            vanished += 1
            continue
        new_id = mapping[era_id]
        if new_id != era_id:
            row["symbol_era_id"] = new_id
            remapped += 1
    return remapped, vanished


def remap_frame(
    frame: pl.DataFrame, mapping: dict[str, str], column: str = "symbol_era_id"
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Translate era ids in a DataFrame, dropping rows on eras the remap does not cover.

    Uncovered ids must not pass through: a vanished old id can collide with an
    unrelated same-symbol era in the new build and misattach the row. [KBT]
    """
    stats = {"remapped": 0, "unmapped_dropped": 0}
    if not mapping or column not in frame.columns:
        return frame, stats
    ids = frame[column].to_list()
    stats["remapped"] = sum(1 for i in ids if i in mapping and mapping[i] != i)
    stats["unmapped_dropped"] = sum(1 for i in ids if i not in mapping)
    translated = frame.filter(pl.col(column).is_in(list(mapping))).with_columns(
        pl.col(column).replace(mapping).alias(column)
    )
    return translated, stats


def _normalize_days(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        pl.col("first_day").cast(pl.Utf8).str.replace_all("-", ""),
        pl.col("last_day").cast(pl.Utf8).str.replace_all("-", ""),
    )


def _match_tier(
    old: pl.DataFrame, new: pl.DataFrame, join_on: list[str], kind: str | None
) -> tuple[list[dict[str, Any]], pl.DataFrame]:
    joined = old.join(new, on=join_on, how="left")
    matched = joined.filter(pl.col("new_era_id").is_not_null())
    ambiguous = matched.group_by("old_era_id").len().filter(pl.col("len") > 1)
    if ambiguous.height:
        raise ValueError(
            f"ambiguous era remap on {join_on}: {sorted(ambiguous['old_era_id'].to_list())[:5]}"
        )
    rows = [
        {
            "old_era_id": r["old_era_id"],
            "new_era_id": r["new_era_id"],
            "match_kind": kind
            or ("unchanged" if r["old_era_id"] == r["new_era_id"] else "id_shift"),
        }
        for r in matched.iter_rows(named=True)
    ]
    remaining = joined.filter(pl.col("new_era_id").is_null()).drop("new_era_id")
    right_cols = [col for col in remaining.columns if col.endswith("_right")]
    return rows, remaining.drop(right_cols)


def _summary(rows: list[dict[str, Any]], remaining: pl.DataFrame) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["match_kind"]] = counts.get(row["match_kind"], 0) + 1
    counts["vanished"] = remaining.height
    counts["total_old_eras"] = len(rows) + remaining.height
    return counts


def _write_outputs(output_path: Path, rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(REMAP_COLUMNS))
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: r["old_era_id"]))
    output_path.with_suffix(".summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    raise SystemExit(main())
