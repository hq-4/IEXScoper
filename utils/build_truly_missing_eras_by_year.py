"""Year-by-year breakdown of ticker eras with no usable canonical identity.

Answers "how much manual-resolution work is left, and in which years" directly from
the refreshed `dead_ticker_review_queue.parquet` (which now carries the OpenFIGI-tiered
canonical identity columns via `utils/canonical_identity_join.py`). An era counts as
"truly missing" when `canonical_identity_usable_default` is not `True` — i.e. no
verified/corroborated fact and no non-contested openfigi_asserted fact either, so it is
not just legacy-CSV-unresolved but has zero usable identity evidence anywhere in the
canonical store. [CDiP][KBT]
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT

DEFAULT_REVIEW_QUEUE_PATH = DEFAULT_OUTPUT_ROOT / "dead_ticker_review_queue.parquet"
DETAIL_COLUMNS = [
    "first_year",
    "symbol",
    "symbol_era_id",
    "source_classification",
    "instrument_type",
    "trade_rows",
    "first_day",
    "last_day",
]


@dataclass(frozen=True)
class TrulyMissingConfig:
    review_queue_path: Path
    output_root: Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--review-queue-path", default=str(DEFAULT_REVIEW_QUEUE_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    config = TrulyMissingConfig(
        review_queue_path=Path(args.review_queue_path), output_root=Path(args.output_root)
    )
    setup_logging(str(config.output_root / "truly_missing_eras_by_year.jsonl"))
    result = build_truly_missing_by_year(config)
    get_logger(__name__).info(
        "truly missing eras by year complete",
        extra={"event": "truly_missing_eras_by_year_complete", "detail": result["summary"]},
    )
    return 0


def build_truly_missing_by_year(config: TrulyMissingConfig) -> dict[str, Any]:
    if not config.review_queue_path.exists():
        raise FileNotFoundError(f"review queue does not exist: {config.review_queue_path}")
    config.output_root.mkdir(parents=True, exist_ok=True)
    queue = pl.read_parquet(config.review_queue_path)
    if "canonical_identity_usable_default" not in queue.columns:
        raise ValueError(
            "review queue is missing canonical_identity_usable_default; "
            "rerun utils/build_dead_ticker_review_queue.py first"
        )
    missing = queue.filter(
        ~pl.col("canonical_identity_usable_default").fill_null(False)
    ).with_columns(pl.col("first_day").str.slice(0, 4).alias("first_year"))
    by_year = year_summary(missing)
    summary = build_summary(missing, by_year)
    write_outputs(config.output_root, missing, by_year, summary)
    return {"summary": summary, "by_year": by_year.to_dicts()}


def year_summary(missing: pl.DataFrame) -> pl.DataFrame:
    return (
        missing.group_by("first_year")
        .agg(
            pl.len().alias("eras"),
            pl.col("trade_rows").sum().alias("trade_rows"),
            (pl.col("instrument_type") == "probable_operating_company")
            .sum()
            .alias("probable_operating_company"),
            (pl.col("source_classification") == "delisted_or_acquired_candidate")
            .sum()
            .alias("delisted_or_acquired"),
        )
        .sort("first_year")
    )


def build_summary(missing: pl.DataFrame, by_year: pl.DataFrame) -> dict[str, Any]:
    floor_day = missing["first_day"].min()
    at_floor = missing.filter(pl.col("first_day") == floor_day).height
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "eras with no usable canonical identity fact, grouped by first_day year",
        "total_truly_missing_eras": missing.height,
        "total_trade_rows": int(missing["trade_rows"].sum() or 0),
        "years": by_year.to_dicts(),
        "caveats": [
            "'truly missing' means canonical_identity_usable_default is not True: no "
            "verified/corroborated fact and no non-contested openfigi_asserted fact.",
            f"first_year is left-censored at the TOPS capture floor ({floor_day}); "
            f"{at_floor} of {missing.height} rows share that exact first_day, so the "
            "earliest year bucket overcounts true launches and undercounts tickers that "
            "were already trading before data collection began.",
            "instrument_type/source_classification are first-pass heuristics, not proof.",
        ],
    }


def write_outputs(
    root: Path, missing: pl.DataFrame, by_year: pl.DataFrame, summary: dict[str, Any]
) -> None:
    detail = missing.select(DETAIL_COLUMNS).sort(
        ["first_year", "trade_rows"], descending=[False, True]
    )
    detail.write_csv(root / "truly_missing_eras_by_year.csv")
    (root / "truly_missing_eras_by_year_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_markdown(root / "truly_missing_eras_by_year_report.md", detail, summary)


def write_markdown(path: Path, detail: pl.DataFrame, summary: dict[str, Any]) -> None:
    lines = [
        "# Truly Missing Ticker Eras, By Year",
        "",
        f"- Total eras with no usable canonical identity: `{summary['total_truly_missing_eras']}`",
        f"- Total trade rows: `{summary['total_trade_rows']}`",
        "",
        "## By Year",
        "",
        "| Year | Eras | Trade Rows | Probable Operating Co. | Delisted/Acquired |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in summary["years"]:
        lines.append(
            "| {first_year} | {eras} | {trade_rows} | {probable_operating_company} | "
            "{delisted_or_acquired} |".format(**row)
        )
    lines.extend(["", "## Top 10 By Year (highest trade_rows)", ""])
    for year in sorted({row["first_year"] for row in summary["years"]}):
        year_rows = detail.filter(pl.col("first_year") == year).head(10)
        if year_rows.height == 0:
            continue
        lines.extend(
            [
                f"### {year}",
                "",
                "| Symbol | Era | Type | Trade Rows | First | Last |",
                "|---|---|---|---:|---|---|",
            ]
        )
        for row in year_rows.to_dicts():
            lines.append(
                "| {symbol} | {symbol_era_id} | {instrument_type} | {trade_rows} | "
                "{first_day} | {last_day} |".format(**row)
            )
        lines.append("")
    lines.extend(["## Caveats", ""])
    lines.extend(f"- {item}" for item in summary["caveats"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
