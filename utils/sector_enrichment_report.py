"""Summary/report writers for `utils/build_era_sector_enriched.py`, split out to keep
the orchestrator under the file-size gate — purely derived from the enriched frame and
SIC lookup, no fetch/reconcile logic lives here. [CA][CDiP]
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl


def build_summary(
    enriched: pl.DataFrame, sic_lookup: pl.DataFrame, total_ciks: int, fetched_ciks: int
) -> dict[str, Any]:
    cache_hits = int(sic_lookup["from_cache"].sum()) if sic_lookup.height else 0
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_eras": enriched.height,
        "distinct_ciks_resolved": total_ciks,
        "distinct_ciks_fetched": fetched_ciks,
        "cache_hits": cache_hits,
        "network_requests": fetched_ciks - cache_hits,
        "cik_source_counts": _counts(enriched, "cik_source"),
        "sic_coverage_status_counts": _counts(enriched, "sic_coverage_status"),
        "continuity_status_counts": _counts(enriched, "continuity_status"),
        "fetch_status_counts": _counts(sic_lookup, "fetch_status") if sic_lookup.height else {},
        "sector_counts": _counts(enriched, "sector_name"),
        "by_source_classification": _counts_nested(
            enriched, "source_classification", "sic_coverage_status"
        ),
    }


def _counts(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def _counts_nested(frame: pl.DataFrame, outer: str, inner: str) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for row in frame.group_by([outer, inner]).len().sort([outer, inner]).to_dicts():
        result.setdefault(str(row[outer]), {})[str(row[inner])] = row["len"]
    return result


def write_outputs(
    root: Path, enriched: pl.DataFrame, sic_lookup: pl.DataFrame, summary: dict[str, Any]
) -> None:
    enriched.write_parquet(root / "eras_sector_enriched.parquet", compression="zstd")
    sic_lookup.write_parquet(root / "cik_sic_lookup.parquet", compression="zstd")
    (root / "sector_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_markdown(root / "sector_report.md", summary)


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Era x Sector Enrichment",
        "",
        f"- Total eras: `{summary['total_eras']}`",
        f"- Distinct CIKs resolved: `{summary['distinct_ciks_resolved']}`",
        f"- Distinct CIKs fetched this run: `{summary['distinct_ciks_fetched']}`",
        f"- Cache hits: `{summary['cache_hits']}`",
        f"- Network requests: `{summary['network_requests']}`",
        "",
        "## CIK Source",
        "",
    ]
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["cik_source_counts"].items())
    lines.extend(["", "## SIC Coverage Status", ""])
    lines.extend(
        f"- `{key}`: `{value}`" for key, value in summary["sic_coverage_status_counts"].items()
    )
    lines.extend(
        [
            "",
            "## Continuity Status",
            "",
            "Only meaningful where a CIK was resolved: `terminal` means the CIK's current SEC "
            "record shows no active ticker at all; `still_active_same_symbol` means the era's "
            "own symbol is still trading (its end date is a stale data-window artifact, not a "
            "real event); `renamed_or_successor` means the CIK trades today under a different "
            "symbol than this era's.",
            "",
        ]
    )
    lines.extend(
        f"- `{key}`: `{value}`" for key, value in summary["continuity_status_counts"].items()
    )
    lines.extend(["", "## Fetch Status", ""])
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["fetch_status_counts"].items())
    lines.extend(["", "## SIC Coverage by Source Classification", ""])
    for outer, inner_counts in summary["by_source_classification"].items():
        lines.append(f"- `{outer}`: " + ", ".join(f"{k}={v}" for k, v in inner_counts.items()))
    lines.extend(["", "## Top Sectors", ""])
    top_sectors = sorted(summary["sector_counts"].items(), key=lambda kv: kv[1], reverse=True)
    lines.extend(f"- `{key}`: `{value}`" for key, value in top_sectors[:15])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
