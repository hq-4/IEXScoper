"""Batch-runs `utils.edgar_company_search_match.match_issuer_name` over every unique
issuer name with no automatically-resolved CIK, producing
`utils.sector_cik_reconcile`'s Tier E input. Deduped by unique name — not per-era-row —
before any request is made, since many eras share an issuer across multiple listing
periods; that dedup alone is usually a meaningful cut in request count on top of the
per-name search+validate design already being as cheap as it can be (one search
request, plus a SIC/name-validation request that's a free cache hit whenever the CIK
was already seen by the main SIC pass). No dry-run/apply gate — read-only against SEC,
writes regenerable `reports/` output. [CA][REH][KBT]
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.edgar_company_search_match import STATUS_MATCHED, match_issuer_name
from utils.resolution_v2_network import CachedPrimaryClient, NetworkConfig
from utils.resolution_v2_registry import EvidenceRegistry

DEFAULT_ERAS_SECTOR_ENRICHED_PATH = Path("reports/era-identity/eras_sector_enriched.parquet")
DEFAULT_OUTPUT_ROOT = Path("reports/edgar-company-search")
DEFAULT_REGISTRY_PATH = Path("data/resolution/evidence_registry.sqlite")
DEFAULT_USER_AGENT = "IEXScoper research contact@example.com"
DEFAULT_DELAY_SECONDS = 0.3  # ~3.3 req/sec, comfortably under SEC's 10 req/sec guidance
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_RETRIES = 3
DEFAULT_MAX_AGE_DAYS = 90
LOG_EVERY = 100

RESULT_SCHEMA = {
    "identity_issuer": pl.String,
    "match_status": pl.String,
    "matched_cik": pl.String,
    "candidate_count": pl.Int64,
    "candidate_name": pl.String,
    "sic": pl.String,
    "sic_description": pl.String,
}


@dataclass(frozen=True)
class EdgarSearchConfig:
    eras_sector_enriched_path: Path = DEFAULT_ERAS_SECTOR_ENRICHED_PATH
    output_root: Path = DEFAULT_OUTPUT_ROOT
    registry_path: Path = DEFAULT_REGISTRY_PATH
    user_agent: str = ""
    delay_seconds: float = DEFAULT_DELAY_SECONDS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    retries: int = DEFAULT_RETRIES
    max_age_days: int = DEFAULT_MAX_AGE_DAYS
    limit_names: int | None = None
    skip_fetch: bool = False


def main() -> int:
    args = parse_args()
    config = EdgarSearchConfig(
        eras_sector_enriched_path=Path(args.eras_sector_enriched_path),
        output_root=Path(args.output_root),
        registry_path=Path(args.registry_path),
        user_agent=args.user_agent or os.getenv("SEC_USER_AGENT") or DEFAULT_USER_AGENT,
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
        max_age_days=args.max_age_days,
        limit_names=args.limit_names,
        skip_fetch=args.skip_fetch,
    )
    config.output_root.mkdir(parents=True, exist_ok=True)
    setup_logging(str(config.output_root / "edgar_company_search.jsonl"))
    result = build_edgar_company_search_matches(config)
    get_logger(__name__).info(
        "EDGAR company search complete",
        extra={"event": "edgar_company_search_complete", "detail": result["summary"]},
    )
    return 0


def build_edgar_company_search_matches(config: EdgarSearchConfig) -> dict[str, Any]:
    validate_inputs(config)
    config.output_root.mkdir(parents=True, exist_ok=True)
    names = unresolved_issuer_names(config.eras_sector_enriched_path)
    search_names = names[: config.limit_names] if config.limit_names is not None else names
    rows = [] if config.skip_fetch else _search_all(config, search_names)
    matches = pl.DataFrame(rows, schema=RESULT_SCHEMA) if rows else pl.DataFrame(schema=RESULT_SCHEMA)
    summary = build_summary(len(names), matches)
    write_outputs(config.output_root, matches, summary)
    return {"summary": summary}


def validate_inputs(config: EdgarSearchConfig) -> None:
    if not config.eras_sector_enriched_path.exists():
        raise FileNotFoundError(
            f"era-sector enriched product does not exist: {config.eras_sector_enriched_path} "
            "(run utils/build_era_sector_enriched.py first)"
        )


def unresolved_issuer_names(path: Path) -> list[str]:
    """Every unique `identity_issuer` on an era with no resolved CIK and no fund/ETF
    exclusion — the exact same population as the manual-research worklist."""
    frame = pl.read_parquet(path)
    pool = frame.filter(
        pl.col("resolved_cik").is_null()
        & pl.col("identity_issuer").is_not_null()
        & (pl.col("sic_coverage_status") != "fund_no_sic_needed")
    )
    return sorted(pool["identity_issuer"].unique().drop_nulls().to_list())


def _search_all(config: EdgarSearchConfig, names: list[str]) -> list[dict[str, Any]]:
    if not names:
        return []
    logger = get_logger(__name__)
    registry = EvidenceRegistry(config.registry_path)
    try:
        network_config = NetworkConfig(
            user_agent=config.user_agent,
            delay_seconds=config.delay_seconds,
            timeout_seconds=config.timeout_seconds,
            retries=config.retries,
        )
        client = CachedPrimaryClient(network_config, registry)
        results = []
        for index, name in enumerate(names, start=1):
            results.append(match_issuer_name(client, name, max_age_days=config.max_age_days))
            if index % LOG_EVERY == 0 or index == len(names):
                logger.info(
                    "EDGAR company search progress",
                    extra={
                        "event": "edgar_company_search_progress",
                        "detail": {"done": index, "total": len(names)},
                    },
                )
        return results
    finally:
        registry.close()


def build_summary(total_names: int, matches: pl.DataFrame) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_unresolved_names": total_names,
        "names_searched": matches.height,
        "status_counts": _counts(matches, "match_status") if matches.height else {},
        "matched_count": int((matches["match_status"] == STATUS_MATCHED).sum())
        if matches.height
        else 0,
    }


def _counts(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(root: Path, matches: pl.DataFrame, summary: dict[str, Any]) -> None:
    matches.write_parquet(root / "edgar_company_search_matches.parquet", compression="zstd")
    matches.write_csv(root / "edgar_company_search_matches.csv")
    (root / "edgar_company_search_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_markdown(root / "edgar_company_search_report.md", matches, summary)


def write_markdown(path: Path, matches: pl.DataFrame, summary: dict[str, Any]) -> None:
    lines = [
        "# EDGAR Company-Name Search Matches",
        "",
        f"- Total unresolved names: `{summary['total_unresolved_names']}`",
        f"- Names searched this run: `{summary['names_searched']}`",
        f"- Matched: `{summary['matched_count']}`",
        "",
        "## Status Breakdown",
        "",
    ]
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["status_counts"].items())
    lines.extend(["", "## Sample Non-Matches (for review)", ""])
    non_matches = matches.filter(pl.col("match_status") != STATUS_MATCHED).head(20)
    lines.extend(
        [
            "| Issuer | Status | Candidates | Candidate Name |",
            "|---|---|---:|---|",
        ]
    )
    for row in non_matches.to_dicts():
        lines.append(
            "| {identity_issuer} | {match_status} | {candidate_count} | {candidate_name} |".format(
                **{
                    **row,
                    "candidate_count": row["candidate_count"] or "",
                    "candidate_name": row["candidate_name"] or "",
                }
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search EDGAR's company browse index for a CIK per unresolved issuer name."
    )
    parser.add_argument(
        "--eras-sector-enriched-path", default=str(DEFAULT_ERAS_SECTOR_ENRICHED_PATH)
    )
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--registry-path", default=str(DEFAULT_REGISTRY_PATH))
    parser.add_argument("--user-agent", default="")
    parser.add_argument("--delay-seconds", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS)
    parser.add_argument("--limit-names", type=int, default=None)
    parser.add_argument("--skip-fetch", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
