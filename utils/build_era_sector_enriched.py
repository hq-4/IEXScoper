"""Orchestrates SIC/sector enrichment: reconcile CIKs, fetch SIC for the distinct
resolved CIKs, join the SIC-division rollup, and write one table for the whole era
universe. Only reads external SEC data and writes regenerable `reports/` output —
nothing is "applied" to the tracked canonical fact store, so there is no dry-run/apply
gate the way `apply_openfigi_identity_candidates.py` has one. `--limit-ciks` bounds a
first supervised run instead. [CA][REH][KBT]
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.resolution_v2_network import CachedPrimaryClient, NetworkConfig
from utils.resolution_v2_registry import EvidenceRegistry
from utils.sec_sic_client import fetch_many
from utils.sector_cik_reconcile import distinct_ciks, reconcile_cik
from utils.sector_enrichment_inputs import (
    apply_blank_sic_lead,
    apply_iex_fallback_issuer,
    apply_identity_disproven,
    load_edgar_matches,
    load_iex_fallback_names,
    load_name_matches,
    load_stable_classes,
)
from utils.sector_enrichment_report import build_summary, write_outputs
from utils.sic_division_table import sic_division_code_expr, sic_division_name_expr
from utils.ticker_continuity import (
    CONTINUITY_LOOKUP_SCHEMA,
    apply_continuity_status,
    fetch_many_current_tickers,
)

DEFAULT_ERA_IDENTITY_PATH = Path("reports/era-identity/eras_identity_enriched.parquet")
DEFAULT_SEC_TICKER_CIK_PATH = Path("reports/sec-ticker-cik/symbol_eras_sec_enriched.parquet")
DEFAULT_STABLE_OPENFIGI_PATH = Path("reports/openfigi-identity-stable/era_classes.parquet")
DEFAULT_SEC_COMPANY_TICKERS_PATH = Path(
    "reports/sec-ticker-cik/sec_company_tickers_exchange.parquet"
)
DEFAULT_EDGAR_MATCHES_PATH = Path(
    "reports/edgar-company-search/edgar_company_search_matches.parquet"
)
DEFAULT_IEX_ERAS_PATH = Path("reports/iex-entity-enrichment/symbol_eras_iex_enriched.parquet")
DEFAULT_OUTPUT_ROOT = Path("reports/era-identity")
DEFAULT_REGISTRY_PATH = Path("data/resolution/evidence_registry.sqlite")
DEFAULT_USER_AGENT = "IEXScoper research contact@example.com"
DEFAULT_DELAY_SECONDS = 0.3  # ~3.3 req/sec, comfortably under SEC's 10 req/sec guidance
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_RETRIES = 3
DEFAULT_MAX_AGE_DAYS = 90
FUND_INSTRUMENT_CLASS = "fund_etf"

SIC_LOOKUP_SCHEMA = {
    "cik": pl.String,
    "sic": pl.String,
    "sic_description": pl.String,
    "entity_name": pl.String,
    "fetch_status": pl.String,
    "from_cache": pl.Boolean,
}


@dataclass(frozen=True)
class SectorConfig:
    era_identity_path: Path = DEFAULT_ERA_IDENTITY_PATH
    sec_ticker_cik_path: Path = DEFAULT_SEC_TICKER_CIK_PATH
    stable_openfigi_path: Path = DEFAULT_STABLE_OPENFIGI_PATH
    sec_company_tickers_path: Path = DEFAULT_SEC_COMPANY_TICKERS_PATH
    edgar_matches_path: Path = DEFAULT_EDGAR_MATCHES_PATH
    iex_eras_path: Path = DEFAULT_IEX_ERAS_PATH
    output_root: Path = DEFAULT_OUTPUT_ROOT
    registry_path: Path = DEFAULT_REGISTRY_PATH
    user_agent: str = ""
    delay_seconds: float = DEFAULT_DELAY_SECONDS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    retries: int = DEFAULT_RETRIES
    max_age_days: int = DEFAULT_MAX_AGE_DAYS
    limit_ciks: int | None = None
    skip_fetch: bool = False
    refresh: bool = False


def main() -> int:
    args = parse_args()
    config = SectorConfig(
        era_identity_path=Path(args.era_identity_path),
        sec_ticker_cik_path=Path(args.sec_ticker_cik_path),
        stable_openfigi_path=Path(args.stable_openfigi_path),
        sec_company_tickers_path=Path(args.sec_company_tickers_path),
        edgar_matches_path=Path(args.edgar_matches_path),
        iex_eras_path=Path(args.iex_eras_path),
        output_root=Path(args.output_root),
        registry_path=Path(args.registry_path),
        user_agent=args.user_agent or os.getenv("SEC_USER_AGENT") or DEFAULT_USER_AGENT,
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
        max_age_days=args.max_age_days,
        limit_ciks=args.limit_ciks,
        skip_fetch=args.skip_fetch,
        refresh=args.refresh,
    )
    config.output_root.mkdir(parents=True, exist_ok=True)
    setup_logging(str(config.output_root / "era_sector_enriched.jsonl"))
    result = build_era_sector_enriched(config)
    get_logger(__name__).info(
        "era sector enrichment complete",
        extra={"event": "era_sector_enriched_complete", "detail": result["summary"]},
    )
    return 0


def build_era_sector_enriched(config: SectorConfig) -> dict[str, Any]:
    validate_inputs(config)
    config.output_root.mkdir(parents=True, exist_ok=True)
    era_identity = pl.read_parquet(config.era_identity_path)
    iex_fallback = load_iex_fallback_names(config.iex_eras_path)
    era_identity = apply_iex_fallback_issuer(era_identity, iex_fallback)
    sec_ticker_cik = pl.read_parquet(config.sec_ticker_cik_path)
    name_matches = load_name_matches(config.sec_company_tickers_path, era_identity)
    edgar_matches = load_edgar_matches(config.edgar_matches_path)
    cik_table = reconcile_cik(era_identity, sec_ticker_cik, name_matches, edgar_matches)
    ciks = distinct_ciks(cik_table)
    fetch_ciks = ciks[: config.limit_ciks] if config.limit_ciks is not None else ciks
    sic_rows = [] if config.skip_fetch else _fetch_sic(config, fetch_ciks)
    sic_lookup = _sic_lookup_frame(sic_rows)
    continuity_rows = [] if config.skip_fetch else _fetch_continuity(config, fetch_ciks)
    continuity_lookup = _continuity_lookup_frame(continuity_rows)
    stable_classes = load_stable_classes(config.stable_openfigi_path)
    enriched = _build_enriched(era_identity, cik_table, sic_lookup, stable_classes)
    enriched = apply_continuity_status(enriched, continuity_lookup)
    enriched = apply_identity_disproven(enriched, edgar_matches)
    enriched = apply_blank_sic_lead(enriched, edgar_matches)
    summary = build_summary(enriched, sic_lookup, len(ciks), len(sic_rows))
    write_outputs(config.output_root, enriched, sic_lookup, summary)
    return {"summary": summary}


def validate_inputs(config: SectorConfig) -> None:
    for path, label in [
        (config.era_identity_path, "era-identity product"),
        (config.sec_ticker_cik_path, "SEC ticker/CIK enrichment"),
    ]:
        if not path.exists():
            raise FileNotFoundError(f"{label} does not exist: {path}")


def _fetch_sic(config: SectorConfig, ciks: list[str]) -> list[dict[str, Any]]:
    if not ciks:
        return []
    registry = EvidenceRegistry(config.registry_path)
    try:
        network_config = NetworkConfig(
            user_agent=config.user_agent,
            delay_seconds=config.delay_seconds,
            timeout_seconds=config.timeout_seconds,
            retries=config.retries,
        )
        client = CachedPrimaryClient(network_config, registry)
        max_age_days = 0 if config.refresh else config.max_age_days
        return fetch_many(client, ciks, max_age_days=max_age_days)
    finally:
        registry.close()


def _sic_lookup_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    return (
        pl.DataFrame(rows, schema=SIC_LOOKUP_SCHEMA)
        if rows
        else pl.DataFrame(schema=SIC_LOOKUP_SCHEMA)
    )


def _fetch_continuity(config: SectorConfig, ciks: list[str]) -> list[dict[str, Any]]:
    if not ciks:
        return []
    registry = EvidenceRegistry(config.registry_path)
    try:
        network_config = NetworkConfig(
            user_agent=config.user_agent,
            delay_seconds=config.delay_seconds,
            timeout_seconds=config.timeout_seconds,
            retries=config.retries,
        )
        client = CachedPrimaryClient(network_config, registry)
        max_age_days = 0 if config.refresh else config.max_age_days
        return fetch_many_current_tickers(client, ciks, max_age_days=max_age_days)
    finally:
        registry.close()


def _continuity_lookup_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    return (
        pl.DataFrame(rows, schema=CONTINUITY_LOOKUP_SCHEMA)
        if rows
        else pl.DataFrame(schema=CONTINUITY_LOOKUP_SCHEMA)
    )


def _build_enriched(
    era_identity: pl.DataFrame,
    cik_table: pl.DataFrame,
    sic_lookup: pl.DataFrame,
    stable_classes: pl.DataFrame,
) -> pl.DataFrame:
    joined = (
        era_identity.join(cik_table, on="symbol_era_id", how="left")
        .join(
            sic_lookup.select("cik", "sic", "sic_description", "entity_name", "fetch_status"),
            left_on="resolved_cik",
            right_on="cik",
            how="left",
        )
        .join(stable_classes, on="symbol_era_id", how="left")
    )
    joined = joined.with_columns(
        pl.coalesce(["identity_instrument", "stable_openfigi_class"]).alias("instrument_class")
    )
    return joined.with_columns(
        sic_division_code_expr("sic").alias("sector_code"),
        sic_division_name_expr("sic").alias("sector_name"),
    ).with_columns(_coverage_status_expr().alias("sic_coverage_status"))


def _coverage_status_expr() -> pl.Expr:
    is_fund = (pl.col("instrument_class") == FUND_INSTRUMENT_CLASS).fill_null(False)
    return (
        pl.when(pl.col("sic").is_not_null())
        .then(pl.lit("sic_and_sector"))
        .when(pl.col("resolved_cik").is_not_null())
        .then(pl.lit("cik_no_sic"))
        .when(is_fund)
        .then(pl.lit("fund_no_sic_needed"))
        .otherwise(pl.lit("no_cik"))
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile CIKs, fetch SIC, and join the sector rollup onto the era universe."
    )
    parser.add_argument("--era-identity-path", default=str(DEFAULT_ERA_IDENTITY_PATH))
    parser.add_argument("--sec-ticker-cik-path", default=str(DEFAULT_SEC_TICKER_CIK_PATH))
    parser.add_argument("--stable-openfigi-path", default=str(DEFAULT_STABLE_OPENFIGI_PATH))
    parser.add_argument("--sec-company-tickers-path", default=str(DEFAULT_SEC_COMPANY_TICKERS_PATH))
    parser.add_argument("--edgar-matches-path", default=str(DEFAULT_EDGAR_MATCHES_PATH))
    parser.add_argument("--iex-eras-path", default=str(DEFAULT_IEX_ERAS_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--registry-path", default=str(DEFAULT_REGISTRY_PATH))
    parser.add_argument("--user-agent", default="")
    parser.add_argument("--delay-seconds", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS)
    parser.add_argument("--limit-ciks", type=int, default=None)
    parser.add_argument("--skip-fetch", action="store_true")
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
