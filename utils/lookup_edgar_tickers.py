from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import requests

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.dead_ticker_review_schema import DEFAULT_OUTPUT_ROOT as DEFAULT_DEAD_TICKER_ROOT
from utils.enrich_symbol_eras_sec import (
    SEC_TICKERS_EXCHANGE_URL,
    sec_lookup_frame,
    sec_ticker_frame,
)

DEFAULT_TEMPLATE_PATH = DEFAULT_DEAD_TICKER_ROOT / "manual_resolution_template.csv"
DEFAULT_OUTPUT_ROOT = Path("reports/dead-ticker-review/edgar-lookup")
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_SLEEP_SECONDS = 0.12
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
INTERESTING_FORMS = {"8-K", "10-K", "10-Q", "S-4", "425", "SC 13E3", "15-12B", "25-NSE"}


@dataclass(frozen=True)
class EdgarLookupConfig:
    template_path: Path | None
    output_root: Path
    cache_path: Path
    symbols: tuple[str, ...]
    user_agent: str
    timeout_seconds: float
    sleep_seconds: float
    fetch_submissions: bool
    refresh: bool


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root)
    config = EdgarLookupConfig(
        template_path=Path(args.template_path) if args.template_path else None,
        output_root=output_root,
        cache_path=Path(args.cache_path)
        if args.cache_path
        else output_root / "company_tickers_exchange.json",
        symbols=parse_symbols(args.symbols),
        user_agent=resolve_user_agent(args.user_agent),
        timeout_seconds=args.timeout_seconds,
        sleep_seconds=args.sleep_seconds,
        fetch_submissions=args.fetch_submissions,
        refresh=args.refresh,
    )
    setup_logging(str(config.output_root / "edgar_ticker_lookup.jsonl"))
    result = lookup_edgar_tickers(config)
    get_logger(__name__).info(
        "EDGAR ticker lookup complete",
        extra={"event": "edgar_ticker_lookup_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template-path", default=str(DEFAULT_TEMPLATE_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--cache-path")
    parser.add_argument(
        "--symbols", help="Comma-separated ticker symbols; overrides template symbols."
    )
    parser.add_argument("--user-agent")
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    parser.add_argument("--fetch-submissions", action="store_true")
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def parse_symbols(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(symbol.strip().upper() for symbol in value.split(",") if symbol.strip())


def resolve_user_agent(value: str | None) -> str:
    user_agent = value or os.getenv("SEC_USER_AGENT")
    if not user_agent:
        raise ValueError("provide --user-agent or set SEC_USER_AGENT before hitting EDGAR")
    return user_agent


def lookup_edgar_tickers(config: EdgarLookupConfig) -> dict[str, Any]:
    validate_config(config)
    config.output_root.mkdir(parents=True, exist_ok=True)
    symbols = load_symbols(config)
    payload = load_or_download_ticker_payload(config)
    matches = build_lookup_rows(symbols, payload)
    if config.fetch_submissions:
        matches = add_submission_evidence(matches, config)
    summary = build_summary(config, symbols, matches, payload)
    write_outputs(config.output_root, matches, summary)
    return {"summary": summary, "rows": matches.to_dicts()}


def validate_config(config: EdgarLookupConfig) -> None:
    if config.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be positive")
    if config.sleep_seconds < 0:
        raise ValueError("--sleep-seconds cannot be negative")
    if config.template_path and not config.template_path.exists() and not config.symbols:
        raise FileNotFoundError(f"template file does not exist: {config.template_path}")


def load_symbols(config: EdgarLookupConfig) -> list[str]:
    if config.symbols:
        return sorted(set(config.symbols))
    assert config.template_path is not None
    frame = pl.read_csv(config.template_path, infer_schema_length=0)
    if "symbol" not in frame.columns:
        raise ValueError(f"{config.template_path} missing required column: symbol")
    return sorted(set(frame["symbol"].drop_nulls().str.to_uppercase().to_list()))


def load_or_download_ticker_payload(config: EdgarLookupConfig) -> dict[str, Any]:
    if config.cache_path.exists() and not config.refresh:
        return json.loads(config.cache_path.read_text(encoding="utf-8"))
    payload = download_json(SEC_TICKERS_EXCHANGE_URL, config.user_agent, config.timeout_seconds)
    config.cache_path.parent.mkdir(parents=True, exist_ok=True)
    config.cache_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return payload


def download_json(url: str, user_agent: str, timeout_seconds: float) -> dict[str, Any]:
    response = requests.get(
        url,
        headers={"User-Agent": user_agent, "Accept": "application/json"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"SEC response must be a JSON object: {url}")
    return payload


def build_lookup_rows(symbols: list[str], payload: dict[str, Any]) -> pl.DataFrame:
    requested = pl.DataFrame({"symbol": symbols}).with_columns(
        symbol_key_expr().alias("sec_symbol_key")
    )
    lookup = sec_lookup_frame(sec_ticker_frame(payload))
    return (
        requested.join(lookup, on="sec_symbol_key", how="left")
        .with_columns(
            pl.when(pl.col("sec_match_count").is_null())
            .then(pl.lit("edgar_current_ticker_unmatched"))
            .when(pl.col("sec_match_count") > 1)
            .then(pl.lit("edgar_current_ticker_multiple"))
            .otherwise(pl.lit("edgar_current_ticker_match"))
            .alias("edgar_lookup_status")
        )
        .sort("symbol")
    )


def symbol_key_expr() -> pl.Expr:
    return pl.col("symbol").map_elements(
        lambda value: "".join(character for character in str(value).upper() if character.isalnum()),
        return_dtype=pl.String,
    )


def add_submission_evidence(matches: pl.DataFrame, config: EdgarLookupConfig) -> pl.DataFrame:
    rows = []
    for row in matches.to_dicts():
        cik = row.get("sec_cik")
        if cik:
            row.update(submission_summary(str(cik), config))
            time.sleep(config.sleep_seconds)
        rows.append(row)
    return pl.DataFrame(rows)


def submission_summary(cik: str, config: EdgarLookupConfig) -> dict[str, str | int | None]:
    url = SEC_SUBMISSIONS_URL.format(cik=cik)
    payload = download_json(url, config.user_agent, config.timeout_seconds)
    recent = payload.get("filings", {}).get("recent", {})
    forms = recent.get("form") if isinstance(recent, dict) else None
    accession_numbers = recent.get("accessionNumber") if isinstance(recent, dict) else None
    filing_dates = recent.get("filingDate") if isinstance(recent, dict) else None
    if not isinstance(forms, list):
        return {
            "recent_filing_count": 0,
            "interesting_forms": None,
            "latest_interesting_filing": None,
        }
    interesting = interesting_filings(forms, accession_numbers, filing_dates)
    return {
        "recent_filing_count": len(forms),
        "interesting_forms": " | ".join(sorted({item["form"] for item in interesting})) or None,
        "latest_interesting_filing": interesting[0]["summary"] if interesting else None,
    }


def interesting_filings(
    forms: list[Any], accession_numbers: Any, filing_dates: Any
) -> list[dict[str, str]]:
    if not isinstance(accession_numbers, list) or not isinstance(filing_dates, list):
        return []
    rows = []
    for form, accession, date in zip(forms, accession_numbers, filing_dates, strict=False):
        if str(form) in INTERESTING_FORMS:
            rows.append(
                {
                    "form": str(form),
                    "summary": f"{date} {form} {accession}",
                }
            )
    return rows


def build_summary(
    config: EdgarLookupConfig,
    symbols: list[str],
    matches: pl.DataFrame,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "EDGAR current ticker lookup for dead ticker manual review",
        "source_url": SEC_TICKERS_EXCHANGE_URL,
        "source_as_of": payload.get("as_of"),
        "template_path": str(config.template_path) if config.template_path else None,
        "symbol_count": len(symbols),
        "fetch_submissions": config.fetch_submissions,
        "status_counts": count_by(matches, "edgar_lookup_status"),
        "limitations": [
            "SEC ticker lookup is current-biased and does not prove historical ticker identity.",
            "Use EDGAR hits as leads, then verify issuer/event with filings or primary transaction sources.",
        ],
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {
        str(row[column]): row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(root: Path, matches: pl.DataFrame, summary: dict[str, Any]) -> None:
    matches.write_csv(root / "edgar_ticker_lookup.csv")
    matches.write_parquet(root / "edgar_ticker_lookup.parquet", compression="zstd")
    (root / "edgar_ticker_lookup_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    raise SystemExit(main())
