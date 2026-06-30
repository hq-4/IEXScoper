from __future__ import annotations

import argparse
import json
import os
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

SEC_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
DEFAULT_SYMBOL_ERAS_PATH = Path("reports/symbol-stability/symbol_eras.parquet")
DEFAULT_OUTPUT_ROOT = Path("reports/sec-ticker-cik")
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_USER_AGENT = "IEXScoper/0.1 research contact@example.com"


@dataclass(frozen=True)
class SecTickerConfig:
    symbol_eras_path: Path
    output_root: Path
    cache_path: Path
    user_agent: str
    timeout_seconds: float
    refresh: bool


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root)
    config = SecTickerConfig(
        symbol_eras_path=Path(args.symbol_eras_path),
        output_root=output_root,
        cache_path=Path(args.cache_path) if args.cache_path else output_root / "company_tickers_exchange.json",
        user_agent=args.user_agent or os.getenv("SEC_USER_AGENT") or DEFAULT_USER_AGENT,
        timeout_seconds=args.timeout_seconds,
        refresh=args.refresh,
    )
    setup_logging(str(config.output_root / "sec_ticker_cik_enrichment.jsonl"))
    result = enrich_symbol_eras_sec(config)
    get_logger(__name__).info(
        "SEC ticker/CIK enrichment complete",
        extra={"event": "sec_ticker_cik_enrichment_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol-eras-path", default=str(DEFAULT_SYMBOL_ERAS_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--cache-path")
    parser.add_argument("--user-agent")
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def enrich_symbol_eras_sec(config: SecTickerConfig) -> dict[str, Any]:
    validate_inputs(config)
    config.output_root.mkdir(parents=True, exist_ok=True)
    payload = load_or_download_sec_payload(config)
    sec_rows = sec_ticker_frame(payload)
    lookup = sec_lookup_frame(sec_rows)
    symbol_eras = pl.read_parquet(config.symbol_eras_path)
    enriched = enrich_symbol_eras(symbol_eras, lookup)
    summary = build_summary(config, payload, sec_rows, lookup, enriched)
    write_outputs(config.output_root, summary, sec_rows, lookup, enriched)
    return {"summary": summary, "rows": enriched.to_dicts()}


def validate_inputs(config: SecTickerConfig) -> None:
    if not config.symbol_eras_path.exists():
        raise FileNotFoundError(f"symbol eras parquet does not exist: {config.symbol_eras_path}")


def load_or_download_sec_payload(config: SecTickerConfig) -> dict[str, Any]:
    if config.cache_path.exists() and not config.refresh:
        return json.loads(config.cache_path.read_text(encoding="utf-8"))
    payload = download_sec_payload(config.user_agent, config.timeout_seconds)
    config.cache_path.parent.mkdir(parents=True, exist_ok=True)
    config.cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def download_sec_payload(user_agent: str, timeout_seconds: float) -> dict[str, Any]:
    response = requests.get(
        SEC_TICKERS_EXCHANGE_URL,
        headers={"User-Agent": user_agent, "Accept": "application/json"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("SEC ticker response must be a JSON object")
    return payload


def sec_ticker_frame(payload: dict[str, Any]) -> pl.DataFrame:
    fields = payload.get("fields")
    data = payload.get("data")
    if fields != ["cik", "name", "ticker", "exchange"] or not isinstance(data, list):
        raise ValueError("unexpected SEC company_tickers_exchange payload shape")
    rows = [
        {
            "sec_cik": str(item[0]).zfill(10),
            "sec_name": str(item[1]),
            "sec_ticker": str(item[2]).upper(),
            "sec_exchange": str(item[3]),
            "sec_symbol_key": symbol_key(str(item[2])),
        }
        for item in data
        if isinstance(item, list) and len(item) == 4 and item[2]
    ]
    return pl.DataFrame(rows).sort("sec_ticker")


def sec_lookup_frame(sec_rows: pl.DataFrame) -> pl.DataFrame:
    return (
        sec_rows.group_by("sec_symbol_key")
        .agg(
            pl.len().alias("sec_match_count"),
            pl.col("sec_cik").sort().first().alias("sec_cik"),
            pl.col("sec_name").sort().first().alias("sec_name"),
            pl.col("sec_ticker").sort().first().alias("sec_ticker"),
            pl.col("sec_exchange").sort().first().alias("sec_exchange"),
            pl.col("sec_ticker").sort().str.join(" | ").alias("sec_ticker_variants"),
        )
        .sort("sec_symbol_key")
    )


def enrich_symbol_eras(symbol_eras: pl.DataFrame, lookup: pl.DataFrame) -> pl.DataFrame:
    require_columns(symbol_eras, ["symbol", "symbol_era_id", "source_classification"])
    return (
        symbol_eras.with_columns(pl.col("symbol").map_elements(symbol_key, return_dtype=pl.String).alias("sec_symbol_key"))
        .join(lookup, on="sec_symbol_key", how="left")
        .with_columns(sec_confidence_expr().alias("sec_current_confidence"))
        .sort(["symbol", "symbol_era_id"])
    )


def require_columns(frame: pl.DataFrame, columns: list[str]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"symbol eras parquet missing required columns: {missing}")


def sec_confidence_expr() -> pl.Expr:
    return (
        pl.when(pl.col("sec_match_count").is_null())
        .then(pl.lit("sec_unmatched"))
        .when(pl.col("sec_match_count") > 1)
        .then(pl.lit("sec_multiple_current_matches"))
        .otherwise(pl.lit("sec_current_match"))
    )


def symbol_key(symbol: str) -> str:
    return "".join(character for character in symbol.upper() if character.isalnum())


def build_summary(
    config: SecTickerConfig,
    payload: dict[str, Any],
    sec_rows: pl.DataFrame,
    lookup: pl.DataFrame,
    enriched: pl.DataFrame,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "SEC current ticker to CIK enrichment for symbol eras",
        "source_url": SEC_TICKERS_EXCHANGE_URL,
        "source_as_of": payload.get("as_of"),
        "symbol_eras_path": str(config.symbol_eras_path),
        "cache_path": str(config.cache_path),
        "sec_row_count": sec_rows.height,
        "sec_lookup_symbol_count": lookup.height,
        "symbol_era_count": enriched.height,
        "unique_symbol_count": enriched.select("symbol").n_unique(),
        "confidence_counts": count_by(enriched, "sec_current_confidence"),
        "classification_confidence_counts": classification_counts(enriched),
        "limitations": [
            "SEC ticker mapping is current-biased and is not historical ticker identity proof.",
            "CIK coverage is strongest for SEC registrants and weaker for funds, warrants, units, and dead tickers.",
            "Intermittent eras must remain keyed by symbol_era_id until historical listing evidence is added.",
        ],
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {row[column]: row["len"] for row in frame.group_by(column).len().sort(column).to_dicts()}


def classification_counts(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return (
        frame.group_by("source_classification", "sec_current_confidence")
        .len()
        .sort(["source_classification", "sec_current_confidence"])
        .to_dicts()
    )


def write_outputs(
    root: Path,
    summary: dict[str, Any],
    sec_rows: pl.DataFrame,
    lookup: pl.DataFrame,
    enriched: pl.DataFrame,
) -> None:
    sec_rows.write_parquet(root / "sec_company_tickers_exchange.parquet", compression="zstd")
    lookup.write_parquet(root / "sec_ticker_cik_lookup.parquet", compression="zstd")
    enriched.write_parquet(root / "symbol_eras_sec_enriched.parquet", compression="zstd")
    enriched.write_csv(root / "symbol_eras_sec_enriched.csv")
    (root / "sec_ticker_cik_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_markdown(root / "sec_ticker_cik_report.md", summary)


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# SEC Ticker CIK Enrichment",
        "",
        "This report joins SEC current ticker/CIK metadata to ticker-era rows.",
        "It is current evidence, not a historical security master.",
        "",
        f"- Symbol eras: `{summary['symbol_era_count']}`",
        f"- Unique symbols: `{summary['unique_symbol_count']}`",
        f"- SEC rows: `{summary['sec_row_count']}`",
        f"- SEC lookup symbols: `{summary['sec_lookup_symbol_count']}`",
        "",
        "## Confidence Counts",
        "",
    ]
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["confidence_counts"].items())
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in summary["limitations"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
