from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl

from src.framework.logging import get_logger, setup_logging

TIER_RANK = {"verified": 0, "corroborated": 1, "openfigi_asserted": 2}
DEFAULT_ERAS = Path("reports/symbol-stability/symbol_eras.parquet")
DEFAULT_FACT_ROOT = Path("data/resolution")
DEFAULT_OUTPUT_ROOT = Path("reports/era-identity")


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    setup_logging(str(output_root / "era_identity_enriched.jsonl"))
    logger = get_logger(__name__)
    result = build_enriched(
        Path(args.eras), Path(args.fact_root), output_root / "eras_identity_enriched.parquet"
    )
    (output_root / "summary.json").write_text(json.dumps(result, indent=2, sort_keys=True))
    logger.info(
        "Era identity enrichment complete",
        extra={"event": "era_identity_enriched", "detail": result},
    )
    return 0


def build_enriched(eras_path: Path, fact_root: Path, output_path: Path) -> dict[str, Any]:
    eras = pl.read_parquet(eras_path)
    identities = pl.from_dicts(
        _best_per_era(_read_jsonl(fact_root / "identity_facts.jsonl"), _identity_row)
    )
    events = pl.from_dicts(_best_per_era(_read_jsonl(fact_root / "event_facts.jsonl"), _event_row))
    enriched = eras.join(identities, on="symbol_era_id", how="left").join(
        events, on="symbol_era_id", how="left"
    )
    enriched = enriched.with_columns(
        pl.when(pl.col("identity_tier").is_in(["verified", "corroborated"]))
        .then(True)
        .when((pl.col("identity_tier") == "openfigi_asserted") & ~pl.col("identity_contested"))
        .then(True)
        .otherwise(False)
        .alias("identity_usable_default"),
        pl.when(pl.col("first_day").is_not_null() & pl.col("last_day").is_not_null())
        .then(
            (
                pl.col("last_day").str.strptime(pl.Date, "%Y%m%d")
                - pl.col("first_day").str.strptime(pl.Date, "%Y%m%d")
            ).dt.total_days()
        )
        .otherwise(None)
        .alias("era_span_days"),
    )
    enriched.write_parquet(output_path)
    return _summary(enriched)


def _best_per_era(facts: list[dict[str, Any]], row_fn: Any) -> list[dict[str, Any]]:
    best: dict[str, tuple[int, dict[str, Any]]] = {}
    for fact in facts:
        rank = TIER_RANK.get(str(fact.get("verification_state")), 9)
        era = fact.get("symbol_era_id")
        if era and (era not in best or rank < best[era][0]):
            best[era] = (rank, row_fn(fact))
    return [row for _, row in best.values()]


def _identity_row(fact: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol_era_id": fact["symbol_era_id"],
        "identity_tier": fact.get("verification_state"),
        "identity_issuer": fact.get("issuer"),
        "identity_entity_id": fact.get("entity_id"),
        "identity_method": fact.get("evidence_method"),
        "identity_instrument": fact.get("instrument"),
        "identity_contested": "contested" in (fact.get("flags") or []),
    }


def _event_row(fact: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol_era_id": fact["symbol_era_id"],
        "event_type": fact.get("event_type"),
        "event_date": fact.get("event_date"),
        "event_verification": fact.get("verification_state"),
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _summary(enriched: pl.DataFrame) -> dict[str, Any]:
    tiers = enriched.group_by(pl.col("identity_tier").fill_null("none")).agg(
        pl.len().alias("eras"), pl.col("trade_rows").sum().alias("trade_rows")
    )
    usable = enriched.filter("identity_usable_default")
    funds = enriched.filter(pl.col("identity_instrument") == "fund_etf")
    return {
        "total_eras": enriched.height,
        "by_identity_tier": {row["identity_tier"]: row for row in tiers.iter_rows(named=True)},
        "usable_default_eras": usable.height,
        "usable_default_trade_rows": int(usable["trade_rows"].sum() or 0),
        "fund_etf_eras": funds.height,
        "fund_etf_median_span_days": (
            int(funds["era_span_days"].drop_nulls().median()) if funds.height else None
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Join tiered identity/event facts onto symbol eras."
    )
    parser.add_argument("--eras", default=str(DEFAULT_ERAS))
    parser.add_argument("--fact-root", default=str(DEFAULT_FACT_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
