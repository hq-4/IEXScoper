from __future__ import annotations

import argparse
import json
from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl

from src.framework.logging import get_logger, setup_logging
from utils.openfigi_era_binding import (
    apply_corroboration,
    build_event_candidates,
    build_identity_candidates,
    load_resolved_era_ids,
    summarize,
    write_stage,
)

DEFAULT_ERA_CLASSES = Path("reports/openfigi-identity/era_classes.parquet")
DEFAULT_FIGI_MAP = Path("reports/openfigi-identity/symbol_figi_map.parquet")
DEFAULT_MATCHES = Path("reports/event-catalog-probe/matched_eras.parquet")
DEFAULT_IDENTITY_FACTS = Path("data/resolution/identity_facts.jsonl")
DEFAULT_LEDGER = Path("data/manual_overrides/ticker_era_resolution_ledger.csv")
DEFAULT_STAGE_ROOT = Path("data/resolution/staged")
DEFAULT_SUMMARY = Path("reports/openfigi-identity/phase3_binding_summary.json")
UNRESOLVED_BASELINE = 17_677


def main() -> int:
    args = parse_args()
    setup_logging("logs/app.jsonl")
    logger = get_logger(__name__)
    eras = pl.read_parquet(args.era_classes)
    figi_map = pl.read_parquet(args.symbol_figi_map)
    matches = pl.read_parquet(args.matches)
    resolved = load_resolved_era_ids(Path(args.identity_facts), Path(args.ledger))
    identity, stats = build_identity_candidates(eras, figi_map, matches, resolved)
    sec_enriched = pl.read_parquet(args.sec_enriched) if Path(args.sec_enriched).exists() else None
    corroboration = apply_corroboration(identity, matches, sec_enriched)
    stats.update(corroboration)
    events = build_event_candidates(matches, resolved)
    summary = summarize(identity, events, stats, UNRESOLVED_BASELINE)
    stage_dir = write_stage(Path(args.stage_root), identity, events, summary)
    summary["stage_dir"] = str(stage_dir)
    Path(args.summary).write_text(json.dumps(summary, indent=2, sort_keys=True))
    logger.info(
        "OpenFIGI era binding staged",
        extra={"event": "openfigi_era_binding_staged", "detail": summary},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage OpenFIGI identity + Form 25 event candidates."
    )
    parser.add_argument("--era-classes", default=str(DEFAULT_ERA_CLASSES))
    parser.add_argument("--symbol-figi-map", default=str(DEFAULT_FIGI_MAP))
    parser.add_argument("--matches", default=str(DEFAULT_MATCHES))
    parser.add_argument("--identity-facts", default=str(DEFAULT_IDENTITY_FACTS))
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--stage-root", default=str(DEFAULT_STAGE_ROOT))
    parser.add_argument("--summary", default=str(DEFAULT_SUMMARY))
    parser.add_argument(
        "--sec-enriched", default="reports/sec-ticker-cik/symbol_eras_sec_enriched.parquet"
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
