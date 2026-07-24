"""Build review-only symbol-change (rename) candidate pairs from ticker eras.

Pairs an era A `last_day` with a different-symbol era B `first_day` within a small
calendar gap, scores the pair on boundary proximity, volume recapture, IEX issuer-name
similarity, and SEC CIK agreement, then keeps only mutual-heaviest boundary matches
ranked by recaptured volume. Candidates are review-only
(`research_status=candidate_needs_review`); nothing is imported as evidence here.
Enrichment tables may predate the current era build; missing joins score neutral. [CA][IV][KBT]
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging

DEFAULT_ERAS_PATH = Path("reports/symbol-stability/symbol_eras.parquet")
DEFAULT_IEX_ENRICHED_PATH = Path("reports/iex-entity-enrichment/symbol_eras_iex_enriched.parquet")
DEFAULT_SEC_ENRICHED_PATH = Path("reports/sec-ticker-cik/symbol_eras_sec_enriched.parquet")
DEFAULT_OUTPUT_ROOT = Path("reports/symbol-change-candidates")
DEFAULT_MAX_GAP_DAYS = 10
DAY_FORMAT = "%Y%m%d"

W_BOUNDARY = 0.35
W_VOLUME = 0.25
W_ISSUER = 0.25
W_CIK = 0.15
NEUTRAL_SCORE = 0.3
ZERO_VOLUME_SCORE = 0.25

# Known renames used as a recovery seed: old symbol -> new symbol.
SEED_RENAMES = {
    "FB": "META",
    "SQ": "XYZ",
    "GOLD": "B",
    "SWN": "EXE",
    "GPS": "GAP",
    "FISV": "FI",
    "NYCB": "FLG",
    "COG": "CTRA",
}

ISSUER_STOPWORDS = {
    "INC",
    "CORP",
    "CORPORATION",
    "CO",
    "COMPANY",
    "LTD",
    "LLC",
    "LP",
    "LLP",
    "PLC",
    "HOLDINGS",
    "HLDGS",
    "GROUP",
    "CLASS",
    "CL",
    "COM",
    "NEW",
    "THE",
}


@dataclass(frozen=True)
class CandidateConfig:
    eras_path: Path
    output_root: Path
    iex_enriched_path: Path
    sec_enriched_path: Path
    max_gap_days: int


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--eras-path", default=str(DEFAULT_ERAS_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--iex-enriched-path", default=str(DEFAULT_IEX_ENRICHED_PATH))
    parser.add_argument("--sec-enriched-path", default=str(DEFAULT_SEC_ENRICHED_PATH))
    parser.add_argument("--max-gap-days", type=int, default=DEFAULT_MAX_GAP_DAYS)
    args = parser.parse_args()
    config = CandidateConfig(
        eras_path=Path(args.eras_path),
        output_root=Path(args.output_root),
        iex_enriched_path=Path(args.iex_enriched_path),
        sec_enriched_path=Path(args.sec_enriched_path),
        max_gap_days=args.max_gap_days,
    )
    setup_logging(str(config.output_root / "symbol_change_candidates.jsonl"))
    result = build_symbol_change_candidates(config)
    get_logger(__name__).info(
        "symbol change candidates complete",
        extra={"event": "symbol_change_candidates_complete", "detail": result["summary"]},
    )
    return 0


def build_symbol_change_candidates(config: CandidateConfig) -> dict[str, Any]:
    config.output_root.mkdir(parents=True, exist_ok=True)
    eras = load_eras(config)
    pairs = pair_eras(eras, config.max_gap_days)
    # Enrichment hints are per-symbol-latest (dead eras get None or a smeared modern
    # issuer), so they cannot gate rename candidates. Selectivity comes from the
    # mutual-heaviest-boundary rule: a rename recaptures the retired symbol's volume,
    # while a delisting next to an unrelated IPO is almost never mutual. [KBT]
    candidates = sorted(
        mutual_best_pairs(pairs),
        key=lambda row: (-min(row["a_trade_rows"], row["b_trade_rows"]), row["a_symbol"]),
    )
    seed_recovery = check_seed_recovery(candidates)
    summary = build_summary(config, eras, candidates, seed_recovery, len(pairs))
    write_outputs(config.output_root, candidates, summary)
    return {"summary": summary, "candidates": candidates}


def load_eras(config: CandidateConfig) -> list[dict[str, Any]]:
    frame = pl.read_parquet(config.eras_path).select(
        "symbol", "symbol_era_id", "first_day", "last_day", "trade_rows"
    )
    frame = join_hint(frame, config.iex_enriched_path, ["symbol_era_id", "iex_latest_issuer"])
    frame = join_hint(frame, config.sec_enriched_path, ["symbol_era_id", "sec_cik", "sec_name"])
    return frame.to_dicts()


def join_hint(frame: pl.DataFrame, path: Path, columns: list[str]) -> pl.DataFrame:
    if not path.exists():
        return frame.with_columns([pl.lit(None).alias(col) for col in columns[1:]])
    hint = pl.read_parquet(path).select(columns).unique(subset=["symbol_era_id"])
    return frame.join(hint, on="symbol_era_id", how="left")


def pair_eras(eras: list[dict[str, Any]], max_gap_days: int) -> list[dict[str, Any]]:
    by_first_day: dict[str, list[dict[str, Any]]] = {}
    for era in eras:
        by_first_day.setdefault(era["first_day"], []).append(era)
    pairs = []
    for a in eras:
        for gap_days in range(1, max_gap_days + 1):
            day = shift_day(a["last_day"], gap_days)
            for b in by_first_day.get(day, []):
                if b["symbol"] != a["symbol"]:
                    pairs.append(score_pair(a, b, gap_days, max_gap_days))
    return pairs


def score_pair(
    a: dict[str, Any], b: dict[str, Any], gap_days: int, max_gap_days: int
) -> dict[str, Any]:
    scores = {
        "boundary_score": round(1 - (gap_days - 1) / max_gap_days, 4),
        "volume_score": volume_score(a["trade_rows"], b["trade_rows"]),
        "issuer_score": issuer_score(a.get("iex_latest_issuer"), b.get("iex_latest_issuer")),
        "cik_score": id_score(a.get("sec_cik"), b.get("sec_cik")),
    }
    total = (
        W_BOUNDARY * scores["boundary_score"]
        + W_VOLUME * scores["volume_score"]
        + W_ISSUER * scores["issuer_score"]
        + W_CIK * scores["cik_score"]
    )
    return {
        "a_symbol": a["symbol"],
        "a_symbol_era_id": a["symbol_era_id"],
        "a_last_day": a["last_day"],
        "a_trade_rows": int(a["trade_rows"]),
        "b_symbol": b["symbol"],
        "b_symbol_era_id": b["symbol_era_id"],
        "b_first_day": b["first_day"],
        "b_trade_rows": int(b["trade_rows"]),
        "gap_days": gap_days,
        **scores,
        "score": round(total, 4),
        "research_status": "candidate_needs_review",
    }


def mutual_best_pairs(pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep pairs where each era is the other's heaviest boundary neighbour."""
    best_succ: dict[str, dict[str, Any]] = {}
    best_pred: dict[str, dict[str, Any]] = {}
    for pair in pairs:
        succ = best_succ.get(pair["a_symbol_era_id"])
        if succ is None or _heavier(pair, succ, "b"):
            best_succ[pair["a_symbol_era_id"]] = pair
        pred = best_pred.get(pair["b_symbol_era_id"])
        if pred is None or _heavier(pair, pred, "a"):
            best_pred[pair["b_symbol_era_id"]] = pair
    return [
        pair
        for pair in best_succ.values()
        if best_pred.get(pair["b_symbol_era_id"], {}).get("a_symbol_era_id")
        == pair["a_symbol_era_id"]
    ]


def _heavier(candidate: dict[str, Any], current: dict[str, Any], side: str) -> bool:
    key = f"{side}_trade_rows"
    return (candidate[key], candidate["score"]) > (current[key], current["score"])


def volume_score(a_rows: Any, b_rows: Any) -> float:
    a_val, b_val = int(a_rows or 0), int(b_rows or 0)
    if a_val <= 0 or b_val <= 0:
        return ZERO_VOLUME_SCORE
    return round(min(a_val, b_val) / max(a_val, b_val), 4)


def issuer_score(a_name: Any, b_name: Any) -> float:
    a_tokens, b_tokens = normalize_issuer(a_name), normalize_issuer(b_name)
    if not a_tokens or not b_tokens:
        return NEUTRAL_SCORE
    if a_tokens == b_tokens:
        return 1.0
    overlap = len(a_tokens & b_tokens) / len(a_tokens | b_tokens)
    return 0.6 if overlap >= 0.5 else 0.0


def id_score(a_id: Any, b_id: Any) -> float:
    if a_id in (None, "") or b_id in (None, ""):
        return NEUTRAL_SCORE
    return 1.0 if str(a_id) == str(b_id) else 0.0


def normalize_issuer(name: Any) -> set[str]:
    if not name:
        return set()
    tokens = set(re.findall(r"[A-Z0-9]+", str(name).upper()))
    return tokens - ISSUER_STOPWORDS


def shift_day(day: str, days: int) -> str:
    return (datetime.strptime(day, DAY_FORMAT) + timedelta(days=days)).strftime(DAY_FORMAT)


def check_seed_recovery(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    ranks = {}
    for old, new in SEED_RENAMES.items():
        hit = next(
            (
                i
                for i, c in enumerate(candidates, 1)
                if c["a_symbol"] == old and c["b_symbol"] == new
            ),
            None,
        )
        ranks[f"{old}->{new}"] = {"recovered": hit is not None, "rank": hit}
    return ranks


def build_summary(
    config: CandidateConfig,
    eras: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    seed_recovery: dict[str, Any],
    raw_pair_count: int,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "eras_path": str(config.eras_path),
        "max_gap_days": config.max_gap_days,
        "era_count": len(eras),
        "raw_pair_count": raw_pair_count,
        "candidate_count": len(candidates),
        "seed_recovery": seed_recovery,
        "seed_recovered_count": sum(1 for r in seed_recovery.values() if r["recovered"]),
        "method": "review-only rename pairing: mutual-heaviest era boundary match ranked by min volume",
    }


def write_outputs(
    output_root: Path, candidates: list[dict[str, Any]], summary: dict[str, Any]
) -> None:
    (output_root / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_root / "candidates.jsonl").write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in candidates)
        + ("\n" if candidates else ""),
        encoding="utf-8",
    )
    if candidates:
        with (output_root / "candidates.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(candidates[0].keys()))
            writer.writeheader()
            writer.writerows(candidates)


if __name__ == "__main__":
    raise SystemExit(main())
