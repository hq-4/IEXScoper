"""Derives an OpenFIGI input file for the `stable_candidate` and
`ipo_or_new_listing_candidate` eras — the ~11,244-era slice of the universe that never
went through OpenFIGI keyed enrichment, because that pass was scoped to
`data/resolution/observation_facts.jsonl` (the dead-ticker review cohort only) when it
was built. Output matches `utils.openfigi_identity_outputs.load_eras`'s expected JSONL
shape exactly, so `utils/build_openfigi_symbol_identities.py --input <this file>` runs
unmodified against it. [CA][CDiP]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DEFAULT_SYMBOL_ERAS_PATH = Path("reports/symbol-stability/symbol_eras.parquet")
DEFAULT_OUTPUT_PATH = Path("reports/openfigi-identity-stable/stable_universe_input.jsonl")
DEFAULT_CLASSES = ("stable_candidate", "ipo_or_new_listing_candidate")


def build_stable_universe_input(
    symbol_eras_path: Path, output_path: Path, classes: tuple[str, ...] = DEFAULT_CLASSES
) -> dict[str, int]:
    if not symbol_eras_path.exists():
        raise FileNotFoundError(f"symbol eras file does not exist: {symbol_eras_path}")
    eras = pl.read_parquet(symbol_eras_path).filter(
        pl.col("source_classification").is_in(list(classes))
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for row in eras.select("symbol", "symbol_era_id", "first_day", "last_day").iter_rows(
            named=True
        ):
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    return {
        "eras": eras.height,
        "unique_symbols": eras["symbol"].n_unique(),
        "classes": len(classes),
    }


def main() -> int:
    args = parse_args()
    result = build_stable_universe_input(
        Path(args.symbol_eras_path), Path(args.output_path), tuple(args.classes)
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Derive an OpenFIGI input file for eras outside the dead-ticker review cohort."
    )
    parser.add_argument("--symbol-eras-path", default=str(DEFAULT_SYMBOL_ERAS_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--classes", nargs="+", default=list(DEFAULT_CLASSES))
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
