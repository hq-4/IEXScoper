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
from utils.diff_iex_entities_snapshots import Snapshot, load_snapshots, product_hint

DEFAULT_ENTITIES_ROOT = Path("iex_entities")
DEFAULT_SYMBOL_ERAS_PATH = Path("reports/symbol-stability/symbol_eras.parquet")
DEFAULT_STABLE_UNIVERSE_PATH = Path(
    "reports/stable-long-window-universe/stable_long_window_universe.parquet"
)
DEFAULT_OUTPUT_ROOT = Path("reports/iex-entity-enrichment")


@dataclass(frozen=True)
class EntityEnrichmentConfig:
    entities_root: Path
    symbol_eras_path: Path
    stable_universe_path: Path
    output_root: Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--entities-root", default=str(DEFAULT_ENTITIES_ROOT))
    parser.add_argument("--symbol-eras-path", default=str(DEFAULT_SYMBOL_ERAS_PATH))
    parser.add_argument("--stable-universe-path", default=str(DEFAULT_STABLE_UNIVERSE_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    config = EntityEnrichmentConfig(
        entities_root=Path(args.entities_root),
        symbol_eras_path=Path(args.symbol_eras_path),
        stable_universe_path=Path(args.stable_universe_path),
        output_root=Path(args.output_root),
    )
    setup_logging(str(config.output_root / "iex_entity_enrichment.jsonl"))
    result = build_iex_entity_enrichment(config)
    get_logger(__name__).info(
        "IEX entity enrichment complete",
        extra={"event": "iex_entity_enrichment_complete", "detail": result["summary"]},
    )
    return 0


def build_iex_entity_enrichment(config: EntityEnrichmentConfig) -> dict[str, Any]:
    validate_inputs(config)
    config.output_root.mkdir(parents=True, exist_ok=True)
    snapshots, invalid = load_snapshots(config.entities_root)
    if not snapshots:
        raise ValueError(f"no valid entity snapshots found under {config.entities_root}")
    snapshot_rows = snapshot_frame(snapshots)
    lifecycle = lifecycle_frame(snapshots)
    symbol_eras = pl.read_parquet(config.symbol_eras_path)
    stable_universe = pl.read_parquet(config.stable_universe_path)
    first_day = snapshots[0].snapshot_date.replace("-", "")
    last_day = snapshots[-1].snapshot_date.replace("-", "")
    enriched_eras = enrich_frame(symbol_eras, lifecycle, first_day, last_day)
    enriched_stable = enrich_frame(stable_universe, lifecycle, first_day, last_day)
    summary = build_summary(
        config, snapshots, invalid, snapshot_rows, lifecycle, enriched_eras, enriched_stable
    )
    write_outputs(config.output_root, summary, snapshot_rows, lifecycle, enriched_eras, enriched_stable)
    return {"summary": summary}


def validate_inputs(config: EntityEnrichmentConfig) -> None:
    for path, label in [
        (config.entities_root, "entities root"),
        (config.symbol_eras_path, "symbol eras parquet"),
        (config.stable_universe_path, "stable universe parquet"),
    ]:
        if not path.exists():
            raise FileNotFoundError(f"{label} does not exist: {path}")


def snapshot_frame(snapshots: list[Snapshot]) -> pl.DataFrame:
    rows = [
        {
            "snapshot_date": snapshot.snapshot_date,
            "symbol": symbol,
            "issuer": row["Issuer"],
            "is_enabled": row["isEnabled"],
            "lit": row["lit"],
            "product_hint": product_hint(row["Issuer"]),
        }
        for snapshot in snapshots
        for symbol, row in snapshot.rows.items()
    ]
    return pl.DataFrame(rows).sort(["snapshot_date", "symbol"])


def lifecycle_frame(snapshots: list[Snapshot]) -> pl.DataFrame:
    latest_snapshot = snapshots[-1].snapshot_date
    state: dict[str, dict[str, Any]] = {}
    variants: dict[str, set[str]] = {}
    for snapshot in snapshots:
        for symbol, row in snapshot.rows.items():
            issuer = row["Issuer"]
            item = state.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "iex_first_seen": snapshot.snapshot_date,
                    "iex_seen_days": 0,
                },
            )
            item["iex_last_seen"] = snapshot.snapshot_date
            item["iex_seen_days"] += 1
            item["iex_latest_issuer"] = issuer
            item["iex_latest_is_enabled"] = row["isEnabled"]
            item["iex_latest_lit"] = row["lit"]
            item["iex_product_hint"] = product_hint(issuer)
            variants.setdefault(symbol, set()).add(issuer)
    rows = []
    for symbol, item in state.items():
        issuer_variants = sorted(value for value in variants[symbol] if value)
        rows.append(
            {
                **item,
                "iex_issuer_variant_count": len(issuer_variants),
                "iex_issuer_variants": " | ".join(issuer_variants),
                "iex_latest_snapshot": latest_snapshot,
                "iex_seen_in_latest": item["iex_last_seen"] == latest_snapshot,
                "iex_removed_after_seen": item["iex_last_seen"] != latest_snapshot,
            }
        )
    return pl.DataFrame(rows).sort("symbol")


def enrich_frame(
    frame: pl.DataFrame, lifecycle: pl.DataFrame, first_snapshot_day: str, last_snapshot_day: str
) -> pl.DataFrame:
    require_columns(frame, ["symbol", "first_day", "last_day"])
    return (
        frame.join(lifecycle, on="symbol", how="left")
        .with_columns(
            confidence_expr(first_snapshot_day, last_snapshot_day).alias(
                "iex_entity_confidence"
            )
        )
        .sort([column for column in ("symbol", "symbol_era_id") if column in frame.columns])
    )


def require_columns(frame: pl.DataFrame, columns: list[str]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"input parquet missing required columns: {missing}")


def confidence_expr(first_snapshot_day: str, last_snapshot_day: str) -> pl.Expr:
    first_seen = pl.col("iex_first_seen").str.replace_all("-", "")
    last_seen = pl.col("iex_last_seen").str.replace_all("-", "")
    matched = first_seen.is_not_null()
    overlaps = (pl.col("last_day") >= first_snapshot_day) & (pl.col("first_day") <= last_snapshot_day)
    changed = pl.col("iex_issuer_variant_count").fill_null(0) > 1
    current = pl.col("iex_seen_in_latest").fill_null(False)
    removed = pl.col("iex_removed_after_seen").fill_null(False)
    return (
        pl.when(~matched)
        .then(pl.lit("iex_snapshot_unmatched"))
        .when(changed & overlaps)
        .then(pl.lit("iex_snapshot_changed_during_window"))
        .when(removed | (last_seen < pl.lit(last_snapshot_day)))
        .then(pl.lit("iex_snapshot_removed_before_latest"))
        .when(overlaps)
        .then(pl.lit("iex_snapshot_overlap"))
        .when((pl.col("last_day") < first_snapshot_day) & current)
        .then(pl.lit("iex_current_symbol_only"))
        .when((pl.col("first_day") > last_snapshot_day) & matched)
        .then(pl.lit("iex_seen_before_era"))
        .otherwise(pl.lit("iex_snapshot_matched"))
    )


def build_summary(
    config: EntityEnrichmentConfig,
    snapshots: list[Snapshot],
    invalid: list[dict[str, str]],
    snapshot_rows: pl.DataFrame,
    lifecycle: pl.DataFrame,
    enriched_eras: pl.DataFrame,
    enriched_stable: pl.DataFrame,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "entities_root": str(config.entities_root),
        "symbol_eras_path": str(config.symbol_eras_path),
        "stable_universe_path": str(config.stable_universe_path),
        "first_snapshot": snapshots[0].snapshot_date,
        "last_snapshot": snapshots[-1].snapshot_date,
        "valid_snapshot_count": len(snapshots),
        "invalid_snapshot_count": len(invalid),
        "snapshot_row_count": snapshot_rows.height,
        "entity_symbol_count": lifecycle.height,
        "symbol_era_count": enriched_eras.height,
        "stable_universe_count": enriched_stable.height,
        "symbol_era_confidence_counts": count_by(enriched_eras, "iex_entity_confidence"),
        "stable_universe_confidence_counts": count_by(enriched_stable, "iex_entity_confidence"),
        "limitations": [
            "IEX entity snapshots are only available for the local snapshot window.",
            "This enrichment is current/listing evidence, not historical CUSIP/CIK identity proof.",
        ],
    }


def count_by(frame: pl.DataFrame, column: str) -> dict[str, int]:
    return {
        row[column]: row["len"]
        for row in frame.group_by(column).len().sort(column).to_dicts()
    }


def write_outputs(
    root: Path,
    summary: dict[str, Any],
    snapshot_rows: pl.DataFrame,
    lifecycle: pl.DataFrame,
    enriched_eras: pl.DataFrame,
    enriched_stable: pl.DataFrame,
) -> None:
    snapshot_rows.write_parquet(root / "iex_entity_snapshots.parquet", compression="zstd")
    lifecycle.write_parquet(root / "iex_entity_lifecycle.parquet", compression="zstd")
    lifecycle.write_csv(root / "iex_entity_lifecycle.csv")
    enriched_eras.write_parquet(root / "symbol_eras_iex_enriched.parquet", compression="zstd")
    enriched_eras.write_csv(root / "symbol_eras_iex_enriched.csv")
    enriched_stable.write_parquet(
        root / "stable_long_window_universe_iex_enriched.parquet", compression="zstd"
    )
    enriched_stable.write_csv(root / "stable_long_window_universe_iex_enriched.csv")
    (root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    write_markdown(root / "report.md", summary)


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# IEX Entity Enrichment",
        "",
        "This report joins locally captured IEX entity snapshots to ticker-era analysis outputs.",
        "It is an annotation layer, not a historical security master.",
        "",
        f"- Snapshot window: `{summary['first_snapshot']}` to `{summary['last_snapshot']}`",
        f"- Valid snapshots: `{summary['valid_snapshot_count']}`",
        f"- Invalid snapshots skipped: `{summary['invalid_snapshot_count']}`",
        f"- Snapshot rows: `{summary['snapshot_row_count']}`",
        f"- Entity symbols observed: `{summary['entity_symbol_count']}`",
        f"- Symbol eras enriched: `{summary['symbol_era_count']}`",
        f"- Stable universe rows enriched: `{summary['stable_universe_count']}`",
        "",
        "## Symbol-Era Confidence",
        "",
    ]
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["symbol_era_confidence_counts"].items())
    lines.extend(["", "## Stable Universe Confidence", ""])
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["stable_universe_confidence_counts"].items())
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in summary["limitations"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
