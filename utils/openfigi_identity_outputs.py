from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl


def load_eras(path: Path) -> list[dict[str, str]]:
    eras: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            fact = json.loads(line)
            eras.append(
                {
                    "symbol": str(fact.get("symbol") or "").strip().upper(),
                    "symbol_era_id": str(fact.get("symbol_era_id") or ""),
                    "first_day": str(fact.get("first_day") or ""),
                    "last_day": str(fact.get("last_day") or ""),
                    "gap_status": str(fact.get("gap_status") or ""),
                }
            )
    return eras


def load_cache(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    cache: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        cache[item["symbol"]] = item["response"]
    return cache


def append_cache(path: Path, symbol: str, response: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"symbol": symbol, "response": response}, sort_keys=True) + "\n")


def write_outputs(
    output_root: Path,
    map_rows: list[dict[str, Any]],
    era_rows: list[dict[str, Any]],
    summary: dict[str, Any],
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(map_rows).write_parquet(output_root / "symbol_figi_map.parquet")
    pl.DataFrame(era_rows).write_parquet(output_root / "era_classes.parquet")
    (output_root / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
