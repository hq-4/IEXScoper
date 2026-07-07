from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from utils.search_edgar_full_text_types import EdgarFullTextConfig


def load_targets(config: EdgarFullTextConfig) -> list[dict[str, Any]]:
    aliases = load_aliases(config.alias_path)
    if config.symbols:
        targets = [symbol_target(symbol, aliases) for symbol in config.symbols]
    else:
        frame = pl.read_csv(config.template_path, infer_schema_length=0)
        required = ["symbol", "symbol_era_id", "first_day", "last_day", "priority_rank"]
        missing = [column for column in required if column not in frame.columns]
        if missing:
            raise ValueError(f"resolution template missing required columns: {missing}")
        targets = frame.select(required).to_dicts()
        for target in targets:
            target["edgar_aliases"] = aliases.get(str(target["symbol"]).upper(), ())
    return targets[: config.max_symbols] if config.max_symbols else targets


def symbol_target(symbol: str, aliases: dict[str, tuple[str, ...]]) -> dict[str, Any]:
    normalized = symbol.upper()
    return {
        "symbol": normalized,
        "first_day": None,
        "last_day": None,
        "edgar_aliases": aliases.get(normalized, ()),
    }


def load_aliases(path: Path) -> dict[str, tuple[str, ...]]:
    if not path.exists():
        return {}
    frame = pl.read_csv(path, infer_schema_length=0)
    missing = [column for column in ("symbol", "alias") if column not in frame.columns]
    if missing:
        raise ValueError(f"EDGAR alias file missing required columns: {missing}")
    aliases: dict[str, list[str]] = {}
    for row in frame.select(["symbol", "alias"]).to_dicts():
        symbol = str(row["symbol"]).upper().strip()
        alias = str(row["alias"]).strip()
        if symbol and alias:
            aliases.setdefault(symbol, []).append(alias)
    return {symbol: tuple(dict.fromkeys(values)) for symbol, values in aliases.items()}
