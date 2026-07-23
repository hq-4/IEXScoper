from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from utils.resolution_v2_schema import FACT_FILES, fact_id, prepare_fact


class CanonicalFactStore:
    """Deterministic JSONL fact projections with atomic, duplicate-free replacement."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def load(self, kind: str) -> list[dict[str, Any]]:
        path = self.path(kind)
        if not path.exists():
            return []
        with path.open(encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]

    def merge(self, kind: str, records: Iterable[dict[str, Any]]) -> tuple[int, int]:
        existing = {item["fact_id"]: item for item in self.load(kind)}
        before = len(existing)
        for record in records:
            prepared = record if record.get("fact_id") else prepare_fact(kind, record)
            existing.setdefault(prepared["fact_id"], prepared)
        self._write(kind, sorted(existing.values(), key=_record_sort_key))
        return len(existing) - before, len(existing)

    def replace(self, kind: str, records: Iterable[dict[str, Any]]) -> int:
        unique: dict[str, dict[str, Any]] = {}
        for record in records:
            prepared = record if record.get("fact_id") else prepare_fact(kind, record)
            unique[prepared["fact_id"]] = prepared
        self._write(kind, sorted(unique.values(), key=_record_sort_key))
        return len(unique)

    def path(self, kind: str) -> Path:
        if kind not in FACT_FILES:
            raise ValueError(f"unknown canonical record kind: {kind}")
        return self.root / FACT_FILES[kind]

    def _write(self, kind: str, records: list[dict[str, Any]]) -> None:
        path = self.path(kind)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        temporary.replace(path)


def _record_sort_key(record: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(record.get("symbol_era_id") or ""),
        str(record.get("record_type") or ""),
        str(record.get("fact_id") or fact_id("unknown", record)),
    )
