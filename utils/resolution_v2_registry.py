from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from utils.resolution_v2_schema import RESOLVER_VERSION, canonical_json, fingerprint, utc_now

MAX_SNIPPETS = 8
MAX_SNIPPET_CHARS = 1_000
SCHEMA = """
CREATE TABLE IF NOT EXISTS requests (
  cache_key TEXT PRIMARY KEY, source TEXT NOT NULL, request_json TEXT NOT NULL,
  response_json TEXT NOT NULL, negative INTEGER NOT NULL, fetched_at TEXT NOT NULL,
  resolver_version TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS documents (
  document_url TEXT PRIMARY KEY, sha256 TEXT NOT NULL, metadata_json TEXT NOT NULL,
  snippets_json TEXT NOT NULL, recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
  symbol_era_id TEXT NOT NULL, resolver TEXT NOT NULL, resolver_version TEXT NOT NULL,
  evidence_fingerprint TEXT NOT NULL, status TEXT NOT NULL, missing_json TEXT NOT NULL,
  requests INTEGER NOT NULL, facts INTEGER NOT NULL, trade_rows INTEGER NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(symbol_era_id, resolver, resolver_version, evidence_fingerprint)
);
CREATE TABLE IF NOT EXISTS metrics (
  name TEXT PRIMARY KEY, value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS stages (
  stage_id TEXT PRIMARY KEY, cohort_sha256 TEXT NOT NULL, path TEXT NOT NULL,
  created_at TEXT NOT NULL, applied_at TEXT
);
"""


@dataclass(frozen=True)
class CachePolicy:
    max_age: timedelta | None = None
    immutable_negative: bool = False


class EvidenceRegistry:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.executescript(SCHEMA)

    def get(self, source: str, request: dict[str, Any], policy: CachePolicy) -> Any | None:
        key = _cache_key(source, request)
        row = self.connection.execute(
            "SELECT response_json, negative, fetched_at, resolver_version FROM requests "
            "WHERE cache_key = ?",
            (key,),
        ).fetchone()
        if not row or not _cache_valid(row, policy):
            return None
        self._increment("cache_hits")
        return json.loads(row[0])

    def put(self, source: str, request: dict[str, Any], response: Any, *, negative: bool) -> None:
        if source == "filing_document":
            raise ValueError("complete filing documents cannot be stored in the request cache")
        values = (
            _cache_key(source, request),
            source,
            canonical_json(request),
            canonical_json(response),
            int(negative),
            utc_now(),
            RESOLVER_VERSION,
        )
        self.connection.execute(
            "INSERT OR REPLACE INTO requests VALUES (?, ?, ?, ?, ?, ?, ?)", values
        )
        self.connection.commit()

    def record_document(
        self, url: str, text: str, metadata: dict[str, Any], snippets: Iterable[str]
    ) -> None:
        bounded = [_normalize_snippet(item) for item in snippets]
        bounded = [item for item in dict.fromkeys(bounded) if item][:MAX_SNIPPETS]
        values = (
            url,
            hashlib.sha256(text.encode("utf-8")).hexdigest(),
            canonical_json(metadata),
            canonical_json(bounded),
            utc_now(),
        )
        self.connection.execute("INSERT OR REPLACE INTO documents VALUES (?, ?, ?, ?, ?)", values)
        self.connection.commit()

    def metric(self, name: str) -> int:
        row = self.connection.execute(
            "SELECT value FROM metrics WHERE name = ?", (name,)
        ).fetchone()
        return int(row[0]) if row else 0

    def close(self) -> None:
        self.connection.close()

    def _increment(self, name: str) -> None:
        self.connection.execute(
            "INSERT INTO metrics(name, value) VALUES (?, 1) "
            "ON CONFLICT(name) DO UPDATE SET value = value + 1",
            (name,),
        )
        self.connection.commit()


class ResumeRegistry:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def completed(self, era: str, resolver: str, evidence: str) -> bool:
        row = self.connection.execute(
            "SELECT status FROM attempts WHERE symbol_era_id=? AND resolver=? "
            "AND resolver_version=? AND evidence_fingerprint=?",
            (era, resolver, RESOLVER_VERSION, evidence),
        ).fetchone()
        return bool(row and row[0] in {"completed", "automation_exhausted", "research_closed"})

    def record(
        self,
        era: str,
        resolver: str,
        evidence: str,
        result: dict[str, Any],
    ) -> None:
        values = (
            era,
            resolver,
            RESOLVER_VERSION,
            evidence,
            result["status"],
            canonical_json(sorted(result.get("missing_requirements", []))),
            int(result.get("requests", 0)),
            int(result.get("facts", 0)),
            int(result.get("trade_rows", 0)),
            utc_now(),
        )
        self.connection.execute(
            "INSERT OR REPLACE INTO attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values
        )
        self.connection.commit()


def _cache_key(source: str, request: dict[str, Any]) -> str:
    return fingerprint({"source": source, "request": request})


def _cache_valid(row: tuple[Any, ...], policy: CachePolicy) -> bool:
    _, negative, fetched_at, version = row
    if bool(negative) and policy.immutable_negative:
        return version == RESOLVER_VERSION
    if policy.max_age is None:
        return True
    fetched = datetime.fromisoformat(str(fetched_at))
    return datetime.now(timezone.utc) - fetched <= policy.max_age


def _normalize_snippet(value: str) -> str:
    return " ".join(str(value).split())[:MAX_SNIPPET_CHARS]
