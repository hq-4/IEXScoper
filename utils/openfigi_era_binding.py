from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import polars as pl

from utils.probe_event_catalog_coverage import normalize_issuer_name
from utils.resolution_v2_schema import fingerprint, prepare_fact

FIGI_SOURCE = "openfigi:/v3/mapping"
IDENTITY_METHOD = "openfigi_symbol_identity"
EVENT_TYPE = "delisting_form25"

IDENTITY_COLUMNS = ["symbol", "figi", "composite_figi", "name", "security_type2", "match_status"]


def load_resolved_era_ids(identity_facts: Path, ledger: Path) -> set[str]:
    resolved = {
        json.loads(line)["symbol_era_id"]
        for line in identity_facts.read_text().splitlines()
        if line.strip()
    }
    with ledger.open(newline="", encoding="utf-8") as handle:
        resolved.update(row[0] for row in csv.reader(handle) if row and row[0] != "symbol_era_id")
    return resolved


def build_identity_candidates(
    eras: pl.DataFrame, figi_map: pl.DataFrame, matches: pl.DataFrame, exclude: set[str]
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    figi_rows = _figi_lookup(figi_map)
    form25_names = _form25_names_by_era(matches)
    candidates: list[dict[str, Any]] = []
    stats = {"single_figi_staged": 0, "multi_figi_bound": 0, "multi_figi_held": 0}
    for era in eras.iter_rows(named=True):
        if era["symbol_era_id"] in exclude:
            continue
        entries = figi_rows.get(era["symbol"])
        if not entries:
            continue
        if era.get("match_status") == "single":
            candidates.append(
                _identity_fact(era, entries[0], ["openfigi_ticker_mapping", "single_figi"])
            )
            stats["single_figi_staged"] += 1
        elif era.get("match_status") == "multi":
            bound = _disambiguate(entries, form25_names.get(era["symbol_era_id"]))
            if bound:
                candidates.append(
                    _identity_fact(
                        era,
                        bound,
                        ["openfigi_ticker_mapping", "multi_figi", "form25_name_disambiguated"],
                    )
                )
                stats["multi_figi_bound"] += 1
            else:
                stats["multi_figi_held"] += 1
    return candidates, stats


def build_event_candidates(matches: pl.DataFrame, exclude: set[str]) -> list[dict[str, Any]]:
    candidates = []
    for row in matches.iter_rows(named=True):
        if row["symbol_era_id"] in exclude or row["source"] != "sec_form25":
            continue
        record = {
            "symbol": row["symbol"],
            "symbol_era_id": row["symbol_era_id"],
            "event_type": EVENT_TYPE,
            "event_date": row["catalog_event_date"],
            "date_basis": "form25_filing_or_extracted",
            "old_symbol": row["symbol"],
            "new_symbol": "",
            "filer_cik": "",
            "accession": "",
            "form": "25",
            "source": "sec_form25_catalog",
            "verification_state": "event_candidate",
            "flags": ["event_candidate", "form25_catalog"],
            "snippet": f"form25 catalog: {row.get('catalog_name') or ''} | {row.get('catalog_issuer') or ''}",
        }
        candidates.append(prepare_fact("event", record))
    return candidates


def write_stage(
    stage_root: Path,
    identity: list[dict[str, Any]],
    events: list[dict[str, Any]],
    summary: dict[str, Any],
) -> Path:
    stage_id = fingerprint([fact["fact_id"] for fact in [*identity, *events]])[:16]
    stage_dir = stage_root / stage_id
    stage_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl(stage_dir / "identity_facts.jsonl", identity)
    _write_jsonl(stage_dir / "event_facts.jsonl", events)
    manifest = {
        "resolver_version": "openfigi_era_binding_v1",
        "status": "complete",
        "stopping_reason": "openfigi_era_binding_dry_run",
        "identity_candidates": len(identity),
        "event_candidates": len(events),
        "summary": summary,
    }
    (stage_dir / "stage_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return stage_dir


def summarize(
    identity: list[dict[str, Any]],
    events: list[dict[str, Any]],
    stats: dict[str, int],
    unresolved_total: int,
) -> dict[str, Any]:
    covered = {fact["symbol_era_id"] for fact in identity}
    by_class: dict[str, int] = {}
    for fact in identity:
        key = str(fact.get("instrument") or "unknown")
        by_class[key] = by_class.get(key, 0) + 1
    return {
        "identity_candidates": len(identity),
        "event_candidates": len(events),
        "unique_eras_with_identity_candidate": len(covered),
        "unresolved_baseline_eras": unresolved_total,
        "candidate_coverage_share": round(len(covered) / unresolved_total, 4)
        if unresolved_total
        else 0.0,
        "by_instrument_class": dict(sorted(by_class.items())),
        **stats,
    }


def apply_corroboration(
    facts: list[dict[str, Any]], matches: pl.DataFrame, sec_enriched: pl.DataFrame | None
) -> dict[str, int]:
    form25_names = _form25_names_by_era(matches)
    sec_names = _sec_names_by_era(sec_enriched)
    counts = {
        "form25_agree": 0,
        "form25_conflict": 0,
        "sec_name_agree": 0,
        "sec_name_conflict": 0,
        "uncorroborated": 0,
    }
    for index, fact in enumerate(facts):
        label = _corroboration_label(fact, form25_names, sec_names)
        counts[label] += 1
        flags = [flag for flag in fact["flags"] if not flag.startswith("corroboration:")]
        if label.endswith("_conflict"):
            flags.append("contested")
        state = "corroborated" if label.endswith("_agree") else "openfigi_asserted"
        record = {
            k: v
            for k, v in fact.items()
            if k not in {"fact_id", "created_at", "record_type", "resolver_version"}
        }
        facts[index] = prepare_fact(
            "identity",
            {**record, "verification_state": state, "flags": [*flags, f"corroboration:{label}"]},
        )
    return counts


def _corroboration_label(
    fact: dict[str, Any], form25_names: dict[str, str], sec_names: dict[str, str]
) -> str:
    figi_name = normalize_issuer_name(fact.get("issuer"))
    form25_name = form25_names.get(fact["symbol_era_id"])
    if form25_name:
        return (
            "form25_agree" if normalize_issuer_name(form25_name) == figi_name else "form25_conflict"
        )
    sec_name = sec_names.get(fact["symbol_era_id"])
    if sec_name:
        return (
            "sec_name_agree"
            if normalize_issuer_name(sec_name) == figi_name
            else "sec_name_conflict"
        )
    return "uncorroborated"


def _sec_names_by_era(sec_enriched: pl.DataFrame | None) -> dict[str, str]:
    if sec_enriched is None or "sec_name" not in sec_enriched.columns:
        return {}
    names: dict[str, str] = {}
    for row in sec_enriched.select(["symbol_era_id", "sec_name"]).iter_rows(named=True):
        if row["sec_name"]:
            names[row["symbol_era_id"]] = str(row["sec_name"])
    return names


def _figi_lookup(figi_map: pl.DataFrame) -> dict[str, list[dict[str, Any]]]:
    lookup: dict[str, list[dict[str, Any]]] = {}
    for row in figi_map.select(IDENTITY_COLUMNS).iter_rows(named=True):
        if row["match_status"] == "unmatched":
            continue
        lookup.setdefault(row["symbol"], []).append(row)
    return lookup


def _form25_names_by_era(matches: pl.DataFrame) -> dict[str, str]:
    names: dict[str, str] = {}
    for row in matches.iter_rows(named=True):
        if row["source"] == "sec_form25" and row.get("catalog_name"):
            names.setdefault(row["symbol_era_id"], str(row["catalog_name"]))
    return names


def _disambiguate(entries: list[dict[str, Any]], form25_name: str | None) -> dict[str, Any] | None:
    if not form25_name:
        return None
    target = normalize_issuer_name(form25_name)
    hits = [entry for entry in entries if normalize_issuer_name(entry.get("name")) == target]
    return hits[0] if len(hits) == 1 else None


def _identity_fact(era: dict[str, Any], entry: dict[str, Any], flags: list[str]) -> dict[str, Any]:
    record = {
        "symbol": era["symbol"],
        "symbol_era_id": era["symbol_era_id"],
        "entity_id": entry["figi"],
        "issuer": entry.get("name"),
        "instrument": era.get("openfigi_class"),
        "evidence_method": IDENTITY_METHOD,
        "evidence_date": era.get("last_day"),
        "valid_from": era.get("first_day"),
        "valid_through": era.get("last_day"),
        "verification_state": "candidate",
        "flags": flags,
        "source": FIGI_SOURCE,
        "related_symbols": [era["symbol"]],
    }
    return prepare_fact("identity", record)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
