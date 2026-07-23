from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from typing import Any

from utils.derivative_identity_resolution import exact_symbol, parent_root_symbol
from utils.resolution_v2_schema import prepare_fact
from utils.sec_identity_evidence import parse_day

NON_COMMON_TERMS = ("WARRANT", "UNIT", "RIGHT", "PREFERRED", "DEPOSITARY", "NOTE", "BOND")


def propagate_known_gaps(
    cohort: list[dict[str, Any]], identities: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    known = {
        row["symbol_era_id"]: row
        for row in identities
        if row.get("verification_state") == "verified"
    }
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in cohort:
        by_symbol[str(row.get("symbol") or "").upper()].append(row)
    new_identities, observations = [], []
    for eras in by_symbol.values():
        ordered = sorted(eras, key=lambda row: str(row.get("first_day") or ""))
        for before, target, after in zip(ordered, ordered[1:], ordered[2:], strict=False):
            if target["symbol_era_id"] in known:
                continue
            identity, observation = propagate_identity_gap(
                target,
                known.get(before["symbol_era_id"], {}),
                known.get(after["symbol_era_id"], {}),
            )
            if identity and observation:
                new_identities.append(identity)
                observations.append(observation)
    return new_identities, observations


def propagate_identity_gap(
    target: dict[str, Any], before: dict[str, Any], after: dict[str, Any]
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    same_entity = _entity(before) and _entity(before) == _entity(after)
    same_instrument = _instrument(before) and _instrument(before) == _instrument(after)
    conflict = bool(target.get("candidate_ciks") or target.get("conflicting_action"))
    chronological = _chronological(before, target, after)
    if not (same_entity and same_instrument and chronological) or conflict:
        return None, None
    identity = _propagated_identity(target, before, after)
    observation = _gap_observation(target, before, after, identity)
    return identity, observation


def _propagated_identity(
    target: dict[str, Any], before: dict[str, Any], after: dict[str, Any]
) -> dict[str, Any]:
    return prepare_fact(
        "identity",
        {
            "symbol": target["symbol"].upper(),
            "symbol_era_id": target["symbol_era_id"],
            "valid_from": target.get("first_day", ""),
            "valid_through": target.get("last_day", ""),
            "entity_id": _entity(before),
            "issuer": before.get("issuer", ""),
            "instrument": _instrument(before),
            "evidence_method": "cross_era_continuity",
            "source": f"{before.get('fact_id', '')}|{after.get('fact_id', '')}",
            "verification_state": "verified",
            "flags": ["same_entity_before_after", "same_instrument_before_after"],
        },
    )


def _gap_observation(
    target: dict[str, Any],
    before: dict[str, Any],
    after: dict[str, Any],
    identity: dict[str, Any],
) -> dict[str, Any]:
    return prepare_fact(
        "observation",
        {
            "symbol": target["symbol"].upper(),
            "symbol_era_id": target["symbol_era_id"],
            "boundary": "cross_era_gap",
            "gap_status": "feed_gap_same_security",
            "related_eras": [before["symbol_era_id"], after["symbol_era_id"]],
            "evidence": identity["source"],
            "verification_state": "verified",
        },
    )


def group_derivative_children(
    rows: list[dict[str, Any]], parent_identities: dict[str, dict[str, Any]], window_days: int = 14
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        root = parent_root_symbol(
            str(row.get("symbol") or ""), str(row.get("instrument_type") or "")
        )
        parent = parent_identities.get(root, {})
        cik, day = _entity(parent), parse_day(row.get("last_day"))
        if not root or not cik or not day:
            continue
        anchor = day - timedelta(days=day.toordinal() % max(window_days, 1))
        groups[(cik, anchor.isoformat())].append(row)
    return [
        {"parent_cik": key[0], "action_window": key[1], "children": values}
        for key, values in sorted(groups.items())
    ]


def child_security_is_exact(row: dict[str, Any], evidence_text: str) -> bool:
    symbol = str(row.get("symbol") or "").upper()
    title = str(row.get("security_title") or row.get("instrument_type") or "").upper()
    normalized = evidence_text.upper()
    return bool(
        exact_symbol(symbol, normalized) and any(term in normalized for term in _title_terms(title))
    )


def authoritative_instrument_decision(security_title: str) -> dict[str, str]:
    title = " ".join(str(security_title or "").upper().split())
    if not title:
        return {"instrument_status": "unresolved", "research_status": "action_required"}
    if any(term in title for term in NON_COMMON_TERMS):
        return {
            "instrument_status": "verified_non_common",
            "research_status": "research_excluded_non_common",
        }
    if "COMMON" in title or "ORDINARY" in title:
        return {"instrument_status": "verified_common", "research_status": "action_required"}
    return {"instrument_status": "unresolved", "research_status": "automation_exhausted"}


def _chronological(before: dict[str, Any], target: dict[str, Any], after: dict[str, Any]) -> bool:
    before_end = parse_day(before.get("valid_through") or before.get("last_day"))
    target_start = parse_day(target.get("first_day"))
    target_end = parse_day(target.get("last_day"))
    after_start = parse_day(after.get("valid_from") or after.get("first_day"))
    return bool(
        before_end
        and target_start
        and target_end
        and after_start
        and before_end < target_start <= target_end < after_start
    )


def _entity(row: dict[str, Any]) -> str:
    return str(row.get("entity_id") or row.get("identity_cik") or "").lstrip("0")


def _instrument(row: dict[str, Any]) -> str:
    return str(row.get("instrument") or row.get("security_title") or "").strip().upper()


def _title_terms(title: str) -> tuple[str, ...]:
    terms = tuple(term for term in NON_COMMON_TERMS if term in title)
    return terms or ("COMMON", "ORDINARY", "CLASS")
