from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from utils.instrument_classifier import (
    TYPE_FUND_OR_TRUST,
    TYPE_OPERATING_COMPANY,
    TYPE_PREFERRED,
    TYPE_RIGHT,
    TYPE_SHARE_CLASS,
    TYPE_UNIT,
    TYPE_UNKNOWN,
    TYPE_WARRANT,
)

ROUTE_OPERATING_COMPANY_SEC_EVENT = "operating_company_sec_event"
ROUTE_FUND_OR_TRUST_CLOSURE = "fund_or_trust_closure"
ROUTE_PREFERRED_REDEMPTION_OR_DELISTING = "preferred_redemption_or_delisting"
ROUTE_SECURITY_ACTION = "warrant_unit_right_security_action"
ROUTE_SHARE_CLASS_CORPORATE_ACTION = "share_class_corporate_action"
ROUTE_MANUAL_SYNTAX_REVIEW = "manual_syntax_review"

EVIDENCE_OPERATING_COMPANY = "8-K, merger proxy/S-4, 25-NSE, 15-12B, issuer/acquirer release"
EVIDENCE_FUND_OR_TRUST = "fund liquidation, closure, merger, trust termination, or sponsor notice"
EVIDENCE_PREFERRED = "preferred redemption, exchange, delisting, prospectus, or issuer notice"
EVIDENCE_SECURITY_ACTION = "warrant/unit/right redemption, separation, expiration, or SPAC action"
EVIDENCE_SHARE_CLASS = (
    "share-class rename, ADR/common share conversion, merger, or delisting notice"
)
EVIDENCE_MANUAL_SYNTAX = "manual symbol syntax review before choosing SEC or security-action path"


@dataclass(frozen=True)
class ResearchRoute:
    research_route: str
    recommended_evidence: str
    routing_reason: str


def route_for_instrument_type(instrument_type: str | None) -> ResearchRoute:
    key = instrument_type or TYPE_UNKNOWN
    route, evidence = _ROUTE_MAP.get(key, _ROUTE_MAP[TYPE_UNKNOWN])
    return ResearchRoute(
        research_route=route,
        recommended_evidence=evidence,
        routing_reason=f"instrument_type={key}",
    )


def research_route_expr() -> pl.Expr:
    return _route_field_expr("route")


def recommended_evidence_expr() -> pl.Expr:
    return _route_field_expr("evidence")


def routing_reason_expr() -> pl.Expr:
    return pl.concat_str(
        [pl.lit("instrument_type="), pl.col("instrument_type").fill_null(TYPE_UNKNOWN)]
    )


def _route_field_expr(field: str) -> pl.Expr:
    values = _field_values(field)
    expr = pl.when(pl.col("instrument_type") == TYPE_OPERATING_COMPANY).then(
        pl.lit(values[TYPE_OPERATING_COMPANY])
    )
    for instrument_type in _ORDERED_NON_OPERATING_TYPES:
        expr = expr.when(pl.col("instrument_type") == instrument_type).then(
            pl.lit(values[instrument_type])
        )
    return expr.otherwise(pl.lit(values[TYPE_UNKNOWN]))


def _field_values(field: str) -> dict[str, str]:
    index = 0 if field == "route" else 1
    return {instrument_type: route[index] for instrument_type, route in _ROUTE_MAP.items()}


_ROUTE_MAP = {
    TYPE_OPERATING_COMPANY: (ROUTE_OPERATING_COMPANY_SEC_EVENT, EVIDENCE_OPERATING_COMPANY),
    TYPE_FUND_OR_TRUST: (ROUTE_FUND_OR_TRUST_CLOSURE, EVIDENCE_FUND_OR_TRUST),
    TYPE_PREFERRED: (ROUTE_PREFERRED_REDEMPTION_OR_DELISTING, EVIDENCE_PREFERRED),
    TYPE_WARRANT: (ROUTE_SECURITY_ACTION, EVIDENCE_SECURITY_ACTION),
    TYPE_UNIT: (ROUTE_SECURITY_ACTION, EVIDENCE_SECURITY_ACTION),
    TYPE_RIGHT: (ROUTE_SECURITY_ACTION, EVIDENCE_SECURITY_ACTION),
    TYPE_SHARE_CLASS: (ROUTE_SHARE_CLASS_CORPORATE_ACTION, EVIDENCE_SHARE_CLASS),
    TYPE_UNKNOWN: (ROUTE_MANUAL_SYNTAX_REVIEW, EVIDENCE_MANUAL_SYNTAX),
}

_ORDERED_NON_OPERATING_TYPES = (
    TYPE_FUND_OR_TRUST,
    TYPE_PREFERRED,
    TYPE_WARRANT,
    TYPE_UNIT,
    TYPE_RIGHT,
    TYPE_SHARE_CLASS,
    TYPE_UNKNOWN,
)
