from __future__ import annotations

import polars as pl

from utils.instrument_classifier import (
    TYPE_FUND_OR_TRUST,
    TYPE_OPERATING_COMPANY,
    TYPE_PREFERRED,
    TYPE_RIGHT,
    TYPE_SHARE_CLASS,
    TYPE_UNKNOWN,
    TYPE_UNIT,
    TYPE_WARRANT,
)
from utils.instrument_research_routing import (
    ROUTE_FUND_OR_TRUST_CLOSURE,
    ROUTE_MANUAL_SYNTAX_REVIEW,
    ROUTE_OPERATING_COMPANY_SEC_EVENT,
    ROUTE_PREFERRED_REDEMPTION_OR_DELISTING,
    ROUTE_SECURITY_ACTION,
    ROUTE_SHARE_CLASS_CORPORATE_ACTION,
    recommended_evidence_expr,
    research_route_expr,
    route_for_instrument_type,
    routing_reason_expr,
)


def test_route_for_instrument_type_maps_expected_routes() -> None:
    expected = {
        TYPE_OPERATING_COMPANY: ROUTE_OPERATING_COMPANY_SEC_EVENT,
        TYPE_FUND_OR_TRUST: ROUTE_FUND_OR_TRUST_CLOSURE,
        TYPE_PREFERRED: ROUTE_PREFERRED_REDEMPTION_OR_DELISTING,
        TYPE_WARRANT: ROUTE_SECURITY_ACTION,
        TYPE_UNIT: ROUTE_SECURITY_ACTION,
        TYPE_RIGHT: ROUTE_SECURITY_ACTION,
        TYPE_SHARE_CLASS: ROUTE_SHARE_CLASS_CORPORATE_ACTION,
        TYPE_UNKNOWN: ROUTE_MANUAL_SYNTAX_REVIEW,
    }
    for instrument_type, research_route in expected.items():
        assert route_for_instrument_type(instrument_type).research_route == research_route


def test_routing_expressions_emit_route_evidence_and_reason() -> None:
    frame = pl.DataFrame(
        {
            "instrument_type": [
                TYPE_OPERATING_COMPANY,
                TYPE_FUND_OR_TRUST,
                TYPE_PREFERRED,
                TYPE_WARRANT,
                TYPE_SHARE_CLASS,
                TYPE_UNKNOWN,
            ]
        }
    )
    rows = frame.with_columns(
        research_route_expr().alias("research_route"),
        recommended_evidence_expr().alias("recommended_evidence"),
        routing_reason_expr().alias("routing_reason"),
    ).to_dicts()

    assert rows[0]["research_route"] == ROUTE_OPERATING_COMPANY_SEC_EVENT
    assert "8-K" in rows[0]["recommended_evidence"]
    assert rows[1]["research_route"] == ROUTE_FUND_OR_TRUST_CLOSURE
    assert rows[2]["research_route"] == ROUTE_PREFERRED_REDEMPTION_OR_DELISTING
    assert rows[3]["research_route"] == ROUTE_SECURITY_ACTION
    assert rows[4]["research_route"] == ROUTE_SHARE_CLASS_CORPORATE_ACTION
    assert rows[5]["research_route"] == ROUTE_MANUAL_SYNTAX_REVIEW
    assert rows[5]["routing_reason"] == f"instrument_type={TYPE_UNKNOWN}"
