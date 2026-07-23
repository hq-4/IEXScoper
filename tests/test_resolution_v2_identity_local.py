from __future__ import annotations

import json

from utils.resolution_v2_identity import (
    FinraDailyListAdapter,
    IssuerSourceAdapter,
    NasdaqTraderAdapter,
    NyseDelistingAdapter,
    entity_selector_candidates,
    parse_inline_xbrl,
    verify_identity,
)
from utils.resolution_v2_local import (
    authoritative_instrument_decision,
    child_security_is_exact,
    group_derivative_children,
    propagate_identity_gap,
)


def test_inline_xbrl_extracts_exact_dei_identity_and_instrument_facts() -> None:
    text = """
    <ix:nonNumeric name="dei:TradingSymbol">ACME</ix:nonNumeric>
    <dei:EntityRegistrantName>Acme Corp</dei:EntityRegistrantName>
    <dei:EntityCentralIndexKey>0000000123</dei:EntityCentralIndexKey>
    <dei:SecurityTitle>Common Stock</dei:SecurityTitle>
    <dei:SecurityExchangeName>NYSE</dei:SecurityExchangeName>
    """
    facts = parse_inline_xbrl(text)
    assert facts["tickers"] == ["ACME"]
    assert facts["ciks"] == ["123"]
    assert facts["security_titles"] == ["Common Stock"]


def test_sec_entity_selector_is_discovery_only_and_exact_gate_rejects_collision() -> None:
    payload = {
        "hits": {
            "hits": [
                {"_source": {"display_names": ["Acme Corp (ACME) (CIK 123)"]}},
                {"_source": {"display_names": ["Other Corp (ACME) (CIK 456)"]}},
            ]
        }
    }
    discovered = entity_selector_candidates(payload, "ACME")
    assert len(discovered) == 2
    assert {row["verification_state"] for row in discovered} == {"discovery_only"}
    exact = [
        _official("Acme", "123", "finra_daily_list"),
        _official("Other", "456", "nyse_delisting"),
    ]
    assert verify_identity("ACME", "20240601", exact)["verification_state"] == "collision_hold"


def test_public_primary_adapters_parse_network_free_records() -> None:
    fixture = json.dumps(
        {
            "data": [
                {
                    "symbol": "ACME",
                    "company_name": "Acme Corp",
                    "security_title": "Common Stock",
                    "cik": "123",
                    "effective_date": "2024-01-01",
                    "end_date": "2024-12-31",
                }
            ]
        }
    )
    adapters = (FinraDailyListAdapter(), NasdaqTraderAdapter(), NyseDelistingAdapter())
    for adapter in adapters:
        row = adapter.parse(fixture)[0]
        assert verify_identity("ACME", "20240601", [row])["verification_state"] == "verified"


def test_issuer_adapter_rejects_syndication_host() -> None:
    payload = [
        {
            "symbol": "ACME",
            "issuer": "Acme",
            "security_title": "Common Stock",
            "effective_date": "2024-01-01",
        }
    ]
    adapter = IssuerSourceAdapter()
    assert adapter.parse(payload, url="https://acme.test/news", issuer_domains={"acme.test"})
    assert not adapter.parse(
        payload, url="https://syndication.test/news", issuer_domains={"acme.test"}
    )


def test_cross_era_identity_propagation_requires_same_entity_and_instrument() -> None:
    target = {
        "symbol": "ACME",
        "symbol_era_id": "ACME#002",
        "first_day": "20240201",
        "last_day": "20240228",
    }
    before = {
        "symbol_era_id": "ACME#001",
        "entity_id": "123",
        "instrument": "Common Stock",
        "issuer": "Acme",
        "valid_through": "20240115",
        "fact_id": "before",
    }
    after = {
        "symbol_era_id": "ACME#003",
        "entity_id": "123",
        "instrument": "Common Stock",
        "valid_from": "20240315",
        "fact_id": "after",
    }
    identity, observation = propagate_identity_gap(target, before, after)
    assert identity and identity["symbol_era_id"] == "ACME#002"
    assert observation and observation["gap_status"] == "feed_gap_same_security"
    assert propagate_identity_gap(target | {"candidate_ciks": ["999"]}, before, after) == (
        None,
        None,
    )
    assert propagate_identity_gap(target, before, after | {"entity_id": "456"}) == (None, None)


def test_derivative_grouping_requires_exact_child_security_evidence() -> None:
    rows = [
        {
            "symbol": "ACME-W",
            "symbol_era_id": "ACME-W#001",
            "instrument_type": "probable_warrant",
            "last_day": "20240105",
        },
        {
            "symbol": "ACME-U",
            "symbol_era_id": "ACME-U#001",
            "instrument_type": "probable_unit",
            "last_day": "20240106",
        },
    ]
    groups = group_derivative_children(rows, {"ACME": {"entity_id": "123"}})
    assert len(groups) == 1 and len(groups[0]["children"]) == 2
    warrant = rows[0] | {"security_title": "Warrant"}
    assert child_security_is_exact(warrant, "ACME-W warrants were redeemed")
    assert not child_security_is_exact(warrant, "ACME common stock was redeemed")
    decision = authoritative_instrument_decision("Series A Preferred Stock")
    assert decision["research_status"] == "research_excluded_non_common"


def _official(issuer: str, cik: str, source: str) -> dict[str, str]:
    return {
        "symbol": "ACME",
        "issuer": issuer,
        "security_title": "Common Stock",
        "cik": cik,
        "effective_date": "2024-01-01",
        "end_date": "2024-12-31",
        "source": source,
    }
