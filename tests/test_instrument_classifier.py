from __future__ import annotations

import polars as pl

from utils.instrument_classifier import (
    TYPE_FUND_OR_TRUST,
    TYPE_PREFERRED,
    TYPE_RIGHT,
    TYPE_SHARE_CLASS,
    TYPE_UNIT,
    TYPE_WARRANT,
    classify_instrument,
    instrument_type_expr,
)


def test_preferred_patterns() -> None:
    for symbol in ["WFC-X", "SLG-I", "AHL-D", "TNP-B", "BAC-PRA", "ABC-PRC", "BACPRC"]:
        assert classify_instrument(symbol).instrument_type == TYPE_PREFERRED


def test_single_letter_base_pr_strings_are_not_preferred() -> None:
    for symbol in ["RPRX", "APRN", "TPRE", "FPRX", "SPRB"]:
        assert classify_instrument(symbol).instrument_type != TYPE_PREFERRED


def test_share_class_patterns_are_not_preferred() -> None:
    for symbol in ["RDS.A", "LGF.A", "BRK.B"]:
        assert classify_instrument(symbol).instrument_type == TYPE_SHARE_CLASS


def test_warrant_unit_and_right_patterns() -> None:
    expected = {
        "AACIW": TYPE_WARRANT,
        "AACIU": TYPE_UNIT,
        "XYZWS": TYPE_WARRANT,
        "XYZWT": TYPE_WARRANT,
        "ABC RT": TYPE_RIGHT,
    }
    for symbol, instrument_type in expected.items():
        assert classify_instrument(symbol).instrument_type == instrument_type


def test_iex_product_hint_precedence() -> None:
    classification = classify_instrument("AACIW", iex_product_hint="etf")
    assert classification.instrument_type == TYPE_FUND_OR_TRUST


def test_polars_expression_matches_scalar_precedence() -> None:
    frame = pl.DataFrame(
        {
            "symbol": ["AACIW", "WFC-X", "RDS.A", "ABC"],
            "iex_product_hint": ["etf", None, None, None],
            "iex_latest_issuer": [None, None, None, None],
        }
    )
    rows = frame.with_columns(instrument_type_expr().alias("instrument_type")).to_dicts()
    assert [row["instrument_type"] for row in rows] == [
        TYPE_FUND_OR_TRUST,
        TYPE_PREFERRED,
        TYPE_SHARE_CLASS,
        "probable_operating_company",
    ]
