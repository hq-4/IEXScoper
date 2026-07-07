from __future__ import annotations

import re
from dataclasses import dataclass

import polars as pl

FUND_PRODUCT_HINTS = ("etf", "etn", "fund_or_trust")

TYPE_FUND_OR_TRUST = "probable_fund_or_trust"
TYPE_WARRANT = "probable_warrant"
TYPE_UNIT = "probable_unit"
TYPE_RIGHT = "probable_right"
TYPE_PREFERRED = "probable_preferred"
TYPE_SHARE_CLASS = "probable_share_class"
TYPE_OPERATING_COMPANY = "probable_operating_company"
TYPE_UNKNOWN = "unknown_instrument"

HINT_PREFERRED = "possible_preferred"
HINT_OPERATING_OR_OTHER = "probable_operating_or_other"

REASON_IEX_PRODUCT_HINT = "iex_product_hint_fund_or_trust"
REASON_ISSUER_FUND_HINT = "issuer_name_fund_or_trust"
REASON_WARRANT_SUFFIX = "symbol_warrant_suffix"
REASON_UNIT_SUFFIX = "symbol_unit_suffix"
REASON_RIGHT_SUFFIX = "symbol_right_suffix"
REASON_PREFERRED_SUFFIX = "symbol_preferred_suffix"
REASON_DOT_SHARE_CLASS = "dot_share_class_suffix"
REASON_COMMON_SYMBOL = "common_stock_symbol"
REASON_AMBIGUOUS_SYMBOL = "ambiguous_symbol_pattern"

_FUND_ISSUER_PATTERN = r"(?i)\b(etf|etn|fund|trust|portfolio|index)\b"
_PREFERRED_SUFFIX_PATTERN = (
    r"(^[A-Z]{1,5}[- ]PR[A-Z]$)|"
    r"(^[A-Z]{1,5}\.PR[A-Z]$)|"
    r"(^[A-Z]{1,5}[- ][A-Z]$)|"
    r"(^[A-Z]{2,5}PR[A-Z]$)"
)
_DOT_SHARE_CLASS_PATTERN = r"^[A-Z]{1,5}\.[A-Z]$"
_COMMON_SYMBOL_PATTERN = r"^[A-Z]{1,5}$"


@dataclass(frozen=True)
class InstrumentClassification:
    instrument_type: str
    instrument_hint: str
    instrument_reason: str


def classify_instrument(
    symbol: str | None,
    iex_product_hint: str | None = None,
    iex_latest_issuer: str | None = None,
) -> InstrumentClassification:
    product_hint = (iex_product_hint or "").lower()
    normalized_symbol = _normalize_symbol(symbol)
    issuer = iex_latest_issuer or ""

    if product_hint in FUND_PRODUCT_HINTS:
        return _classification(TYPE_FUND_OR_TRUST, REASON_IEX_PRODUCT_HINT)
    if _is_warrant(normalized_symbol):
        return _classification(TYPE_WARRANT, REASON_WARRANT_SUFFIX)
    if _is_unit(normalized_symbol):
        return _classification(TYPE_UNIT, REASON_UNIT_SUFFIX)
    if _is_right(normalized_symbol):
        return _classification(TYPE_RIGHT, REASON_RIGHT_SUFFIX)
    if _is_preferred(normalized_symbol):
        return _classification(TYPE_PREFERRED, REASON_PREFERRED_SUFFIX)
    if _is_share_class(normalized_symbol):
        return _classification(TYPE_SHARE_CLASS, REASON_DOT_SHARE_CLASS)
    if _issuer_has_fund_hint(issuer):
        return _classification(TYPE_FUND_OR_TRUST, REASON_ISSUER_FUND_HINT)
    if _is_common_symbol(normalized_symbol):
        return _classification(TYPE_OPERATING_COMPANY, REASON_COMMON_SYMBOL)
    return _classification(TYPE_UNKNOWN, REASON_AMBIGUOUS_SYMBOL)


def instrument_type_expr() -> pl.Expr:
    return _instrument_expr("type")


def instrument_hint_expr() -> pl.Expr:
    return _instrument_expr("hint")


def instrument_reason_expr() -> pl.Expr:
    return _instrument_expr("reason")


def _instrument_expr(field: str) -> pl.Expr:
    conditions = _symbol_condition_exprs()
    values = _field_values(field)
    issuer_fund_value = values["issuer_fund"] if field == "reason" else values["fund"]

    return (
        pl.when(conditions["fund"])
        .then(pl.lit(values["fund"]))
        .when(conditions["warrant"])
        .then(pl.lit(values["warrant"]))
        .when(conditions["unit"])
        .then(pl.lit(values["unit"]))
        .when(conditions["right"])
        .then(pl.lit(values["right"]))
        .when(conditions["preferred"])
        .then(pl.lit(values["preferred"]))
        .when(conditions["share_class"])
        .then(pl.lit(values["share_class"]))
        .when(conditions["issuer_fund"])
        .then(pl.lit(issuer_fund_value))
        .when(conditions["common"])
        .then(pl.lit(values["operating"]))
        .otherwise(pl.lit(values["unknown"]))
    )


def _symbol_condition_exprs() -> dict[str, pl.Expr]:
    symbol = pl.col("symbol").fill_null("").str.to_uppercase()
    symbol_len = symbol.str.len_chars()
    return {
        "fund": pl.col("iex_product_hint").is_in(FUND_PRODUCT_HINTS),
        "issuer_fund": pl.col("iex_latest_issuer").fill_null("").str.contains(_FUND_ISSUER_PATTERN),
        "warrant": _warrant_expr(symbol, symbol_len),
        "unit": symbol.str.contains(r"(^|[-. ])U$")
        | ((symbol_len >= 5) & symbol.str.ends_with("U")),
        "right": symbol.str.ends_with("RT") | symbol.str.contains(r"(^|[-. ])RTS?$"),
        "preferred": symbol.str.contains(_PREFERRED_SUFFIX_PATTERN),
        "share_class": symbol.str.contains(_DOT_SHARE_CLASS_PATTERN),
        "common": symbol.str.contains(_COMMON_SYMBOL_PATTERN),
    }


def _warrant_expr(symbol: pl.Expr, symbol_len: pl.Expr) -> pl.Expr:
    return (
        symbol.str.ends_with("WS")
        | symbol.str.ends_with("WT")
        | symbol.str.ends_with("WTS")
        | symbol.str.contains(r"(^|[-. ])WTS?$")
        | symbol.str.contains(r"(^|[-. ])WARRANTS?$")
        | ((symbol_len >= 5) & symbol.str.ends_with("W"))
    )


def _field_values(field: str) -> dict[str, str]:
    if field == "type":
        return _type_values()
    if field == "hint":
        return _hint_values()
    return _reason_values()


def _type_values() -> dict[str, str]:
    return {
        "fund": TYPE_FUND_OR_TRUST,
        "warrant": TYPE_WARRANT,
        "unit": TYPE_UNIT,
        "right": TYPE_RIGHT,
        "preferred": TYPE_PREFERRED,
        "share_class": TYPE_SHARE_CLASS,
        "operating": TYPE_OPERATING_COMPANY,
        "unknown": TYPE_UNKNOWN,
    }


def _hint_values() -> dict[str, str]:
    return {
        "fund": TYPE_FUND_OR_TRUST,
        "warrant": TYPE_WARRANT,
        "unit": TYPE_UNIT,
        "right": TYPE_RIGHT,
        "preferred": HINT_PREFERRED,
        "share_class": HINT_OPERATING_OR_OTHER,
        "operating": HINT_OPERATING_OR_OTHER,
        "unknown": HINT_OPERATING_OR_OTHER,
    }


def _reason_values() -> dict[str, str]:
    return {
        "fund": REASON_IEX_PRODUCT_HINT,
        "issuer_fund": REASON_ISSUER_FUND_HINT,
        "warrant": REASON_WARRANT_SUFFIX,
        "unit": REASON_UNIT_SUFFIX,
        "right": REASON_RIGHT_SUFFIX,
        "preferred": REASON_PREFERRED_SUFFIX,
        "share_class": REASON_DOT_SHARE_CLASS,
        "operating": REASON_COMMON_SYMBOL,
        "unknown": REASON_AMBIGUOUS_SYMBOL,
    }


def _classification(instrument_type: str, reason: str) -> InstrumentClassification:
    return InstrumentClassification(
        instrument_type=instrument_type,
        instrument_hint=_legacy_hint(instrument_type),
        instrument_reason=reason,
    )


def _legacy_hint(instrument_type: str) -> str:
    if instrument_type == TYPE_PREFERRED:
        return HINT_PREFERRED
    if instrument_type in {TYPE_FUND_OR_TRUST, TYPE_WARRANT, TYPE_UNIT, TYPE_RIGHT}:
        return instrument_type
    return HINT_OPERATING_OR_OTHER


def _normalize_symbol(symbol: str | None) -> str:
    return (symbol or "").strip().upper()


def _is_warrant(symbol: str) -> bool:
    return (
        symbol.endswith(("WS", "WT", "WTS"))
        or symbol.endswith(("-W", ".W", " W", "-WS", ".WS", " WS", "-WT", ".WT", " WT"))
        or symbol.endswith(("WARRANT", "WARRANTS"))
        or (len(symbol) >= 5 and symbol.endswith("W"))
    )


def _is_unit(symbol: str) -> bool:
    return symbol.endswith(("-U", ".U", " U")) or (len(symbol) >= 5 and symbol.endswith("U"))


def _is_right(symbol: str) -> bool:
    return symbol.endswith(("RT", "-RT", ".RT", " RT", "-RTS", ".RTS", " RTS"))


def _is_preferred(symbol: str) -> bool:
    return bool(re.search(_PREFERRED_SUFFIX_PATTERN, symbol))


def _is_share_class(symbol: str) -> bool:
    return bool(re.search(_DOT_SHARE_CLASS_PATTERN, symbol))


def _is_common_symbol(symbol: str) -> bool:
    return bool(re.search(_COMMON_SYMBOL_PATTERN, symbol))


def _issuer_has_fund_hint(issuer: str) -> bool:
    return bool(re.search(_FUND_ISSUER_PATTERN, issuer))
