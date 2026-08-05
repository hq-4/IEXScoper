"""The standard SIC (Standard Industrial Classification) division table.

SIC assigns every registrant a 4-digit code (e.g. `7372` — "Services-Prepackaged
Software") but has no built-in coarse rollup; the 10-division structure below (A-J,
from the U.S. SIC Manual, the same structure SEC EDGAR's own SIC list follows) is the
standard way to derive a coarse "sector" from it. A handful of numeric ranges between
divisions are genuinely unused by the classification scheme (not typos here) — codes
that fall in a gap, or outside 0100-9729 entirely (including SEC-specific extensions
like `9995` "Non-Classifiable Establishment"), have no division and resolve to
`(None, None)` rather than being force-fit into a neighboring bucket. [CA][CDiP]
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass(frozen=True)
class SicDivision:
    code: str
    name: str
    low: int
    high: int


SIC_DIVISIONS: tuple[SicDivision, ...] = (
    SicDivision("A", "Agriculture, Forestry, And Fishing", 100, 999),
    SicDivision("B", "Mining", 1000, 1499),
    SicDivision("C", "Construction", 1500, 1799),
    # 1800-1999 unused
    SicDivision("D", "Manufacturing", 2000, 3999),
    SicDivision(
        "E", "Transportation, Communications, Electric, Gas, And Sanitary Services", 4000, 4999
    ),
    SicDivision("F", "Wholesale Trade", 5000, 5199),
    SicDivision("G", "Retail Trade", 5200, 5999),
    SicDivision("H", "Finance, Insurance, And Real Estate", 6000, 6799),
    # 6800-6999 unused
    SicDivision("I", "Services", 7000, 8999),
    # 9000-9099 unused
    SicDivision("J", "Public Administration", 9100, 9729),
)


def sic_division(sic: str | int | None) -> tuple[str | None, str | None]:
    """Scalar lookup: `sic_division("7372")` -> `("I", "Services")`.

    Blank, non-numeric, or out-of-range codes (including the unused gaps and SEC's own
    `9995` extension) return `(None, None)` rather than raising or guessing."""
    code = _to_int(sic)
    if code is None:
        return None, None
    for division in SIC_DIVISIONS:
        if division.low <= code <= division.high:
            return division.code, division.name
    return None, None


def sic_division_code_expr(sic_column: str = "sic") -> pl.Expr:
    return _division_when_chain(sic_column, lambda d: d.code)


def sic_division_name_expr(sic_column: str = "sic") -> pl.Expr:
    return _division_when_chain(sic_column, lambda d: d.name)


def _division_when_chain(sic_column: str, pick: Any) -> pl.Expr:
    numeric = pl.col(sic_column).cast(pl.Int64, strict=False)
    first, *rest = SIC_DIVISIONS
    expr = pl.when(numeric.is_between(first.low, first.high)).then(pl.lit(pick(first)))
    for division in rest:
        expr = expr.when(numeric.is_between(division.low, division.high)).then(
            pl.lit(pick(division))
        )
    return expr.otherwise(pl.lit(None, dtype=pl.String))


def _to_int(sic: str | int | None) -> int | None:
    if sic is None:
        return None
    text = str(sic).strip()
    if not text or not text.isdigit():
        return None
    return int(text)
