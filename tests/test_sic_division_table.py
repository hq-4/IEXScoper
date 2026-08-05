from __future__ import annotations

import polars as pl
import pytest

from utils.sic_division_table import sic_division, sic_division_code_expr, sic_division_name_expr

# (sic, expected_division_code) at every division edge, including the unused gaps.
BOUNDARY_CASES = [
    (99, None),  # just below division A
    (100, "A"),
    (999, "A"),
    (1000, "B"),
    (1499, "B"),
    (1500, "C"),
    (1799, "C"),
    (1800, None),  # start of the C/D gap
    (1999, None),  # end of the C/D gap
    (2000, "D"),
    (3999, "D"),
    (4000, "E"),
    (4999, "E"),
    (5000, "F"),
    (5199, "F"),
    (5200, "G"),
    (5999, "G"),
    (6000, "H"),
    (6799, "H"),
    (6800, None),  # start of the H/I gap
    (6999, None),  # end of the H/I gap
    (7000, "I"),
    (8999, "I"),
    (9000, None),  # start of the I/J gap
    (9099, None),  # end of the I/J gap
    (9100, "J"),
    (9729, "J"),
    (9730, None),  # past division J
    (9995, None),  # SEC's "Non-Classifiable Establishment" extension
]


@pytest.mark.parametrize("sic,expected_code", BOUNDARY_CASES)
def test_sic_division_boundaries(sic: int, expected_code: str | None) -> None:
    code, name = sic_division(sic)
    assert code == expected_code
    assert (name is None) == (expected_code is None)


def test_sic_division_accepts_string_and_int_equivalently() -> None:
    assert sic_division("7372") == sic_division(7372)


@pytest.mark.parametrize("blank", [None, "", "  ", "N/A", "abc"])
def test_sic_division_handles_blank_and_non_numeric(blank) -> None:
    assert sic_division(blank) == (None, None)


def test_sic_division_exprs_match_scalar_lookup_vectorized() -> None:
    sics = [str(sic) for sic, _ in BOUNDARY_CASES] + [None, ""]
    frame = pl.DataFrame({"sic": sics}).with_columns(
        sic_division_code_expr().alias("code"),
        sic_division_name_expr().alias("name"),
    )
    for row in frame.iter_rows(named=True):
        expected_code, expected_name = sic_division(row["sic"])
        assert row["code"] == expected_code
        assert row["name"] == expected_name
