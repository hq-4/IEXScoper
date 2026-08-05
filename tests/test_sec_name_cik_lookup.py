from __future__ import annotations

import polars as pl
import pytest

from utils.sec_name_cik_lookup import (
    build_name_cik_index,
    match_by_name,
    normalize_name,
    require_columns,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("AGILENT TECHNOLOGIES, INC.", "AGILENT TECHNOLOGIES"),
        ("Alcoa Corp", "ALCOA"),
        # "III" isn't a legal suffix, so it blocks stripping "CORP" behind it — only
        # trailing tokens are stripped, never ones buried mid-name.
        ("Ares Acquisition Corp III", "ARES ACQUISITION CORP III"),
        ("Some Fund Trust", "SOME FUND"),
        ("XYZ Holdings Inc", "XYZ"),
        (None, ""),
        ("", ""),
        ("   ", ""),
    ],
)
def test_normalize_name(raw: str | None, expected: str) -> None:
    assert normalize_name(raw) == expected


def test_build_name_cik_index_dedupes_multi_ticker_rows() -> None:
    sec_tickers = pl.DataFrame(
        {
            "sec_cik": ["0002034334", "0002034334", "0002034334"],
            "sec_name": [
                "Artius II Acquisition Inc.",
                "Artius II Acquisition Inc.",
                "Artius II Acquisition Inc.",
            ],
            "sec_ticker": ["AACB", "AACBR", "AACBU"],
        }
    )
    index = build_name_cik_index(sec_tickers)
    assert index.height == 1
    assert index["cik"][0] == "2034334"


def test_build_name_cik_index_drops_ambiguous_names() -> None:
    sec_tickers = pl.DataFrame(
        {
            "sec_cik": ["0000000001", "0000000002"],
            "sec_name": ["Example Holdings Inc.", "Example Holdings LLC"],
            "sec_ticker": ["EX1", "EX2"],
        }
    )
    index = build_name_cik_index(sec_tickers)
    # Both normalize to "EXAMPLE HOLDINGS" -> "EXAMPLE" after suffix stripping; ambiguous, dropped.
    assert index.height == 0


def test_match_by_name_returns_unpadded_cik_on_exact_normalized_match() -> None:
    sec_tickers = pl.DataFrame(
        {
            "sec_cik": ["0001512673"],
            "sec_name": ["Block, Inc."],
            "sec_ticker": ["XYZ"],
        }
    )
    index = build_name_cik_index(sec_tickers)
    era_identity = pl.DataFrame(
        {"symbol_era_id": ["OLD#001", "NEW#001"], "identity_issuer": ["Block Inc", None]}
    )

    matched = match_by_name(era_identity, index)
    rows = {row["symbol_era_id"]: row["name_matched_cik"] for row in matched.iter_rows(named=True)}

    assert rows["OLD#001"] == "1512673"
    assert rows["NEW#001"] is None


def test_match_by_name_no_match_for_unknown_issuer() -> None:
    sec_tickers = pl.DataFrame(
        {"sec_cik": ["0001512673"], "sec_name": ["Block, Inc."], "sec_ticker": ["XYZ"]}
    )
    index = build_name_cik_index(sec_tickers)
    era_identity = pl.DataFrame(
        {"symbol_era_id": ["FOO#001"], "identity_issuer": ["Totally Different Company"]}
    )

    matched = match_by_name(era_identity, index)

    assert matched["name_matched_cik"][0] is None


def test_require_columns_raises_on_missing() -> None:
    with pytest.raises(ValueError, match="missing required columns"):
        require_columns(
            pl.DataFrame({"symbol_era_id": ["A#001"]}), ("symbol_era_id", "identity_issuer")
        )
