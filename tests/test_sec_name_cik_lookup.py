from __future__ import annotations

import polars as pl
import pytest

from utils.sec_name_cik_lookup import (
    build_name_cik_index,
    match_by_name,
    normalize_name,
    require_columns,
    strip_security_descriptors,
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
        # SEC's trailing "/XX" state-of-incorporation disambiguation tag must not block
        # the legal-suffix strip loop from reaching "INC" behind it.
        ("Core Scientific, Inc./tx", "CORE SCIENTIFIC"),
        ("Some Corp/DE", "SOME"),
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


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("ABB LTD-SPON ADR", "ABB LTD"),
        ("ABEONA THERAPEUTICS INC-CW19", "ABEONA THERAPEUTICS INC"),
        ("ADEIA INC-W/I", "ADEIA INC"),
        ("ALITHYA GROUP INC-CLASS A", "ALITHYA GROUP INC"),
        ("ALKERMES PLC-WI", "ALKERMES PLC"),
        ("APEX TREASURY CORP-CL A", "APEX TREASURY CORP"),
        ("ATOUR LIFESTYLE HOLDINGS-ADR", "ATOUR LIFESTYLE HOLDINGS"),
        # The "- CL A" spacing variant (space before "CL") seen on real worklist rows.
        ("ROYALTY PHARMA PLC- CL A", "ROYALTY PHARMA PLC"),
        # A bare trailing "-A"/"-B" share-class letter with no "CL"/"CLASS" word.
        ("EVERPURE INC-A", "EVERPURE INC"),
        ("C3.AI INC-A", "C3.AI INC"),
        ("MOBILEYE GLOBAL INC-A", "MOBILEYE GLOBAL INC"),
        # A genuine two-letter trailing word must not be eaten by the bare-letter rule.
        ("SOME COMPANY-CO", "SOME COMPANY-CO"),
        # No descriptor suffix present -> unchanged.
        ("Agilent Technologies, Inc.", "Agilent Technologies, Inc."),
        (None, ""),
    ],
)
def test_strip_security_descriptors(raw: str | None, expected: str) -> None:
    assert strip_security_descriptors(raw) == expected


def test_match_by_name_falls_back_to_descriptor_stripped_name() -> None:
    """The exact case that motivated the fallback: OpenFIGI's `name` field carries a
    ticker-level descriptor suffix ("-SPON ADR") that isn't part of the real legal
    name and blocks a plain exact match, but the descriptor-stripped name matches
    SEC's current company list exactly."""
    sec_tickers = pl.DataFrame(
        {"sec_cik": ["0000313216"], "sec_name": ["ABB Ltd"], "sec_ticker": ["ABB"]}
    )
    index = build_name_cik_index(sec_tickers)
    era_identity = pl.DataFrame(
        {"symbol_era_id": ["ABB#001"], "identity_issuer": ["ABB LTD-SPON ADR"]}
    )

    matched = match_by_name(era_identity, index)

    assert matched["name_matched_cik"][0] == "313216"


def test_match_by_name_plain_match_wins_over_stripped() -> None:
    """When the plain name already matches exactly, the fallback pass shouldn't need
    to run at all — same result either way, but plain takes priority."""
    sec_tickers = pl.DataFrame(
        {"sec_cik": ["0000000001"], "sec_name": ["Example Co"], "sec_ticker": ["EX"]}
    )
    index = build_name_cik_index(sec_tickers)
    era_identity = pl.DataFrame({"symbol_era_id": ["EX#001"], "identity_issuer": ["Example Co"]})

    matched = match_by_name(era_identity, index)

    assert matched["name_matched_cik"][0] == "1"


def test_match_by_name_falls_back_to_prefix_match_on_truncated_name() -> None:
    """OpenFIGI truncates `name` to 28 characters — "ALPHA METALLURGICAL RESOURCE" for
    the real "Alpha Metallurgical Resources, Inc." — which blocks both exact passes but
    is an unambiguous word-boundary prefix of the real SEC name."""
    sec_tickers = pl.DataFrame(
        {
            "sec_cik": ["0001803599"],
            "sec_name": ["Alpha Metallurgical Resources, Inc."],
            "sec_ticker": ["AMR"],
        }
    )
    index = build_name_cik_index(sec_tickers)
    era_identity = pl.DataFrame(
        {
            "symbol_era_id": ["AMR#001"],
            "identity_issuer": ["ALPHA METALLURGICAL RESOURCE"],
        }
    )

    matched = match_by_name(era_identity, index)

    assert matched["name_matched_cik"][0] == "1803599"


def test_match_by_name_falls_back_to_prefix_match_on_abbreviation() -> None:
    """`HLDGS` isn't a legal suffix `normalize_name` strips, so "HERTZ GLOBAL HLDGS INC"
    only reduces to "HERTZ GLOBAL HLDGS" while the real SEC name "Hertz Global Holdings,
    Inc." reduces to "HERTZ GLOBAL" (HOLDINGS is stripped) — an unambiguous prefix."""
    sec_tickers = pl.DataFrame(
        {
            "sec_cik": ["0001657853"],
            "sec_name": ["Hertz Global Holdings, Inc."],
            "sec_ticker": ["HTZ"],
        }
    )
    index = build_name_cik_index(sec_tickers)
    era_identity = pl.DataFrame(
        {"symbol_era_id": ["HTZ#001"], "identity_issuer": ["HERTZ GLOBAL HLDGS INC"]}
    )

    matched = match_by_name(era_identity, index)

    assert matched["name_matched_cik"][0] == "1657853"


def test_match_by_name_prefix_match_requires_min_two_tokens() -> None:
    """The exact over-match risk that sank the rejected fuzzy matcher: a single generic
    word (sharing a first-token bucket with a real company's longer name) must not
    prefix-match on its own."""
    sec_tickers = pl.DataFrame(
        {
            "sec_cik": ["0000000001"],
            "sec_name": ["Bancorp Financial Services Inc"],
            "sec_ticker": ["BFS"],
        }
    )
    index = build_name_cik_index(sec_tickers)
    era_identity = pl.DataFrame({"symbol_era_id": ["X#001"], "identity_issuer": ["Bancorp"]})

    matched = match_by_name(era_identity, index)

    assert matched["name_matched_cik"][0] is None


def test_match_by_name_prefix_match_rejects_roman_numeral_sequel() -> None:
    """"XYZ Acquisition Corp II" and "...Corp III" are genuinely different SPACs, not a
    truncation of each other — even though "II" is a literal string prefix of "III".
    Real data surfaced this exact shape (Spartacus/Texas Ventures Acquisition)."""
    sec_tickers = pl.DataFrame(
        {
            "sec_cik": ["0000000001"],
            "sec_name": ["Spartacus Acquisition Corp. II"],
            "sec_ticker": ["SRAC"],
        }
    )
    index = build_name_cik_index(sec_tickers)
    era_identity = pl.DataFrame(
        {"symbol_era_id": ["SRAC#001"], "identity_issuer": ["SPARTACUS ACQUISITION CORP I"]}
    )

    matched = match_by_name(era_identity, index)

    assert matched["name_matched_cik"][0] is None


def test_match_by_name_prefix_match_ambiguous_across_two_ciks() -> None:
    """Two distinct real companies both prefix-matching the same query name is genuine
    ambiguity, not a match to guess between."""
    sec_tickers = pl.DataFrame(
        {
            "sec_cik": ["0000000001", "0000000002"],
            "sec_name": ["Example Global Holdings One Inc", "Example Global Holdings Two Inc"],
            "sec_ticker": ["EX1", "EX2"],
        }
    )
    index = build_name_cik_index(sec_tickers)
    era_identity = pl.DataFrame(
        {"symbol_era_id": ["EX#001"], "identity_issuer": ["Example Global Holdings"]}
    )

    matched = match_by_name(era_identity, index)

    assert matched["name_matched_cik"][0] is None


def test_require_columns_raises_on_missing() -> None:
    with pytest.raises(ValueError, match="missing required columns"):
        require_columns(
            pl.DataFrame({"symbol_era_id": ["A#001"]}), ("symbol_era_id", "identity_issuer")
        )
