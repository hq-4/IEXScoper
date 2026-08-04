from __future__ import annotations

from datetime import date

import pytest

from utils.probe_event_catalog_coverage import normalize_issuer_name
from utils.event_catalog_join import (
    combined_summary,
    match_eras_to_catalog,
    summarize_source,
)
from utils.event_catalog_sources import (
    normalize_ticker,
    parse_form25,
    parse_loose_date,
    parse_month_date,
    parse_nasdaq_delisted,
    parse_slash_date,
    parse_wiki_defunct_etfs,
    parse_yyyymmdd,
    ticker_from_display_names,
)

NASDAQ_FIXTURE = """Symbol|Security Name|Delisting Date
ZZZZ|Zzzz Test Corp Common Stock|12/31/2019
FLIO|Franklin Liberty International Opportunities ETF|06/17/2022
BADLINE
File Creation Time: 010120230101
"""

FORM25_FIXTURE = """
<DOCUMENT>
<TYPE>25
<TEXT>
<HTML><BODY>
<P>UNITED STATES SECURITIES AND EXCHANGE COMMISSION</P>
<P>FORM 25</P>
<TABLE>
<TR><TD>Commission File Number</TD><TD>000-50755</TD></TR>
<TR><TD>OptimumBank Holdings, Inc.</TD></TR>
<TR><TD>The NASDAQ Stock Market LLC</TD></TR>
<TR><TD>(Exact name of Issuer as specified in its charter, and name of Exchange where
security is listed and/or registered)</TD></TR>
</TABLE>
<P>Pursuant to 17 CFR 240.12d2-2(c), the Issuer has complied with the rules of the Exchange.
The delisting shall become effective on December 31, 2024.</P>
<P>This voluntary delisting from the Nasdaq Global Select Market under the trading symbol
&#8220;OPHC&#8221; is a result of the issuer's listing elsewhere.</P>
</BODY></HTML>
</TEXT>
</DOCUMENT>
"""

WIKI_FIXTURE = """
<html><body>
<table class="wikitable">
<tr><th>Ticker</th><th>Fund name</th><th>Issuer</th><th>Inception</th><th>Closure</th></tr>
<tr><td>FLIO</td><td>Franklin Liberty International Opportunities ETF</td>
<td>Franklin Templeton</td><td>June 8, 2016</td><td>June 17, 2022</td></tr>
<tr><td>HJEN</td><td>Direxion Hydrogen ETF</td><td>Direxion</td><td>2021-03-25</td>
<td>October 27, 2023</td></tr>
<tr><td>not a ticker row</td><td>broken</td></tr>
</table>
</body></html>
"""


def test_normalize_ticker() -> None:
    assert normalize_ticker("  oph c ") == "OPHC"
    assert normalize_ticker(None) == ""
    assert normalize_ticker("brk.b") == "BRK.B"


def test_date_parsers() -> None:
    assert parse_month_date("December 31, 2024") == date(2024, 12, 31)
    assert parse_slash_date("12/31/2019") == date(2019, 12, 31)
    assert parse_loose_date("2021-03-25") == date(2021, 3, 25)
    assert parse_loose_date("June 8, 2016") == date(2016, 6, 8)
    assert parse_yyyymmdd("20241231") == date(2024, 12, 31)
    assert parse_yyyymmdd("2024-12-31") is None
    assert parse_month_date("not a date") is None


def test_parse_nasdaq_delisted_fixture() -> None:
    rows, failures = parse_nasdaq_delisted(NASDAQ_FIXTURE, "nasdaq_delisted")
    assert failures == 1
    assert len(rows) == 2
    assert rows[0] == {
        "ticker": "ZZZZ",
        "name": "Zzzz Test Corp Common Stock",
        "issuer": "Zzzz Test Corp Common Stock",
        "event_date": "2019-12-31",
        "inception_date": None,
        "source": "nasdaq_delisted",
    }
    assert rows[1]["ticker"] == "FLIO"
    assert rows[1]["event_date"] == "2022-06-17"


def test_parse_form25_fixture() -> None:
    parsed = parse_form25(FORM25_FIXTURE)
    assert parsed["issuer"] == "OptimumBank Holdings, Inc."
    assert parsed["doc_ticker"] == "OPHC"
    assert parsed["effective_date"] == "2024-12-31"


def test_parse_form25_missing_fields_is_tolerant() -> None:
    parsed = parse_form25("<html><body><p>nothing useful here</p></body></html>")
    assert parsed == {
        "issuer": None,
        "security_name": None,
        "doc_ticker": None,
        "effective_date": None,
    }


def test_ticker_from_display_names() -> None:
    names = ["OptimumBank Holdings, Inc.  (OPHC)  (CIK 0001288855)"]
    assert ticker_from_display_names(names) == "OPHC"
    assert ticker_from_display_names(["No Ticker Here (CIK 1)"]) is None
    assert ticker_from_display_names([]) is None


def test_parse_wiki_fixture() -> None:
    rows, failures = parse_wiki_defunct_etfs(WIKI_FIXTURE)
    assert failures == 1
    assert len(rows) == 2
    flio = rows[0]
    assert flio["ticker"] == "FLIO"
    assert flio["issuer"] == "Franklin Templeton"
    assert flio["event_date"] == "2022-06-17"
    assert flio["inception_date"] == "2016-06-08"
    assert rows[1]["event_date"] == "2023-10-27"
    assert rows[1]["inception_date"] == "2021-03-25"


def _era(symbol: str, era_id: str, first: str, last: str, cls: str = "equity_common") -> dict:
    return {
        "symbol": symbol,
        "symbol_era_id": era_id,
        "first_day": first,
        "last_day": last,
        "gap_status": "delisted_or_acquired_candidate",
        "openfigi_class": cls,
    }


def test_match_eras_delist_and_inception_windows() -> None:
    catalog = [
        {
            "ticker": "ZZZZ",
            "name": "Z",
            "issuer": "Z Corp",
            "event_date": "2020-01-10",
            "inception_date": None,
            "source": "nasdaq_delisted",
        },
        {
            "ticker": "FLIO",
            "name": "F",
            "issuer": "F Issuer",
            "event_date": "2022-06-17",
            "inception_date": "2016-06-08",
            "source": "wiki_defunct_etf",
        },
    ]
    eras = [
        _era("ZZZZ", "ZZZZ#001", "20170101", "20200101"),  # 9d from delist -> hit
        _era("ZZZZ", "ZZZZ#002", "20210101", "20220101"),  # too far -> miss
        _era("FLIO", "FLIO#001", "20160601", "20220620", "fund_etf"),  # both windows
        _era("NOPE", "NOPE#001", "20170101", "20180101"),
    ]
    matches = match_eras_to_catalog(eras, catalog)
    by_era = {}
    for match in matches:
        by_era.setdefault(match["symbol_era_id"], set()).add(match["match_basis"])
    assert by_era["ZZZZ#001"] == {"delist_window"}
    assert "ZZZZ#002" not in by_era
    assert by_era["FLIO#001"] == {"delist_window", "inception_window"}
    assert "NOPE#001" not in by_era


def test_match_window_boundary_45_days() -> None:
    catalog = [
        {
            "ticker": "AAAA",
            "name": "A",
            "issuer": "A",
            "event_date": "2020-02-15",
            "inception_date": None,
            "source": "sec_form25",
        }
    ]
    in_window = _era("AAAA", "AAAA#001", "20190101", "20200101")  # 45 days exactly
    out_window = _era("AAAA", "AAAA#002", "20190101", "20191231")  # 46 days
    matches = match_eras_to_catalog([in_window, out_window], catalog)
    assert {m["symbol_era_id"] for m in matches} == {"AAAA#001"}


def test_summaries_track_fund_hit_rate() -> None:
    unresolved = [
        _era("ZZZZ", "ZZZZ#001", "20170101", "20200101"),
        _era("FLIO", "FLIO#001", "20160601", "20220620", "fund_etf"),
        _era("HJEN", "HJEN#001", "20210325", "20231027", "fund_etf"),
    ]
    catalog = [
        {
            "ticker": "FLIO",
            "name": "F",
            "issuer": "F",
            "event_date": "2022-06-17",
            "inception_date": None,
            "source": "wiki_defunct_etf",
        }
    ]
    matches = match_eras_to_catalog(unresolved, catalog)
    summary = summarize_source("wiki_defunct_etf", "ok", catalog, 0, matches, unresolved)
    assert summary["catalog_size"] == 1
    assert summary["eras_matched"] == 1
    assert summary["fund_etf_hit_rate"] == 0.5
    assert summary["by_openfigi_class"]["fund_etf"] == {"total": 2, "matched": 1}

    combined = combined_summary(matches, unresolved)
    assert combined["unresolved_eras"] == 3
    assert combined["eras_matched_unique"] == 1
    assert combined["coverage_share"] == pytest.approx(1 / 3, abs=1e-3)
    assert combined["by_source"] == {"wiki_defunct_etf": 1}


FORM25_SAMELINE_FIXTURE = """
<TEXT><HTML><BODY>
<P>Commission File Number: 001-11255</P>
<P>Amerco NASDAQ GLOBAL SELECT MARKET (Exact name of Issuer as specified in its charter)</P>
<P>Common Stock (Description of class of securities)</P>
</BODY></HTML></TEXT>
"""


def test_parse_form25_sameline_marker_strips_exchange() -> None:
    parsed = parse_form25(FORM25_SAMELINE_FIXTURE)
    assert parsed["issuer"] == "Amerco"


def test_normalize_issuer_name() -> None:
    assert normalize_issuer_name("OptimumBank Holdings, Inc.") == "OPTIMUMBANK"
    assert normalize_issuer_name("Amerco") == "AMERCO"
    assert normalize_issuer_name(None) == ""


def test_form25_row_issuer_name_bind(tmp_path) -> None:
    from utils.event_catalog_fetch import _safe_name
    from utils.probe_event_catalog_coverage import form25_row

    hit = {
        "adsh": "0001-22-000124",
        "display_names": ["NASDAQ STOCK MARKET LLC"],
        "document_url": "https://www.sec.gov/Archives/edgar/data/1/x/form25.htm",
        "file_date": "2022-12-16",
        "form": "25",
    }
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / f"{_safe_name(hit)}.txt").write_text(FORM25_SAMELINE_FIXTURE)
    assert form25_row(hit, docs_dir) is None  # no display ticker, no name map
    row = form25_row(hit, docs_dir, {"AMERCO": "UHAL"})
    assert row is not None
    assert row["ticker"] == "UHAL"
    assert row["ticker_source"] == "issuer_name_bind"


FORM25_OLDFORMAT_FIXTURE = """
0001354457
NASDAQ Stock Market LLC
0001467831
ETF Managers Trust
001-35744
35 Beechwood Road
Summit NJ 07901
The Restaurant ETF
17 CFR 240.12d2-2(a)(2)
Tara Petta
2016-12-28
"""


def test_parse_form25_oldformat_security_name() -> None:
    parsed = parse_form25(FORM25_OLDFORMAT_FIXTURE)
    assert parsed["security_name"] == "The Restaurant ETF"


def test_normalize_issuer_name_article_reorder() -> None:
    assert normalize_issuer_name("RESTAURANT ETF/THE") == "RESTAURANT ETF"
    assert normalize_issuer_name("The Restaurant ETF") == "RESTAURANT ETF"


def test_subject_name_from_display() -> None:
    from utils.probe_event_catalog_coverage import subject_name_from_display

    names = ["ETF Managers Trust  (CIK 0001467831)", "NASDAQ Stock Market LLC  (CIK 0001354457)"]
    assert subject_name_from_display(names) == "ETF Managers Trust"
    assert subject_name_from_display(["PUMA BIOTECHNOLOGY, INC.  (PBYI)  (CIK 0001401667)"]) is None
