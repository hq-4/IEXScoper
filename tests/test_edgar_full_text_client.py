from pathlib import Path

from utils.edgar_full_text_client import search_param_variants
from utils.search_edgar_full_text_types import EdgarFullTextConfig


def test_search_param_variants_drop_brittle_filters_and_dates() -> None:
    target = {"symbol": "SQ", "first_day": "20161212", "last_day": "20250117"}

    variants = search_param_variants(_config(use_form_filter=True), target, "merger")

    labels = [label for label, params in variants]
    params = [params for label, params in variants]
    assert labels == ["primary", "without_forms", "without_dates", "ticker_in_query"]
    assert params[0]["forms"] == "8-K"
    assert "forms" not in params[1]
    assert "startdt" not in params[2]
    assert params[3] == {"q": '"SQ" merger'}


def test_search_param_variants_are_deduplicated_without_date_bounds() -> None:
    target = {"symbol": "SQ", "first_day": None, "last_day": None}

    variants = search_param_variants(_config(use_form_filter=False), target, "merger")

    assert variants == [
        ("primary", {"q": "merger", "entityName": "SQ"}),
        ("ticker_in_query", {"q": '"SQ" merger'}),
    ]


def _config(*, use_form_filter: bool) -> EdgarFullTextConfig:
    return EdgarFullTextConfig(
        template_path=Path("unused.csv"),
        output_root=Path("unused"),
        endpoint="https://efts.sec.gov/LATEST/search-index",
        symbols=("SQ",),
        user_agent="IEXScoper test admin@example.test",
        forms=("8-K",),
        use_form_filter=use_form_filter,
        event_terms=("merger",),
        size=5,
        max_symbols=None,
        timeout_seconds=2,
        sleep_seconds=0,
        retries=1,
    )
