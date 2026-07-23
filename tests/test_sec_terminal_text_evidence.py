from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from utils.build_sec_terminal_text_evidence import (
    TerminalTextEvidenceConfig,
    build_sec_terminal_text_evidence,
)


def test_terminal_text_evidence_conservative_buckets(
    tmp_path: Path, monkeypatch: Any
) -> None:
    verifier_path = tmp_path / "verifier.csv"
    output_dir = tmp_path / "out"
    _write_verifier(verifier_path)

    def fake_fetch_text(url: str, config: Any) -> str:
        if url.endswith("err.htm"):
            raise RuntimeError("503 error")
        return _document_texts()[url.rsplit("/", maxsplit=1)[-1]]

    monkeypatch.setattr("utils.build_sec_terminal_text_evidence.fetch_text", fake_fetch_text)

    result = build_sec_terminal_text_evidence(
        TerminalTextEvidenceConfig(
            verifier_path=verifier_path,
            output_dir=output_dir,
            user_agent="IEXScoper test admin@example.test",
            timeout_seconds=2,
            sleep_seconds=0,
            max_rows=None,
            input_buckets=("strong_review_candidate", "moderate_review_candidate"),
        )
    )

    reviewed = {
        row["symbol"]: row
        for row in pl.read_csv(
            output_dir / "terminal_text_evidence_review.csv", infer_schema_length=0
        ).to_dicts()
    }
    auto = pl.read_csv(output_dir / "terminal_text_auto_verified.csv", infer_schema_length=0)
    assert result["summary"]["auto_ready_count"] == 2
    assert reviewed["AAA"]["terminal_text_bucket"] == "terminal_text_verified_ready"
    assert reviewed["FAR"]["terminal_text_bucket"] == "date_mismatch_review"
    assert reviewed["HOME"]["terminal_text_bucket"] == "reject_symbol_collision"
    assert reviewed["SEAS"]["terminal_text_bucket"] == "ambiguous_identity_review"
    assert reviewed["SFOUR"]["terminal_text_bucket"] == "terminal_text_verified_ready"
    assert reviewed["BADDATE"]["terminal_text_bucket"] == "missing_terminal_date_review"
    assert reviewed["EXPECT"]["terminal_text_bucket"] == "prospective_close_review"
    assert reviewed["ERR"]["terminal_text_bucket"] == "fetch_error"
    assert set(auto["symbol"].to_list()) == {"AAA", "SFOUR"}
    assert set(auto["research_status"].to_list()) == {"verified"}
    assert "COMPLETED THE MERGER" in reviewed["AAA"]["terminal_text_snippet"]


def test_terminal_text_evidence_uses_archive_fallback(
    tmp_path: Path, monkeypatch: Any
) -> None:
    verifier_path = tmp_path / "verifier.csv"
    output_dir = tmp_path / "out"
    _write_verifier(verifier_path, include_direct_url=False)

    def fake_resolve_document_url(archive_url: str, config: Any) -> str:
        assert archive_url == "https://www.sec.gov/archive/AAA/"
        return "https://www.sec.gov/archive/AAA/fallback.htm"

    monkeypatch.setattr(
        "utils.build_sec_terminal_text_evidence.resolve_document_url",
        fake_resolve_document_url,
    )
    monkeypatch.setattr(
        "utils.build_sec_terminal_text_evidence.fetch_text",
        lambda url, config: _document_texts()["aaa.htm"],
    )

    result = build_sec_terminal_text_evidence(
        TerminalTextEvidenceConfig(
            verifier_path=verifier_path,
            output_dir=output_dir,
            user_agent="IEXScoper test admin@example.test",
            timeout_seconds=2,
            sleep_seconds=0,
            max_rows=1,
            input_buckets=("strong_review_candidate",),
        )
    )

    row = result["rows"][0]
    assert row["terminal_text_bucket"] == "terminal_text_verified_ready"
    assert row["terminal_text_document_url"].endswith("/fallback.htm")


def test_terminal_text_evidence_writes_empty_outputs_for_unselected_buckets(
    tmp_path: Path,
) -> None:
    verifier_path = tmp_path / "verifier.csv"
    output_dir = tmp_path / "out"
    _write_verifier(verifier_path)

    result = build_sec_terminal_text_evidence(
        TerminalTextEvidenceConfig(
            verifier_path=verifier_path,
            output_dir=output_dir,
            user_agent="IEXScoper test admin@example.test",
            timeout_seconds=2,
            sleep_seconds=0,
            max_rows=None,
            input_buckets=("weak_review_candidate",),
        )
    )

    reviewed = pl.read_csv(output_dir / "terminal_text_evidence_review.csv")
    auto = pl.read_csv(output_dir / "terminal_text_auto_verified.csv")
    assert result["summary"]["row_count"] == 0
    assert reviewed.is_empty()
    assert auto.is_empty()
    assert "terminal_text_bucket" in reviewed.columns


def _write_verifier(path: Path, include_direct_url: bool = True) -> None:
    rows = _rows()
    if not include_direct_url:
        rows["verifier_document_url"][0] = ""
    pl.DataFrame(rows).write_csv(path)


def _rows() -> dict[str, list[str]]:
    symbols = ["AAA", "FAR", "HOME", "SEAS", "SFOUR", "BADDATE", "EXPECT", "ERR"]
    urls = [f"https://www.sec.gov/archive/{symbol}/{symbol.lower()}.htm" for symbol in symbols]
    return {
        "symbol": symbols,
        "symbol_era_id": [f"{symbol}#001" for symbol in symbols],
        "research_status": ["candidate_needs_review"] * len(symbols),
        "verifier_bucket": ["strong_review_candidate"] * len(symbols),
        "verifier_flags": ["issuer_name_match|event_language|completion_language"] * len(symbols),
        "form": ["8-K", "8-K", "8-K", "425", "S-4", "DEFM14A", "8-K", "8-K"],
        "original_last_day": [
            "20240105",
            "20240105",
            "20240105",
            "20240105",
            "20240202",
            "20240105",
            "20240105",
            "20240105",
        ],
        "last_day": [
            "20240105",
            "20240105",
            "20240105",
            "20240105",
            "20240202",
            "20240105",
            "20240105",
            "20240105",
        ],
        "entity": [
            "AAA CORP (AAA) (CIK 1)",
            "FAR CORP (FAR) (CIK 2)",
            "HOME BANCSHARES INC (HOMB) (CIK 3)",
            "TWELVE SEAS INVESTMENT COMPANY (CIK 4)",
            "SFOUR CORP (SFOUR) (CIK 5)",
            "BADDATE CORP (BADDATE) (CIK 6)",
            "EXPECT CORP (EXPECT) (CIK 8)",
            "ERR CORP (ERR) (CIK 7)",
        ],
        "proposed_historical_issuer_name": [
            "AAA Corp",
            "FAR Corp",
            "Home Bancshares Inc",
            "Twelve Seas Investment Company",
            "SFour Corp",
            "BadDate Corp",
            "Expect Corp",
            "Err Corp",
        ],
        "proposed_historical_event_type": ["merger_or_acquisition_lead"] * len(symbols),
        "proposed_historical_event_date": ["2024-01-05"] * len(symbols),
        "proposed_historical_successor": [""] * len(symbols),
        "primary_source_url": [f"https://www.sec.gov/archive/{symbol}/" for symbol in symbols],
        "secondary_source_url": [""] * len(symbols),
        "research_note": ["note"] * len(symbols),
        "verifier_document_url": urls,
    }


def _document_texts() -> dict[str, str]:
    return {
        "aaa.htm": "On January 5, 2024, AAA Corp ticker AAA completed the merger.",
        "far.htm": "On May 1, 2023, FAR Corp ticker FAR completed the merger.",
        "home.htm": "On January 5, 2024, Home Bancshares completed the merger.",
        "seas.htm": "On January 5, 2024, Twelve Seas completed the merger.",
        "sfour.htm": "On February 2, 2024, SFour Corp ticker SFOUR became effective.",
        "baddate.htm": "On January 99, 2024, BadDate Corp ticker BADDATE completed the merger.",
        "expect.htm": (
            "On January 5, 2024, Expect Corp ticker EXPECT announced the merger "
            "closing of the merger is expected to occur subject to the satisfaction "
            "of closing conditions."
        ),
        "err.htm": "",
    }
