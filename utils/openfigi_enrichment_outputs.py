from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

AUDIT_COLUMNS = [
    "symbol",
    "classification",
    "first_day",
    "last_day",
    "coverage_ratio",
    "major_gap_count",
    "main_rows",
    "trade_rows",
]
OPENFIGI_COLUMNS = [
    "openfigi_status",
    "openfigi_match_count",
    "identity_risk",
    "figi",
    "compositeFIGI",
    "shareClassFIGI",
    "ticker",
    "name",
    "exchCode",
    "securityType",
    "securityType2",
    "marketSector",
    "uniqueID",
    "uniqueIDFutOpt",
    "openfigi_error",
]


def write_outputs(output_root: Path, summary: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    (output_root / "openfigi_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )
    (output_root / "openfigi_enriched_symbols.jsonl").write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + ("\n" if rows else ""),
        encoding="utf-8",
    )
    write_csv(output_root / "openfigi_enriched_symbols.csv", rows)
    write_markdown(output_root / "openfigi_enrichment_report.md", summary, rows)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [*AUDIT_COLUMNS, *OPENFIGI_COLUMNS]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def write_markdown(path: Path, summary: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    review_rows = [row for row in rows if row["identity_risk"] != "stable_candidate_with_match"][
        :25
    ]
    lines = [
        "# OpenFIGI Symbol Enrichment",
        "",
        "This enriches ticker-era continuity rows with OpenFIGI mapping metadata.",
        "It does not prove historical issuer identity.",
        "",
        f"- Input: `{summary['input_path']}`",
        f"- Symbols requested: `{summary['symbol_count']}`",
        f"- Exchange code: `{summary['exch_code']}`",
        f"- Market sector: `{summary['market_sector']}`",
        "",
        "## Match Status",
        "",
    ]
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["status_counts"].items())
    lines.extend(["", "## Identity Risk", ""])
    lines.extend(f"- `{key}`: `{value}`" for key, value in summary["identity_risk_counts"].items())
    lines.extend(
        ["", "## Review Sample", "", "| symbol | audit class | status | risk | figi | name |"]
    )
    lines.append("|---|---|---|---|---|---|")
    for row in review_rows:
        values = {key: row.get(key) or "" for key in [*AUDIT_COLUMNS, *OPENFIGI_COLUMNS]}
        lines.append(
            "| {symbol} | {classification} | {openfigi_status} | {identity_risk} | "
            "{figi} | {name} |".format(**values)
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
