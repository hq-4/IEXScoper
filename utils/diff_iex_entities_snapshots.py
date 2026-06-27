from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_ENTITIES_ROOT = Path("iex_entities")
DEFAULT_OUTPUT_ROOT = Path("reports/iex-entities-diff")
SNAPSHOT_SUFFIX = ".json"
FIELDS = ["Symbol", "Issuer", "date", "isEnabled", "lit"]


@dataclass(frozen=True)
class Snapshot:
    snapshot_date: str
    path: Path
    rows: dict[str, dict[str, str]]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--entities-root", default=str(DEFAULT_ENTITIES_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = diff_snapshots(Path(args.entities_root), Path(args.output_root))
    print(json.dumps(result["summary"], indent=2, sort_keys=True))
    return 0


def diff_snapshots(entities_root: Path, output_root: Path) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    snapshots, invalid = load_snapshots(entities_root)
    daily_changes, adds, removes, changes = build_diffs(snapshots)
    summary = build_summary(snapshots, invalid, daily_changes, adds, removes, changes)
    write_outputs(output_root, summary, snapshots, invalid, daily_changes, adds, removes, changes)
    return {"summary": summary}


def load_snapshots(root: Path) -> tuple[list[Snapshot], list[dict[str, str]]]:
    snapshots: list[Snapshot] = []
    invalid: list[dict[str, str]] = []
    for path in sorted(root.glob(f"*{SNAPSHOT_SUFFIX}")):
        snapshot_date = path.stem
        try:
            rows = load_snapshot(path)
        except (json.JSONDecodeError, ValueError) as exc:
            invalid.append({"snapshot_date": snapshot_date, "path": str(path), "error": str(exc)})
            continue
        snapshots.append(Snapshot(snapshot_date=snapshot_date, path=path, rows=rows))
    return snapshots, invalid


def load_snapshot(path: Path) -> dict[str, dict[str, str]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("snapshot is not a JSON list")
    rows: dict[str, dict[str, str]] = {}
    for item in raw:
        if not isinstance(item, dict) or "Symbol" not in item:
            raise ValueError("snapshot contains malformed entity rows")
        row = {field: str(item.get(field, "")) for field in FIELDS}
        rows[row["Symbol"]] = row
    if len(rows) < 1_000:
        raise ValueError(f"snapshot has too few entity rows: {len(rows)}")
    return rows


def build_diffs(
    snapshots: list[Snapshot],
) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    daily_changes: list[dict[str, Any]] = []
    adds: list[dict[str, str]] = []
    removes: list[dict[str, str]] = []
    changes: list[dict[str, str]] = []
    for previous, current in zip(snapshots, snapshots[1:], strict=False):
        prev_symbols = set(previous.rows)
        current_symbols = set(current.rows)
        added = sorted(current_symbols - prev_symbols)
        removed = sorted(prev_symbols - current_symbols)
        changed = changed_symbols(previous, current, prev_symbols & current_symbols)
        daily_changes.append(
            {
                "from_snapshot": previous.snapshot_date,
                "to_snapshot": current.snapshot_date,
                "from_count": len(previous.rows),
                "to_count": len(current.rows),
                "added_count": len(added),
                "removed_count": len(removed),
                "changed_count": len(changed),
                "net_count_change": len(current.rows) - len(previous.rows),
            }
        )
        adds.extend(event_rows(current, added, "added", previous.snapshot_date))
        removes.extend(event_rows(previous, removed, "removed", current.snapshot_date))
        changes.extend(change_rows(previous, current, changed))
    return daily_changes, adds, removes, changes


def changed_symbols(previous: Snapshot, current: Snapshot, symbols: set[str]) -> list[str]:
    return sorted(
        symbol
        for symbol in symbols
        if comparable(previous.rows[symbol]) != comparable(current.rows[symbol])
    )


def comparable(row: dict[str, str]) -> tuple[str, str, str]:
    return row["Issuer"], row["isEnabled"], row["lit"]


def event_rows(
    snapshot: Snapshot, symbols: list[str], event_type: str, paired_snapshot: str
) -> list[dict[str, str]]:
    return [
        {
            "event_type": event_type,
            "snapshot_date": snapshot.snapshot_date,
            "paired_snapshot": paired_snapshot,
            "symbol": symbol,
            "issuer": snapshot.rows[symbol]["Issuer"],
            "is_enabled": snapshot.rows[symbol]["isEnabled"],
            "lit": snapshot.rows[symbol]["lit"],
            "product_hint": product_hint(snapshot.rows[symbol]["Issuer"]),
        }
        for symbol in symbols
    ]


def change_rows(previous: Snapshot, current: Snapshot, symbols: list[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for symbol in symbols:
        old = previous.rows[symbol]
        new = current.rows[symbol]
        rows.append(
            {
                "from_snapshot": previous.snapshot_date,
                "to_snapshot": current.snapshot_date,
                "symbol": symbol,
                "old_issuer": old["Issuer"],
                "new_issuer": new["Issuer"],
                "old_is_enabled": old["isEnabled"],
                "new_is_enabled": new["isEnabled"],
                "old_lit": old["lit"],
                "new_lit": new["lit"],
                "change_fields": ",".join(changed_fields(old, new)),
                "product_hint": product_hint(new["Issuer"]),
            }
        )
    return rows


def changed_fields(old: dict[str, str], new: dict[str, str]) -> list[str]:
    return [field for field in ("Issuer", "isEnabled", "lit") if old[field] != new[field]]


def product_hint(issuer: str) -> str:
    text = issuer.upper()
    if " ETF" in text or text.endswith("ETF"):
        return "etf"
    if " ETN" in text or text.endswith("ETN"):
        return "etn"
    if any(token in text for token in (" FUND", " TRUST", " INCOME", " TREASURY")):
        return "fund_or_trust"
    if " ACQUISITION" in text or " SPAC" in text:
        return "spac"
    if " ADR" in text or text.endswith("-ADR"):
        return "adr"
    return "operating_or_other"


def build_summary(
    snapshots: list[Snapshot],
    invalid: list[dict[str, str]],
    daily_changes: list[dict[str, Any]],
    adds: list[dict[str, str]],
    removes: list[dict[str, str]],
    changes: list[dict[str, str]],
) -> dict[str, Any]:
    first = snapshots[0] if snapshots else None
    last = snapshots[-1] if snapshots else None
    first_symbols = set(first.rows) if first else set()
    last_symbols = set(last.rows) if last else set()
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "valid_snapshot_count": len(snapshots),
        "invalid_snapshot_count": len(invalid),
        "first_snapshot": first.snapshot_date if first else None,
        "last_snapshot": last.snapshot_date if last else None,
        "first_symbol_count": len(first_symbols),
        "last_symbol_count": len(last_symbols),
        "net_added_since_first": len(last_symbols - first_symbols),
        "net_removed_since_first": len(first_symbols - last_symbols),
        "gross_added_events": len(adds),
        "gross_removed_events": len(removes),
        "changed_events": len(changes),
        "largest_add_day": max(daily_changes, key=lambda row: row["added_count"], default=None),
        "largest_remove_day": max(daily_changes, key=lambda row: row["removed_count"], default=None),
        "add_product_hints": count_by(adds, "product_hint"),
        "remove_product_hints": count_by(removes, "product_hint"),
    }


def count_by(rows: list[dict[str, str]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row[key]] = counts.get(row[key], 0) + 1
    return dict(sorted(counts.items()))


def write_outputs(
    output_root: Path,
    summary: dict[str, Any],
    snapshots: list[Snapshot],
    invalid: list[dict[str, str]],
    daily_changes: list[dict[str, Any]],
    adds: list[dict[str, str]],
    removes: list[dict[str, str]],
    changes: list[dict[str, str]],
) -> None:
    (output_root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    write_csv(output_root / "snapshot_summary.csv", snapshot_rows(snapshots))
    write_csv(output_root / "invalid_snapshots.csv", invalid)
    write_csv(output_root / "daily_changes.csv", daily_changes)
    write_csv(output_root / "added_symbols.csv", adds)
    write_csv(output_root / "removed_symbols.csv", removes)
    write_csv(output_root / "changed_symbols.csv", changes)
    write_markdown(output_root / "report.md", summary, daily_changes, invalid, adds, removes)


def snapshot_rows(snapshots: list[Snapshot]) -> list[dict[str, Any]]:
    return [
        {"snapshot_date": snapshot.snapshot_date, "path": str(snapshot.path), "symbol_count": len(snapshot.rows)}
        for snapshot in snapshots
    ]


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(
    path: Path,
    summary: dict[str, Any],
    daily_changes: list[dict[str, Any]],
    invalid: list[dict[str, str]],
    adds: list[dict[str, str]],
    removes: list[dict[str, str]],
) -> None:
    lines = [
        "# IEX Entity Snapshot Diff",
        "",
        f"- Valid snapshots: `{summary['valid_snapshot_count']}`",
        f"- Invalid snapshots skipped: `{summary['invalid_snapshot_count']}`",
        f"- Window: `{summary['first_snapshot']}` to `{summary['last_snapshot']}`",
        f"- Symbol count: `{summary['first_symbol_count']}` -> `{summary['last_symbol_count']}`",
        f"- Net added since first: `{summary['net_added_since_first']}`",
        f"- Net removed since first: `{summary['net_removed_since_first']}`",
        f"- Gross add events: `{summary['gross_added_events']}`",
        f"- Gross remove events: `{summary['gross_removed_events']}`",
        f"- Issuer/status change events: `{summary['changed_events']}`",
        "",
        "## Invalid Snapshots",
        "",
    ]
    lines += [f"- `{row['snapshot_date']}`: {row['error']}" for row in invalid] or ["- None"]
    lines += ["", "## Largest Daily Changes", ""]
    for row in sorted(daily_changes, key=lambda item: item["added_count"] + item["removed_count"], reverse=True)[:10]:
        lines.append(
            f"- `{row['from_snapshot']}` -> `{row['to_snapshot']}`: "
            f"+{row['added_count']} / -{row['removed_count']} / changed `{row['changed_count']}`"
        )
    lines += ["", "## Recent Adds", ""]
    lines += [event_line(row) for row in adds[-25:]] or ["- None"]
    lines += ["", "## Recent Removes", ""]
    lines += [event_line(row) for row in removes[-25:]] or ["- None"]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def event_line(row: dict[str, str]) -> str:
    return f"- `{row['snapshot_date']}` `{row['symbol']}` {row['issuer']} (`{row['product_hint']}`)"


if __name__ == "__main__":
    raise SystemExit(main())
