from __future__ import annotations

import json
from pathlib import Path

from utils.iex_benchmark_core import DEFAULT_DAYS, as_json


def append_result(path: Path, payload: dict) -> None:
    rows = []
    if path.exists():
        rows = json.loads(path.read_text(encoding="utf-8"))
    rows.append(as_json(payload))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def render_markdown_report(payload: dict) -> str:
    lines = [
        "# IEX Parser Benchmark Report",
        "",
        "## Scope",
        "",
        f"- Benchmark mode: `{payload['benchmark_mode']}`",
        f"- Days requested: `{', '.join(payload.get('days', DEFAULT_DAYS))}`",
        f"- Archive root: `{payload['archive_root']}`",
        f"- Output root: `{payload['output_root']}`",
        f"- Results file: `{payload['results_path']}`",
        "",
        "## Environment",
        "",
    ]
    for key, value in sorted(payload["environment"].items()):
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Reference Contract",
            "",
            f"- Main file: `{payload['reference']['main']['path']}`",
            f"- Quote file: `{payload['reference']['quote']['path']}`",
            f"- Main columns: `{len(payload['reference']['main']['columns'])}`",
            f"- Quote columns: `{len(payload['reference']['quote']['columns'])}`",
            f"- Main compression: `{', '.join(payload['reference']['main']['compression'])}`",
            f"- Quote compression: `{', '.join(payload['reference']['quote']['compression'])}`",
            "",
            "## Repo Commits",
            "",
        ]
    )
    for repo in payload["repos"]:
        lines.append(f"- `{repo['repo_key']}`: `{repo['commit']}` ({repo['path']})")
    lines.extend(
        [
            "",
            "## Parity Runs",
            "",
            "| Repo | Day | Status | Wall s | User s | Sys s | Max RSS KB | Main Rows | Quote Rows | Input Mode | Notes |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for run in payload["runs"]:
        resources = run.get("resources", {})
        lines.append(
            "| {repo} | {day} | {status} | {wall} | {user} | {sys} | {rss} | {main_rows} | {quote_rows} | {input_mode} | {notes} |".format(
                repo=run["repo_key"],
                day=run["day"],
                status=run["status"],
                wall=_fmt(resources.get("elapsed_wall_seconds")),
                user=_fmt(resources.get("user_cpu_seconds")),
                sys=_fmt(resources.get("system_cpu_seconds")),
                rss=_fmt(resources.get("max_rss_kb")),
                main_rows=_fmt(run.get("main_rows")),
                quote_rows=_fmt(run.get("quote_rows")),
                input_mode=run.get("preprocessing", {}).get("input_mode", "unknown"),
                notes=(run.get("error") or "").replace("|", "/"),
            )
        )
    lines.extend(
        [
            "",
            "## Compression Sweep",
            "",
            "| Repo | Codec | Status | Main Size | Quote Size | Main Row Groups | Quote Row Groups |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for sweep in payload["compression_sweeps"]:
        lines.append(
            "| {repo} | {codec} | {status} | {main_size} | {quote_size} | {main_rg} | {quote_rg} |".format(
                repo=sweep["repo_key"],
                codec=sweep["compression"],
                status=sweep["status"],
                main_size=_fmt(sweep.get("main_size_bytes")),
                quote_size=_fmt(sweep.get("quote_size_bytes")),
                main_rg=_fmt(sweep.get("main_row_groups")),
                quote_rg=_fmt(sweep.get("quote_row_groups")),
            )
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This report is generated from the structured benchmark results file.",
            "- Gaps should be read from the `message_counts` and error fields in the raw results.",
        ]
    )
    return "\n".join(lines) + "\n"


def _fmt(value: object) -> str:
    return "" if value is None else str(value)
