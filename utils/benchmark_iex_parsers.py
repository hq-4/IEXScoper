from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import shutil
import subprocess
import sys
import threading
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.iex_benchmark_core import (
    DEFAULT_DAYS,
    REFERENCE_MAIN_PATH,
    REFERENCE_QUOTE_PATH,
    REPO_SPECS,
    REPRESENTATIVE_SWEEP_DAY,
    benchmark_output_paths,
    build_environment_summary,
    ensure_repo_checkout,
    json_dump,
    parse_csv_list,
    repo_commit,
    resolve_archive_day,
    summarize_reference_schema,
    time_binary_path,
)
from utils.iex_benchmark_reporting import render_markdown_report

RESULTS_PATH = Path("utils/benchmark_results/iex_parser_benchmark_results.json")
LOGGER = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", default=",".join(DEFAULT_DAYS))
    parser.add_argument("--archive-root", default="/media/tn/iex")
    parser.add_argument("--output-root", default="/media/tn/pq/tmp")
    parser.add_argument("--local-archive-cache", default="/tmp/iex-benchmark-archive")
    parser.add_argument("--report-path", default="PLANS/iex_parser_benchmark_report.md")
    parser.add_argument("--repos", default="rob-blackbourn,hq-4")
    parser.add_argument("--max-parallel", type=int, default=2)
    parser.add_argument("--keep-intermediate", action="store_true")
    parser.add_argument(
        "--compression", default="snappy", choices=("snappy", "zstd1", "zstd3", "zstd5")
    )
    parser.add_argument("--benchmark-mode", default="parity", choices=("parity",))
    parser.add_argument("--repo-cache-root", default="/tmp/iex-benchmark-repos")
    args = parser.parse_args()

    days = parse_csv_list(args.days)
    repos = parse_csv_list(args.repos)
    archive_root = Path(args.archive_root)
    output_root = Path(args.output_root)
    local_archive_cache = Path(args.local_archive_cache) if args.local_archive_cache else None
    report_path = Path(args.report_path)
    repo_cache_root = Path(args.repo_cache_root)
    output_root.mkdir(parents=True, exist_ok=True)
    if args.max_parallel < 1:
        raise ValueError("--max-parallel must be at least 1")
    setup_logging(str(output_root / "benchmark_iex_parsers.jsonl"))
    logger = get_logger(__name__)
    effective_archive_root = stage_archive_days(archive_root, days, local_archive_cache)
    logger.info(
        "benchmark start",
        extra={
            "event": "iex_benchmark_start",
            "detail": {
                "days": days,
                "repos": repos,
                "archive_root": str(archive_root),
                "effective_archive_root": str(effective_archive_root),
                "local_archive_cache": str(local_archive_cache) if local_archive_cache else None,
                "output_root": str(output_root),
                "max_parallel": args.max_parallel,
            },
        },
    )

    repo_meta = []
    for repo_key in repos:
        if repo_key not in REPO_SPECS:
            raise ValueError(f"unsupported repo {repo_key}")
        repo_root = ensure_repo_checkout(repo_key, repo_cache_root)
        repo_meta.append(
            {
                "repo_key": repo_key,
                "path": str(repo_root),
                "commit": repo_commit(repo_root),
                "input_support": REPO_SPECS[repo_key]["input_support"],
            }
        )
        logger.info(
            "repo ready",
            extra={
                "event": "iex_benchmark_repo_ready",
                "detail": repo_meta[-1],
                "day": "",
            },
        )

    ordered_repo_meta = [repo_meta_map(repo_meta)[repo_key] for repo_key in repos]
    runs = execute_repo_day_series(
        ordered_repo_meta,
        days,
        effective_archive_root,
        output_root,
        args.compression,
        max_parallel=args.max_parallel,
    )
    sweeps = execute_repo_sweeps(
        ordered_repo_meta,
        effective_archive_root,
        output_root,
        max_parallel=args.max_parallel,
    )

    payload = {
        "benchmark_mode": args.benchmark_mode,
        "days": days,
        "archive_root": str(archive_root),
        "effective_archive_root": str(effective_archive_root),
        "local_archive_cache": str(local_archive_cache) if local_archive_cache else None,
        "output_root": str(output_root),
        "max_parallel": args.max_parallel,
        "results_path": str(RESULTS_PATH),
        "environment": build_environment_summary(),
        "reference": {
            "main": summarize_reference_schema(REFERENCE_MAIN_PATH).__dict__,
            "quote": summarize_reference_schema(REFERENCE_QUOTE_PATH).__dict__,
        },
        "repos": repo_meta,
        "runs": runs,
        "compression_sweeps": sweeps,
    }
    json_dump(RESULTS_PATH, payload)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_markdown_report(payload), encoding="utf-8")
    logger.info(
        "benchmark complete",
        extra={
            "event": "iex_benchmark_complete",
            "detail": {
                "report_path": str(report_path),
                "results_path": str(RESULTS_PATH),
                "run_count": len(runs),
                "sweep_count": len(sweeps),
            },
        },
    )

    if not args.keep_intermediate:
        cleanup_sweep_outputs(output_root, repos)
    return 0


def stage_archive_days(
    archive_root: Path, days: list[str], local_archive_cache: Path | None
) -> Path:
    sources = [resolve_archive_day(archive_root, day) for day in days]
    if local_archive_cache is None:
        return archive_root

    local_archive_cache.mkdir(parents=True, exist_ok=True)
    for source in sources:
        target = local_archive_cache / source.name
        if target.exists() and target.stat().st_size == source.stat().st_size:
            LOGGER.info(
                "archive cache hit",
                extra={
                    "event": "iex_benchmark_archive_cache_hit",
                    "day": source.name[:8],
                    "detail": {"source": str(source), "target": str(target)},
                },
            )
            continue
        LOGGER.info(
            "archive cache stage",
            extra={
                "event": "iex_benchmark_archive_cache_stage",
                "day": source.name[:8],
                "detail": {"source": str(source), "target": str(target)},
            },
        )
        shutil.copy2(source, target)
    return local_archive_cache


def execute_repo_day_series(
    repo_meta_rows: list[dict[str, str]],
    days: list[str],
    archive_root: Path,
    output_root: Path,
    compression: str,
    *,
    max_parallel: int,
) -> list[dict]:
    return _run_repo_series(
        repo_meta_rows,
        max_parallel=max_parallel,
        fn=lambda repo_meta: [
            run_repo_day(repo_meta, archive_root, output_root, day, compression) for day in days
        ],
    )


def execute_repo_sweeps(
    repo_meta_rows: list[dict[str, str]],
    archive_root: Path,
    output_root: Path,
    *,
    max_parallel: int,
) -> list[dict]:
    codecs = ("snappy", "zstd1", "zstd3", "zstd5")
    return _run_repo_series(
        repo_meta_rows,
        max_parallel=max_parallel,
        fn=lambda repo_meta: [
            run_repo_day(
                repo_meta,
                archive_root,
                output_root,
                REPRESENTATIVE_SWEEP_DAY,
                codec,
                sweep_only=True,
            )
            for codec in codecs
        ],
    )


def _run_repo_series(
    repo_meta_rows: list[dict[str, str]],
    *,
    max_parallel: int,
    fn: callable,
) -> list[dict]:
    results_by_repo: dict[str, list[dict]] = {}
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(max_parallel, len(repo_meta_rows))
    ) as executor:
        future_map = {
            executor.submit(fn, repo_meta): repo_meta["repo_key"] for repo_meta in repo_meta_rows
        }
        for future in concurrent.futures.as_completed(future_map):
            repo_key = future_map[future]
            results_by_repo[repo_key] = future.result()

    ordered_results: list[dict] = []
    for repo_meta in repo_meta_rows:
        ordered_results.extend(results_by_repo[repo_meta["repo_key"]])
    return ordered_results


def run_repo_day(
    repo_meta: dict[str, str],
    archive_root: Path,
    output_root: Path,
    day: str,
    compression: str,
    *,
    sweep_only: bool = False,
) -> dict:
    input_path = resolve_archive_day(archive_root, day)
    main_output, quote_output = benchmark_output_paths(
        output_root, day, repo_meta["repo_key"], compression
    )
    result_path = output_root / f"{day}_{repo_meta['repo_key']}_{compression}_result.json"
    time_output = output_root / f"{day}_{repo_meta['repo_key']}_{compression}_time.txt"
    runner_log = output_root / f"{day}_{repo_meta['repo_key']}_{compression}_runner.jsonl"
    for target in (main_output, quote_output, result_path, time_output):
        if target.exists():
            target.unlink()
    if runner_log.exists():
        runner_log.unlink()

    command = [time_binary_path(), "-v", "uv", "run"]
    if REPO_SPECS[repo_meta["repo_key"]]["needs_scapy"]:
        command.extend(["--with", "scapy"])
    command.extend(
        [
            "python",
            "utils/iex_parser_repo_runner.py",
            "--repo",
            repo_meta["repo_key"],
            "--repo-path",
            repo_meta["path"],
            "--input-path",
            str(input_path),
            "--main-output",
            str(main_output),
            "--quote-output",
            str(quote_output),
            "--compression",
            compression,
            "--result-path",
            str(result_path),
            "--log-jsonl",
            str(runner_log),
        ]
    )
    LOGGER.info(
        "run start",
        extra={
            "event": "iex_benchmark_run_start",
            "day": day,
            "detail": {
                "repo_key": repo_meta["repo_key"],
                "compression": compression,
                "input_path": str(input_path),
                "main_output": str(main_output),
                "quote_output": str(quote_output),
                "runner_log": str(runner_log),
            },
        },
    )
    completed = subprocess.Popen(
        command,
        cwd=Path(__file__).resolve().parents[1],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    threads = [
        threading.Thread(
            target=_pump_stream,
            args=(completed.stdout, stdout_lines, repo_meta["repo_key"], day, "stdout"),
            daemon=True,
        ),
        threading.Thread(
            target=_pump_stream,
            args=(completed.stderr, stderr_lines, repo_meta["repo_key"], day, "stderr"),
            daemon=True,
        ),
    ]
    for thread in threads:
        thread.start()
    return_code = completed.wait()
    for thread in threads:
        thread.join()
    stderr_text = "".join(stderr_lines)
    stdout_text = "".join(stdout_lines)
    time_output.write_text(stderr_text, encoding="utf-8")
    result = (
        json.loads(result_path.read_text(encoding="utf-8"))
        if result_path.exists()
        else {"status": "failed", "error": "runner did not emit result"}
    )
    result.update(
        {
            "repo_key": repo_meta["repo_key"],
            "repo_commit": repo_meta["commit"],
            "day": day,
            "compression": compression,
            "exit_status": return_code,
            "resources": parse_time_output(stderr_text),
            "input_size_bytes": input_path.stat().st_size,
            "preprocessing": result.get(
                "preprocessing", {"input_mode": repo_meta["input_support"], "steps": []}
            ),
            "sweep_only": sweep_only,
            "runner_log": str(runner_log),
            "stdout_log_excerpt": stdout_text.splitlines()[-20:],
        }
    )
    result["main_size_bytes"] = result.get("main_output", {}).get("size_bytes")
    result["quote_size_bytes"] = result.get("quote_output", {}).get("size_bytes")
    result["main_row_groups"] = result.get("main_output", {}).get("row_groups")
    result["quote_row_groups"] = result.get("quote_output", {}).get("row_groups")
    LOGGER.info(
        "run complete",
        extra={
            "event": "iex_benchmark_run_complete",
            "day": day,
            "detail": {
                "repo_key": repo_meta["repo_key"],
                "compression": compression,
                "status": result.get("status"),
                "exit_status": return_code,
                "main_rows": result.get("main_rows"),
                "quote_rows": result.get("quote_rows"),
                "max_rss_kb": result["resources"].get("max_rss_kb"),
                "elapsed_wall_seconds": result["resources"].get("elapsed_wall_seconds"),
            },
        },
    )
    return result


def parse_time_output(text: str) -> dict[str, float | int | None]:
    values: dict[str, float | int | None] = {
        "elapsed_wall_seconds": None,
        "user_cpu_seconds": None,
        "system_cpu_seconds": None,
        "max_rss_kb": None,
    }
    for line in text.splitlines():
        if "User time (seconds):" in line:
            values["user_cpu_seconds"] = float(line.rsplit(":", 1)[1].strip())
        elif "System time (seconds):" in line:
            values["system_cpu_seconds"] = float(line.rsplit(":", 1)[1].strip())
        elif "Elapsed (wall clock) time" in line:
            values["elapsed_wall_seconds"] = wall_to_seconds(line.split("):", 1)[1].strip())
        elif "Maximum resident set size" in line:
            values["max_rss_kb"] = int(line.rsplit(":", 1)[1].strip())
    return values


def wall_to_seconds(value: str) -> float:
    parts = value.split(":")
    if len(parts) == 1:
        return float(parts[0])
    if len(parts) == 2:
        minutes, seconds = parts
        return int(minutes) * 60 + float(seconds)
    hours, minutes, seconds = parts
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def cleanup_sweep_outputs(output_root: Path, repos: list[str]) -> None:
    for repo_key in repos:
        for codec in ("zstd1", "zstd3", "zstd5"):
            main_output, quote_output = benchmark_output_paths(
                output_root, REPRESENTATIVE_SWEEP_DAY, repo_key, codec
            )
            for target in (main_output, quote_output):
                if target.exists():
                    target.unlink()


def repo_meta_map(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row["repo_key"]: row for row in rows}


def _pump_stream(
    stream: object | None, sink: list[str], repo_key: str, day: str, stream_name: str
) -> None:
    if stream is None:
        return
    for line in stream:
        sink.append(line)
        LOGGER.info(
            "%s",
            line.rstrip(),
            extra={
                "event": f"iex_benchmark_child_{stream_name}",
                "day": day,
                "detail": {"repo_key": repo_key, "stream": stream_name},
            },
        )


if __name__ == "__main__":
    raise SystemExit(main())
