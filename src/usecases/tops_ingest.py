from __future__ import annotations

import csv
import gzip
import json
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import polars as pl
import requests

from src.adapters.io.csv_trades_reader import scan_trades_csv_for_day
from src.framework.config import Settings

HIST_URL = "https://iextrading.com/api/1.0/hist"
SAMPLE_DAYS = (
    "20250102",
    "20250407",
    "20250731",
    "20251031",
    "20251120",
    "20260102",
    "20260130",
    "20260422",
)

SPEC_AUDIT_ROWS: tuple[dict[str, str], ...] = (
    {
        "topic": "Transport framing",
        "spec": "IEX-TP v1 message length prefixes each variable-length message.",
        "parser": "Reads tuple message length, advances by length, and validates final offset.",
        "status": "compatible",
    },
    {
        "topic": "Unknown/grown messages",
        "spec": "TOPS receivers must tolerate unknown message types and messages with appended fields.",
        "parser": "Dispatches only T/8/5 and ignores unhandled types using message length framing.",
        "status": "compatible_for_trade_ingestion",
    },
    {
        "topic": "Trade Report T length",
        "spec": "T has the stable leading fields: type, sale flags, timestamp, symbol, size, price, trade id.",
        "parser": "Reads offsets 0..37 and ignores any appended bytes.",
        "status": "compatible",
    },
    {
        "topic": "Trade Break B",
        "spec": "B identifies broken trades and has the same core trade fields.",
        "parser": "Does not emit B rows. Aggregation also excludes sale conditions containing cancellation/correction labels.",
        "status": "intentional_gap",
    },
    {
        "topic": "Quote Update Q",
        "spec": "TOPS top-of-book quote update is message Q.",
        "parser": "Does not emit Q; bundled parser emits DEEP price-level update types 8 and 5.",
        "status": "not_supported",
    },
    {
        "topic": "Administrative/Auction/Retail messages",
        "spec": "TOPS includes S,D,H,O,P,A,X,I and related non-trade messages.",
        "parser": "Ignored by message type. This is acceptable for trade-only aggregation.",
        "status": "compatible_for_trade_ingestion",
    },
    {
        "topic": "Timestamp semantics",
        "spec": "Timestamp fields are nanoseconds since POSIX epoch UTC; event time is seconds UTC.",
        "parser": "Trade CSV Raw Timestamp is the message timestamp in nanoseconds; aggregation treats it as UTC ns.",
        "status": "compatible",
    },
    {
        "topic": "Sale condition flags",
        "spec": "Trade flags encode ISO, session, odd-lot, Rule 611 exemption, and single-price cross.",
        "parser": "Decodes those bits into pipe-separated labels; odd lots are retained downstream.",
        "status": "compatible",
    },
)


@dataclass
class StageMetric:
    day: str
    stage: str
    status: str
    started_at: str
    finished_at: str
    elapsed_seconds: float
    feed: str = "TOPS"
    path: str | None = None
    size_bytes: int | None = None
    row_count: int | None = None
    min_timestamp_ns: int | None = None
    max_timestamp_ns: int | None = None
    exit_code: int | None = None
    error: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)


class MetricsWriter:
    def __init__(self, report_root: Path) -> None:
        self.report_root = report_root
        self.report_root.mkdir(parents=True, exist_ok=True)
        self.jsonl_path = self.report_root / "tops_ingest_metrics.jsonl"
        self.csv_path = self.report_root / "tops_ingest_metrics.csv"
        self.rows: list[dict[str, Any]] = []

    def write(self, metric: StageMetric) -> None:
        row = asdict(metric)
        self.rows.append(row)
        with self.jsonl_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, default=str, sort_keys=True) + "\n")

    def flush_csv(self) -> None:
        if not self.rows:
            return
        fieldnames = list(self.rows[0].keys())
        with self.csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in self.rows:
                flat = dict(row)
                flat["detail"] = json.dumps(flat["detail"], sort_keys=True)
                writer.writerow(flat)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _timed_metric(day: str, stage: str, fn: Any) -> StageMetric:
    started = _utc_now()
    start = time.perf_counter()
    try:
        detail = fn()
    except Exception as exc:  # noqa: BLE001
        finished = _utc_now()
        return StageMetric(
            day=day,
            stage=stage,
            status="failed",
            started_at=started,
            finished_at=finished,
            elapsed_seconds=round(time.perf_counter() - start, 6),
            error=str(exc),
        )
    finished = _utc_now()
    return StageMetric(
        day=day,
        stage=stage,
        status="succeeded",
        started_at=started,
        finished_at=finished,
        elapsed_seconds=round(time.perf_counter() - start, 6),
        **detail,
    )


def write_tops_spec_audit(report_root: Path) -> dict[str, str | int]:
    report_root.mkdir(parents=True, exist_ok=True)
    json_path = report_root / "tops_spec_audit.json"
    csv_path = report_root / "tops_spec_audit.csv"
    payload = {
        "feed": "TOPS",
        "spec_version": "1.66",
        "transport_spec": "IEX-TP v1",
        "generated_at": _utc_now(),
        "sources": [
            "https://www.iex.io/resources/trading/market-data",
            "https://www.iex.io/documents/tops-v1-66",
        ],
        "rows": list(SPEC_AUDIT_ROWS),
    }
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["topic", "spec", "parser", "status"])
        writer.writeheader()
        writer.writerows(SPEC_AUDIT_ROWS)
    return {"json": str(json_path), "csv": str(csv_path), "rows": len(SPEC_AUDIT_ROWS)}


def discover_hist_files(session: requests.Session | None = None) -> dict[str, dict[str, Any]]:
    client = session or requests.Session()
    response = client.get(HIST_URL, timeout=60)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict):
        entries = payload.get("data", payload.get("files", payload.get("results")))
        if entries is None:
            entries = []
            for day_entries in payload.values():
                if isinstance(day_entries, list):
                    entries.extend(day_entries)
    else:
        entries = payload
    discovered: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        raw_name = str(
            entry.get("name")
            or entry.get("file")
            or entry.get("filename")
            or Path(str(entry.get("link") or "")).name
            or ""
        )
        url = str(entry.get("url") or entry.get("link") or entry.get("downloadUrl") or "")
        feed = str(entry.get("feed") or "").upper()
        protocol = str(entry.get("protocol") or "").upper()
        text = f"{feed} {protocol} {raw_name} {url}".upper()
        if "TOPS" not in text or "IEXTP1" not in text:
            continue
        day = str(entry.get("date") or "") or _extract_day(raw_name) or _extract_day(url)
        if not day:
            continue
        if url and not url.startswith(("http://", "https://")):
            url = urljoin(HIST_URL + "/", url)
        discovered[day] = {
            "day": day,
            "name": raw_name or Path(url).name,
            "url": url,
            "size_bytes": _coerce_int(entry.get("size") or entry.get("fileSize") or entry.get("bytes")),
            "raw": entry,
        }
    return discovered


def _extract_day(value: str) -> str | None:
    digits = "".join(ch if ch.isdigit() else " " for ch in value).split()
    for token in digits:
        if len(token) == 8 and token.startswith(("2025", "2026")):
            return token
    return None


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _download(url: str, target: Path) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with target.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    return target.stat().st_size


def _gunzip_if_needed(path: Path) -> Path:
    if path.suffix != ".gz":
        return path
    target = path.with_suffix("")
    with gzip.open(path, "rb") as source, target.open("wb") as dest:
        shutil.copyfileobj(source, dest)
    path.unlink(missing_ok=True)
    return target


def _is_pcapng(path: Path) -> bool:
    with path.open("rb") as handle:
        return handle.read(4) == b"\x0a\x0d\x0d\x0a"


def _ensure_classic_pcap(path: Path) -> tuple[Path, dict[str, Any]]:
    if not _is_pcapng(path):
        return path, {"pcap_format": "pcap"}

    tcpdump = shutil.which("tcpdump")
    if tcpdump is None:
        raise RuntimeError("Downloaded file is pcapng, but tcpdump is not available for conversion")

    converted = path.with_name(path.stem + ".classic.pcap")
    converted.unlink(missing_ok=True)
    result = subprocess.run(
        [tcpdump, "-r", str(path), "-w", str(converted)],
        check=False,
        capture_output=True,
        text=True,
        timeout=None,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    path.unlink(missing_ok=True)
    return converted, {"pcap_format": "pcapng_converted", "converter": tcpdump}


def _is_gzip_url(url: str) -> bool:
    path = unquote(urlparse(url).path)
    return path.endswith(".gz")


def _csv_stats(path: Path) -> dict[str, Any]:
    df = pl.read_csv(path, infer_schema_length=2000)
    stats: dict[str, Any] = {"row_count": df.height}
    if "Raw Timestamp" in df.columns and df.height:
        ts = df["Raw Timestamp"].cast(pl.Int64)
        stats["min_timestamp_ns"] = int(ts.min())
        stats["max_timestamp_ns"] = int(ts.max())
    return stats


def _convert_csv_to_parquet(csv_path: Path, parquet_root: Path, day: str) -> dict[str, Any]:
    target_dir = parquet_root / "raw" / "tops" / day[:4] / day[4:6]
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{day}_IEXTP1_TOPS1.6_trd.parquet"
    df = pl.read_csv(csv_path, infer_schema_length=2000)
    if df.height == 0:
        raise ValueError(f"Parsed trade CSV is empty: {csv_path}")
    df.write_parquet(target, compression="zstd", compression_level=5, statistics=True)
    stats = _csv_stats(csv_path)
    return {"path": str(target), "size_bytes": target.stat().st_size, **stats}


def _aggregation_stats(csv_root: Path, day: str, settings: Settings) -> dict[str, Any]:
    df = scan_trades_csv_for_day(
        csv_root=str(csv_root),
        yyyymmdd=day,
        symbols=None,
        display_tz=settings.display_tz,
        feed="TOPS",
    )
    if df is None or df.height == 0:
        raise ValueError(f"No aggregate rows produced for {day}")
    session_counts = df.group_by("session").len().to_dicts()
    return {
        "row_count": df.height,
        "min_timestamp_ns": int(df["ts_second_utc"].min().timestamp() * 1_000_000_000),
        "max_timestamp_ns": int(df["ts_second_utc"].max().timestamp() * 1_000_000_000),
        "detail": {
            "symbols": df["symbol"].n_unique(),
            "sessions": {row["session"]: row["len"] for row in session_counts},
            "sparse_output_density": round(df.height / (df["symbol"].n_unique() * 86400), 8),
            "min_vwap": float(df["vwap"].min()),
            "max_vwap": float(df["vwap"].max()),
        },
    }


def run_tops_ingest_validation(
    settings: Settings,
    work_root: str,
    report_root: str,
    days: list[str] | None,
    all_available: bool,
    start_day: str,
    end_day: str | None,
    dry_run: bool,
    keep_raw: bool,
    parser_bin: str,
) -> int:
    work = Path(work_root)
    reports = Path(report_root)
    metrics = MetricsWriter(reports)
    write_tops_spec_audit(reports)

    hist = discover_hist_files()
    if all_available:
        selected_days = [
            day for day in sorted(hist) if day >= start_day and (end_day is None or day <= end_day)
        ]
    else:
        selected_days = days or list(SAMPLE_DAYS)
    exit_code = 0
    for day in selected_days:
        info = hist.get(day)
        if not info:
            metrics.write(
                StageMetric(
                    day=day,
                    stage="discover",
                    status="failed",
                    started_at=_utc_now(),
                    finished_at=_utc_now(),
                    elapsed_seconds=0,
                    error="TOPS file not found in HIST index",
                )
            )
            exit_code = 1
            continue

        metrics.write(
            StageMetric(
                day=day,
                stage="discover",
                status="succeeded",
                started_at=_utc_now(),
                finished_at=_utc_now(),
                elapsed_seconds=0,
                path=info.get("url") or info.get("name"),
                size_bytes=info.get("size_bytes"),
            )
        )
        if dry_run:
            continue

        csv_dir = Path(settings.iex_csv_root or work / "csv") / day[:4] / day[4:6]
        pcap_path = work / "pcap" / f"{day}_IEXTP1_TOPS1.6.pcap"
        url = info.get("url")
        if not url:
            metrics.write(
                StageMetric(
                    day=day,
                    stage="download",
                    status="failed",
                    started_at=_utc_now(),
                    finished_at=_utc_now(),
                    elapsed_seconds=0,
                    error="HIST entry has no download URL",
                )
            )
            exit_code = 1
            continue

        def download_stage() -> dict[str, Any]:
            target = pcap_path.with_suffix(pcap_path.suffix + ".gz") if _is_gzip_url(str(url)) else pcap_path
            downloaded_size = _download(str(url), target)
            expected = info.get("size_bytes")
            if expected is not None and downloaded_size != expected:
                raise ValueError(f"downloaded {downloaded_size} bytes, expected {expected}")
            actual = _gunzip_if_needed(target)
            actual, format_detail = _ensure_classic_pcap(actual)
            return {
                "path": str(actual),
                "size_bytes": actual.stat().st_size,
                "detail": {"downloaded_size_bytes": downloaded_size, **format_detail},
            }

        metric = _timed_metric(day, "download", download_stage)
        metrics.write(metric)
        if metric.status != "succeeded":
            exit_code = 1
            continue
        pcap_path = Path(metric.path or pcap_path)

        def parse_stage() -> dict[str, Any]:
            csv_dir.mkdir(parents=True, exist_ok=True)
            output_prefix = csv_dir / f"{day}_IEXTP1_TOPS1.6"
            result = subprocess.run(
                [parser_bin, str(pcap_path), str(output_prefix), "ALL"],
                check=False,
                capture_output=True,
                text=True,
                timeout=None,
            )
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or result.stdout.strip())
            csv_path = output_prefix.with_name(output_prefix.name + "_trd.csv")
            if not csv_path.exists():
                raise FileNotFoundError(csv_path)
            return {
                "path": str(csv_path),
                "size_bytes": csv_path.stat().st_size,
                "exit_code": result.returncode,
                **_csv_stats(csv_path),
            }

        parse_metric = _timed_metric(day, "parse_pcap_to_csv", parse_stage)
        metrics.write(parse_metric)
        if parse_metric.status != "succeeded":
            exit_code = 1
            continue

        csv_path = Path(parse_metric.path or "")

        parquet_root = Path(settings.iex_parquet_root or work / "parquet")
        parquet_metric = _timed_metric(
            day,
            "convert_csv_to_parquet",
            lambda: _convert_csv_to_parquet(csv_path, parquet_root, day),
        )
        metrics.write(parquet_metric)
        if parquet_metric.status != "succeeded":
            exit_code = 1
            continue

        agg_metric = _timed_metric(
            day,
            "aggregate_per_second",
            lambda: _aggregation_stats(Path(settings.iex_csv_root or work / "csv"), day, settings),
        )
        metrics.write(agg_metric)
        if agg_metric.status != "succeeded":
            exit_code = 1
            continue

        if not keep_raw:
            pcap_path.unlink(missing_ok=True)

    metrics.flush_csv()
    return exit_code
