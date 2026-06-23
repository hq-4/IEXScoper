from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_DAYS = ("20240105", "20250113", "20250221")
REPRESENTATIVE_SWEEP_DAY = "20240105"
REFERENCE_MAIN_PATH = Path("/media/tn/pq/2024/01/20240102_IEXTP1_TOPS1.6.parquet")
REFERENCE_QUOTE_PATH = Path("/media/tn/pq/2024/01/20240102_IEXTP1_TOPS1.6_QuoteUpdate.parquet")
REPO_SPECS = {
    "rob-blackbourn": {
        "slug": "rob-blackbourn_iex_parser",
        "github": "https://github.com/rob-blackbourn/iex_parser.git",
        "import_root": "iex_parser",
        "input_support": "pcap.gz_direct",
        "needs_scapy": True,
    },
    "hq-4": {
        "slug": "hq-4_IEXTools",
        "github": "https://github.com/hq-4/IEXTools.git",
        "import_root": "IEXTools",
        "input_support": "pcap.gz_direct",
        "needs_scapy": False,
    },
}
PARQUET_CODECS = {
    "snappy": {"compression": "snappy", "compression_level": None},
    "zstd1": {"compression": "zstd", "compression_level": 1},
    "zstd3": {"compression": "zstd", "compression_level": 3},
    "zstd5": {"compression": "zstd", "compression_level": 5},
}
TYPE_NAME_MAP = {
    "system_event": "SystemEvent",
    "security_directive": "SecurityDirective",
    "trading_status": "TradingStatus",
    "retail_liquidity_indicator": "RetailLiquidity",
    "operational_halt": "OperationalHalt",
    "short_sale_price_test_status": "ShortSalePriceSale",
    "quote_update": "QuoteUpdate",
    "trade_report": "TradeReport",
    "official_price": "OfficialPrice",
    "trade_break": "TradeBreak",
    "auction_information": "AuctionInformation",
}
SALE_FLAG_BITS = (
    (0x80, "iso"),
    (0x40, "extended_hours"),
    (0x20, "odd_lot"),
    (0x10, "trade_through_exempt"),
    (0x08, "single_price_cross"),
)
SYSTEM_EVENT_STRINGS = {
    "O": "start_of_messages",
    "S": "start_of_system_hours",
    "R": "start_of_regular_hours",
    "C": "end_of_messages",
    "E": "end_of_system_hours",
    "M": "end_of_regular_hours",
}
TRADING_STATUS_STRINGS = {
    "H": "all_halted",
    "O": "iex_released",
    "P": "iex_paused",
    "T": "iex_trading",
}
MAIN_COLUMNS = (
    "type",
    "timestamp",
    "symbol",
    "system_event",
    "system_event_str",
    "flags",
    "round_lot_size",
    "adjusted_poc_close",
    "luld_tire",
    "price",
    "status",
    "reason",
    "trading_status_message",
    "retail_liquidity_indicator",
    "halt_status",
    "short_sale_status",
    "detail",
    "size",
    "price_int",
    "trade_id",
    "price_type",
    "sale_flags",
)
QUOTE_COLUMNS = (
    "type",
    "timestamp",
    "symbol",
    "flags",
    "bid_size",
    "bid_price_int",
    "ask_price_int",
    "ask_size",
    "bid_price",
    "ask_price",
)


@dataclass
class ReferenceSchemaSummary:
    path: str
    columns: list[dict[str, str | bool]]
    compression: list[str]
    row_groups: int
    rows: int


def parse_csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def resolve_archive_day(archive_root: Path, day: str) -> Path:
    target = archive_root / f"{day}_IEXTP1_TOPS1.6.pcap.gz"
    if not target.exists():
        raise FileNotFoundError(f"missing archive day file: {target}")
    return target


def benchmark_output_paths(
    output_root: Path, day: str, repo_key: str, codec: str
) -> tuple[Path, Path]:
    slug = REPO_SPECS[repo_key]["slug"]
    suffix = "" if codec == "snappy" else f"_{codec}"
    return (
        output_root / f"{day}_{slug}_TOPS1.6{suffix}.parquet",
        output_root / f"{day}_{slug}_TOPS1.6_QuoteUpdate{suffix}.parquet",
    )


def load_reference_schemas(
    main_path: Path = REFERENCE_MAIN_PATH,
    quote_path: Path = REFERENCE_QUOTE_PATH,
) -> tuple[pa.Schema, pa.Schema]:
    return pq.ParquetFile(main_path).schema_arrow, pq.ParquetFile(quote_path).schema_arrow


def summarize_reference_schema(path: Path) -> ReferenceSchemaSummary:
    pf = pq.ParquetFile(path)
    schema = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in pf.schema_arrow
    ]
    codecs = sorted(
        {
            pf.metadata.row_group(i).column(j).compression
            for i in range(pf.metadata.num_row_groups)
            for j in range(pf.metadata.row_group(i).num_columns)
        }
    )
    return ReferenceSchemaSummary(
        path=str(path),
        columns=schema,
        compression=codecs,
        row_groups=pf.metadata.num_row_groups,
        rows=pf.metadata.num_rows,
    )


def build_environment_summary() -> dict[str, str]:
    uname = platform.uname()
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "machine": uname.machine,
        "system": uname.system,
        "release": uname.release,
        "cpu_count": str(os.cpu_count() or 0),
        "uv": _safe_capture(["uv", "--version"]),
    }


def ensure_repo_checkout(repo_key: str, cache_root: Path) -> Path:
    spec = REPO_SPECS[repo_key]
    target = cache_root / spec["slug"]
    if target.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "clone", spec["github"], str(target)], check=True)
    return target


def repo_commit(repo_root: Path) -> str:
    return _safe_capture(["git", "-C", str(repo_root), "rev-parse", "HEAD"])


def parquet_write_options(codec: str) -> dict[str, str | int | None]:
    if codec not in PARQUET_CODECS:
        raise ValueError(f"unsupported compression codec {codec}")
    return dict(PARQUET_CODECS[codec])


def time_binary_path() -> str:
    for candidate in ("/usr/bin/time", shutil.which("time")):
        if candidate:
            return candidate
    raise FileNotFoundError("unable to locate time binary")


def decode_sale_flags(flags: int | bytes | None) -> str | None:
    if flags is None:
        return None
    if isinstance(flags, bytes):
        if not flags:
            return None
        flags = flags[0]
    values = [label for bit, label in SALE_FLAG_BITS if flags & bit]
    return "|".join(values) if values else None


def json_dump(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _safe_capture(command: list[str]) -> str:
    try:
        return subprocess.check_output(command, text=True).strip()
    except Exception:  # noqa: BLE001
        return "unavailable"


def as_json(data: object) -> object:
    if hasattr(data, "__dataclass_fields__"):
        return asdict(data)
    return data
