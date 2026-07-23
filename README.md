# IEXScoper

IEXScoper is a research platform for ingesting and analyzing IEX exchange historical market
data. It downloads daily TOPS/DEEP PCAP captures from the IEX HIST API, parses them to Parquet,
and builds derived research datasets on top — per-second per-symbol trade aggregates, daily
OHLCV bars, and a symbol-era continuity panel. A second track resolves dead/delisted ticker
identities using SEC EDGAR, OpenFIGI, and IEX entity snapshots.

## Features

- **TOPS ingest pipeline**: download daily `.pcap.gz` HIST captures, parse with the bundled
  C++-accelerated `iex-parser`, and write two-file TOPS Parquet (TradeReport + QuoteUpdate)
- **Per-second per-symbol aggregation** with Polars: VWAP, mean price, share volume, and trade
  count, labeled by session (pre-market / regular / after-hours)
- **Staging → master compaction**: Parquet staging parts partitioned by symbol, compacted into
  yearly master files (Zstandard compression)
- **Ingest validation**: spec audits, throughput profiling, and scratch-disk budget guards for
  large multi-year backfills
- **Derived research datasets**: symbol eras, daily trade bars, stable-universe panel, and
  stable returns tables
- **Dead-ticker identity resolution**: evidence-based classification and enrichment via SEC
  EDGAR (tickers, submissions, full-text search), OpenFIGI, and daily IEX entity snapshots,
  with facts stored as JSONL + SQLite under `data/resolution/`
- **uv-managed dependencies** through `pyproject.toml` and `uv.lock`

## Setup

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- A C++ toolchain (to build the bundled `iex-parser` binary)

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd IEXScoper
   ```

2. Install dependencies:
   ```bash
   uv sync
   ```

3. Build the vendored PCAP parser (binary lands at
   `iex-parser/iex_cppparser/bin/iex_parser.out`; see `iex-parser/README.md`):
   ```bash
   cd iex-parser/iex_cppparser && uv run python compile_cpp.py && cd ../..
   ```

4. Configure input/output roots:
   ```bash
   cp .env.example .env
   # edit .env with IEX_CSV_ROOT, IEX_PARQUET_ROOT, IEX_WORK_ROOT, IEX_REPORT_ROOT
   ```

   | Variable | Purpose | Default |
   |---|---|---|
   | `IEX_CSV_ROOT` | Parsed trade CSV input root | — |
   | `IEX_PARQUET_ROOT` | Published Parquet output root (e.g. NAS) | — |
   | `IEX_WORK_ROOT` | PCAP scratch space and fallback CSV/Parquet locations | `data/iex` |
   | `IEX_REPORT_ROOT` | Validation metrics and audit reports | `reports` |
   | `DISPLAY_TZ` | Display timezone | `America/New_York` |
   | `LOG_JSONL_PATH` | Structured log file | `logs/app.jsonl` |
   | `DATABASE_URL` | Optional Postgres metadata store | — |

   Backfills assume large local NVMe scratch (guarded by `--min-scratch-free-gb`, typically
   120–200 GB) and a publish target such as a NAS mount.

## Usage

The CLI is exposed as `iexscoper` (`src/framework/cli.py`). Run everything via `uv run`.

Aggregate parsed trade CSVs into per-second staging Parquet files:

```bash
uv run iexscoper aggregate-per-second --year 2025 [--symbols AAPL,MSFT] [--rebuild] [--dry-run] [--limit-days 10]
```

Compact staging Parquet files into the yearly master file:

```bash
uv run iexscoper compact-master --year 2025
```

Validate TOPS discovery, download, PCAP parsing, raw Parquet conversion, and aggregation
before a broad backfill:

```bash
uv run iexscoper tops-spec-audit --report-root reports
uv run iexscoper validate-tops-ingest --dry-run
uv run iexscoper validate-tops-ingest --all-available --limit-days 10 --max-workers 1
uv run iexscoper tops-profile-report --report-root reports
```

Explicit `--work-root` / `--report-root` CLI arguments override the `.env` values.

Beyond the CLI, most operational work runs as standalone scripts under `utils/`, e.g.:

```bash
uv run python utils/backfill_tops_iextools.py --help
uv run python utils/run_dead_ticker_resolution_program.py --help
```

## Project Structure

```
IEXScoper/
├── pyproject.toml         # Package metadata and dependencies
├── uv.lock                # Locked uv dependency graph
├── .env.example           # Required environment variables (no secrets)
├── src/                   # Active application package (clean architecture)
│   ├── domain/            # Pure models: per-second rows, dataset/session enums
│   ├── usecases/          # TOPS ingest, per-second aggregation orchestration
│   ├── adapters/          # I/O boundaries: CSV readers, Parquet writers, DB
│   └── framework/         # CLI entrypoint, config, dual-sink logging
├── utils/                 # One-off operational scripts (backfill, enrichment, resolution)
├── tests/                 # pytest suite mirroring src/ and utils/
├── iex-parser/            # Vendored C++-accelerated IEX PCAP parser (github.com/hq-4/iex-parser)
├── data/                  # Manual overrides and resolution facts (runtime data gitignored)
├── iex_entities/          # Daily IEX entity snapshots (YYYY-MM-DD.json)
├── sample_data/           # Sample TOPS 1.6 Parquet files
├── reports/               # Generated audit/validation reports (gitignored)
├── logs/                  # Runtime logs (gitignored)
├── docs/                  # Living docs: architecture, task list, logging, security, changelog
└── PLANS/                 # Design and planning documents
```

## Development

```bash
uv run -m pytest -q          # tests (markers: unit, integration, slow)
uv run ruff check .          # lint
uv run ruff format .         # format
uv run bandit -q -r src      # static security analysis
```

Logging uses a dual-sink strategy (pretty Rich console + rotating JSONL at `logs/app.jsonl`),
enforced at startup. See `docs/LOGGING.md`.

## External Services

Some `utils/` workflows call external APIs and require polite configuration:

- **IEX HIST** (`https://iextrading.com/api/1.0/hist`) — PCAP capture downloads
- **SEC EDGAR** — ticker/submissions APIs and full-text search (custom User-Agent required)
- **OpenFIGI** — instrument mapping (rate-limited, responses cached)

## License

This project is open source. Please check individual dependency licenses for their terms.
