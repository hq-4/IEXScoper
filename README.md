# IEXScoper

IEXScoper is a research platform for ingesting and analyzing IEX exchange historical market
data. It downloads daily TOPS/DEEP PCAP captures from the IEX HIST API, parses them to Parquet,
and builds derived research datasets on top — per-second per-symbol trade aggregates, daily
OHLCV bars, and a symbol-era continuity panel. A second track resolves dead/delisted ticker
identities through an evidence-delta program over SEC EDGAR, OpenFIGI, and IEX entity
snapshots, with verified facts stored as canonical JSONL.

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
- **Dead-ticker identity resolution (V2)**: an evidence-delta program over the stable
  26,184-era cohort. Canonical identity, event, observation, attempt, and research-decision
  facts with deterministic IDs live as JSONL under `data/resolution/`, backed by a shared
  SQLite request/resume registry. Identity and event proof stay separate; eligibility
  requires hard gates, and absence of evidence is never promoted to proof. Dry runs stage
  deterministically; `--apply` promotes with zero network calls
- **Identity-first SEC resolution**: resumable workflows for high-impact, lifecycle, and
  terminal eras — unique date-scoped CIK anchoring, overlapping historical submissions shards,
  bounded snippet scoring, and conservative import gates
- **EDGAR triage pipeline**: full-text search hit triage into per-symbol review leads,
  review-only override candidates, and filing-page verification into strong/moderate/weak
  buckets — nothing is auto-imported without manual review
- **Resolution workplan**: impact-weighted lanes, instrument-specific derivative gates
  (share classes, warrants, units, rights, preferreds), automation-exhaustion reporting, and
  a terminal-disposition ledger kept separate from verified identity overrides
- **Enrichment**: OpenFIGI mapping and SEC EDGAR ticker/submissions/full-text search, all
  cached and rate-limit aware
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
```

### Dead Ticker Resolution

The resolution V2 program has a single composition root. A dry run builds a deterministic
stage; `--apply` promotes that stage with zero network calls:

```bash
uv run python utils/run_dead_ticker_resolution_program.py           # dry run
uv run python utils/run_dead_ticker_resolution_program.py --apply   # promote staged facts
```

Supporting workflows (all resumable, all requiring an SEC User-Agent for network access):

```bash
uv run python utils/run_sec_high_impact_identity_resolution_iterations.py --help
uv run python utils/run_sec_lifecycle_resolution_iterations.py --help
uv run python utils/run_sec_terminal_resolution_iterations.py --help
```

See `docs/DEAD_TICKER_RESOLUTION.md` for the full program reference and `docs/ARCHITECTURE.md`
for the layer map and evidence invariants.

## Project Structure

```
IEXScoper/
├── pyproject.toml         # Package metadata and dependencies
├── uv.lock                # Locked uv dependency graph
├── AGENTS.md              # Global engineering rules (RAT tags, gates, logging spec)
├── .env.example           # Required environment variables (no secrets)
├── src/                   # Active application package (clean architecture)
│   ├── domain/            # Pure models: per-second rows, dataset/session enums
│   ├── usecases/          # TOPS ingest, per-second aggregation orchestration
│   ├── adapters/          # I/O boundaries: CSV readers, Parquet writers, DB
│   └── framework/         # CLI entrypoint, config, dual-sink logging
├── utils/                 # Operational scripts: backfill, enrichment, resolution V2, SEC lanes
├── tests/                 # pytest suite mirroring src/ and utils/
├── iex-parser/            # Vendored C++-accelerated IEX PCAP parser (github.com/hq-4/iex-parser)
├── data/
│   ├── manual_overrides/  # Verified identity overrides and resolution ledger (tracked)
│   └── resolution/        # Canonical V2 JSONL facts (tracked; SQLite registry gitignored)
├── iex_entities/          # Daily IEX entity snapshots (gitignored, re-downloadable)
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
