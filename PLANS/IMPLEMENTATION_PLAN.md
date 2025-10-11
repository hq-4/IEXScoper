# IEXScoper — Implementation Plan: Yearly Per‑Second Trade Aggregator

Sources: `PLANS/plans2.md` and confirmed answers in `PLANS/plans2-q.md`. Tags: [CA][PA][REH][IV][CMV][RM][SFT]

---

## 0) Summary

- **Objective**: Build a yearly Parquet dataset of per‑second, per‑symbol trade aggregates derived from IEX TOPS 1.6 Trades. Keep source daily Parquet files intact.
- **Aggregation**: For each symbol and NY‑second, compute VWAP and arithmetic mean of trade price, plus trade count and total share volume. Sparse output (no rows for seconds without trades).
- **Write pattern**: Incremental daily → yearly dataset append, partitioned by `symbol`. Idempotent per day; atomic renames. Register outputs/manifests in Postgres with `UNIQUE(path)`.

---

## 1) Requirements Locked (from `plans2-q.md`)

- **Scope**: IEX TOPS 1.6 Trades only; include odd‑lots; exclude cancels/corrections.
- **Sessions**: Include pre/regular/after‑hours. A `session` column will be emitted for downstream filtering.
- **Time**: Bucket by `exchange_ts`. NY (America/New_York) second boundaries (DST‑aware). Floor to the second.
- **No‑trade seconds**: Sparse (emit nothing).
- **Output grain**: Each row = (symbol, day, second) aggregate; union of all days in the year.
- **Year scope**: Calendar year in NY time.
- **Layout**: Single master Parquet file per year at `{IEX_PARQUET_ROOT}/derived/YYYY/` with staging directory for incremental daily outputs.
- **Engine**: Polars + PyArrow.
- **Compression/layout**: ZSTD level=5; target row group ≈ 500k; dictionary encode small categoricals.
- **Throughput**: Incremental daily staging + nightly compaction to master (idempotent per day).
- **Resources**: 16GB RAM.
- **Idempotency**: Keyed by `(year, dataset_type='trades_per_second', filter_version)`; atomic rename on write.
- **Manifests**: Yearly manifest JSON with: `year, dataset_type, path, files, rows, min_ts, max_ts, hash_xx64, codec, zstd_level, created_at`.
- **DB**: Register `datasets`/`files` with `UNIQUE(path)`.
- **Ops**: Nightly after day ingestion (e.g., 06:30–07:00 ET). Retries/timeouts per global rules.
- **QA**: Sanity checks (`trade_count > 0 ⇒ vwap > 0`), daily coverage metrics, golden tests.
- **Extras**: `session` column only (no separate per‑session dataset). No symbol normalization/mapping. Sparse output.

---

## 2) Output Dataset Spec

- **Root**: `{IEX_PARQUET_ROOT}/derived/${YYYY}/`
- **Master file**: `YYYY_IEX_TOPS1.6_trades_persec.parquet` (written via temp → atomic rename).
- **Staging**: `{IEX_PARQUET_ROOT}/derived/_staging/year=${YYYY}/symbol=XXXX/part-YYYYMMDD-<filterv>-<short-hash>.parquet` for per‑day idempotent staging outputs.
- **Clustering (master)**: Sort rows by `symbol, ts_second_ny` for scan efficiency.
- **Schema (proposed Arrow types)**:
  - `symbol`: string (dict‑encoded)
  - `ts_second_ny`: timestamp[ns, tz="America/New_York"]
  - `ts_second_utc`: timestamp[ns, tz="UTC"]
  - `session`: string (Enum: {pre, regular, after})
  - `vwap`: float64
  - `mean_price`: float64
  - `trade_count`: int64
  - `share_volume`: int64
  - `year`: int16 (redundant but helpful for pruning)
  - `day`: date32 (NY date of the bucket)
- **Compression**: ZSTD level=5; row group target ≈ 500k; dict encoding for `symbol` and small enums.

---

## 3) Input Assumptions

- **Inputs**: Daily CSV Trades output from `iex-parser` (files ending in `_trd.csv`) for each date. Read via Polars scan; aggregate to per‑second; write per‑day Parquet staging; nightly compact staging into yearly master.
- **Key columns expected**: Map `iex-parser` CSV columns → aggregator fields: `"Exchange Timestamp"` (ns) → `exchange_ts` (UTC), `"Symbol"` → `symbol`, `"Size"` → `size`, `"Price"` → `price`, `"Trade ID"` → `trade_id`, `"Sale Condition"` for filtering cancels/corrections (keep odd‑lots).

---

## 4) Clean Architecture Deliverables [CA]

- `src/domain/`
  - `enums.py`: `DatasetType` (e.g., `TRADES_PER_SECOND`), `Session` enum.
  - `models.py`: `PerSecondTradeRow` (dataclass/pydantic), `Manifest`, `RunRecord`.
- `src/usecases/`
  - `aggregate_per_second.py`: `aggregate_year(year, filter_version='v1')`, `aggregate_day(day, year_ctx)`; orchestrates read→aggregate→write→register.
  - `register_backfill_year.py`: scan existing year and reconcile manifests/DB.
- `src/adapters/`
  - `io/trades_reader.py`: scan daily trades Parquet (Polars scan), filter bad flags, expose NY‑second truncation.
  - `io/derived_writer.py`: dataset writer with temp paths + atomic rename; symbol partition management; removal of prior day parts per symbol.
  - `io/manifest_repo.py`: write/read JSON manifests; compute `hash_xx64`.
  - `db/metadata_repo.py`: register datasets/files/runs (SQLAlchemy or psycopg).
  - `time/tz.py`: NY timezone helpers (floor to second, session derivation, NY calendar year membership).
- `src/framework/`
  - `config.py`: read `.env` (`IEX_PARQUET_ROOT`, `DISPLAY_TZ=America/New_York`, `LOG_JSONL_PATH`, `DATABASE_URL`).
  - `logging.py`: dual sinks + startup enforcer, structured fields.
  - `cli.py`: `aggregate-per-second --year 2024 [--rebuild] [--symbols AAPL,AMD] [--dry-run] [--limit-days N]`.

---

## 5) Algorithm & Data Flow

```mermaid
flowchart LR
  CLI[CLI: aggregate-per-second] --> UC[Usecase: aggregate_year]
  UC --> D[Discover daily trade files]
  D --> R[Reader: Polars scan]
  R --> F[Filter cancels/corrections; keep odd-lots]
  F --> T[Time: convert to NY, floor to 1s, derive session]
  T --> G[GroupBy (symbol, ts_second_ny)]
  G --> A[Agg: VWAP, mean, count, sum(size)]
  A --> W[Writer: per-symbol partition; temp -> atomic rename]
  W --> M[Manifest: yearly JSON]
  M --> DB[(DB: datasets/files/runs)]
```

- **Discovery**: List daily inputs for target NY year (timezone‑aware boundaries).
- **Transform**:
  - `ts_ny = exchange_ts.convert_time_zone('America/New_York')`
  - `ts_second_ny = ts_ny.dt.truncate('1s')`
  - `ts_second_utc = ts_second_ny.convert_time_zone('UTC')`
  - `session = f(ts_second_ny)` where pre [04:00, 09:30), regular [09:30, 16:00), after [16:00, 20:00) NY.
  - Filter (v1): exclude rows where `Sale Condition` contains any of {`CANCEL`, `CORRECTION`, `CORR`}; retain odd‑lots and extended hours.
  - Dedupe: drop duplicates by `(trade_id, exchange_ts, symbol)`.
  - Aggregations per `(symbol, ts_second_ny)`:
    - `share_volume = sum(size)`
    - `trade_count = count()`
    - `vwap = sum(price * size) / sum(size)` (guard `sum(size) > 0`)
    - `mean_price = mean(price)`
  - Add `day = date(ts_second_ny)` and `year = year(ts_second_ny)`
- **Write (staging)**: For each symbol, write per‑day staging parts to `{IEX_PARQUET_ROOT}/derived/_staging/year=${YYYY}/symbol=SYM/part-YYYYMMDD-...` with ZSTD=5, row group ≈ 500k. Use temp path then atomic rename.
- **Compact (master)**: Stream read staging for the year ordered by `symbol, ts_second_ny` and write to `{IEX_PARQUET_ROOT}/derived/${YYYY}/YYYY_IEX_TOPS1.6_trades_persec.parquet` via temp → atomic rename.
- **Register**: Update manifest + DB entries for the master file; optionally track staging parts for lineage.

---

## 6) Idempotency & Atomicity [REH][RM]

- **Per-day idempotence**: For `(year, day, symbol, filter_version)` locate existing `symbol=SYM/part-YYYYMMDD-*` and replace atomically.
- **Temp paths**: Write to `${...}/_tmp/{uuid}/...` then `rename()`.
- **Reruns**: If a subset of days reprocessed, only affected symbol parts are replaced; manifest recomputed.
- **Hashing**: Compute `hash_xx64` of concatenated part file contents (or metadata + row counts) to detect drift.

---

## 7) Manifest & DB Registration [CA]

- **Manifest (yearly JSON)**:
  - `year, dataset_type='trades_per_second', filter_version`
  - `root_path, master_path, source_days, input_files, files=[{symbol, day, part_path, size_bytes, rows, min_ts, max_ts, hash_xx64}]`
  - `codec='zstd', zstd_level, row_group_rows, created_at`
- **DB**:
  - `datasets`: `{id, dataset_type, year, filter_version, root_path}`
  - `files`: `{id, dataset_id, path UNIQUE, symbol, day, rows, size_bytes, hash_xx64}`
  - `runs`: `{id, kind='aggregate_per_second', year, started_at, ended_at, status, detail}`
  - Indexes: `files(symbol, day)`, `datasets(year, dataset_type)`

---

## 8) CLI, Config, Scheduling

- **CLI**: `uv run python -m src.framework.cli aggregate-per-second --year 2024 [--rebuild] [--symbols AAPL,AMD] [--dry-run] [--limit-days 5]`
- **CLI (compaction)**: `uv run python -m src.framework.cli compact-master --year 2024`
- **.env**: `IEX_PARQUET_ROOT`, `DISPLAY_TZ=America/New_York`, `LOG_JSONL_PATH=logs/app.jsonl`, `DATABASE_URL`.
- **Cron (example)**: `30 6 * * * uv run python -m src.framework.cli aggregate-per-second --year $(date +\%Y)`
  - Runs after ingestion completes and manifests are available.
 - **Retention**: Prune staging parts older than 7 days after successful compaction.

---

## 9) Logging & Observability [REH]

- **Dual sinks**: Rich console + JSONL file at `logs/app.jsonl` with enforcer.
- **Structured fields**: `event='aggregate_per_second'`, `year`, `day`, `symbol`, `part_path`, `rows`, `duration_ms`, `attempt`.
- **Failure handling**: 10s timeouts for SMB/FS ops, 3 retries with exp backoff+jitter. Clear error messages and remediation hints.

---

## 10) Performance Plan [PA]

- **Engine**: Polars lazy + streaming where possible; write via PyArrow Parquet writer.
- **Memory target**: Fit within 16GB RAM by processing day‑by‑day and symbol batches; avoid all‑year materialization.
- **Parallelism**: Optional per‑day concurrency with bounded worker pool; ensure one writer per `(symbol)` at a time per process.
- **Benchmarks**: After initial run, evaluate ZSTD {3,5,7,9} and row group {200k,500k,1M} on 2 symbols × 2 days.

---

## 11) Testing & Validation [tests/]

- **Unit**: Verify NY second bucketing, session derivation, VWAP formula guards, idempotent part file naming.
- **Golden**: On curated small days (e.g., `INTC`, `AMD`) confirm aggregates; compare against reference CSV.
- **Property**: `sum(size) = share_volume`, `trade_count >= 1 ⇒ vwap > 0`.
- **Integration**: End‑to‑end run for a single day; validate manifest and DB records.
- **Fixtures**: Use provided Parquet samples in `sample_data/` (e.g., `sample_data/20250211_IEXTP1_TOPS1.6.parquet`) for reference cross‑checks; aggregator reads CSV but can validate coverage vs these samples.

---

## 12) Risks & Mitigations

- **Timezone/DST complexity**: Centralize tz logic in `time/tz.py`; add tests around DST transitions.
- **SMB atomicity**: Use local temp + rename within same mount; if underlying FS lacks atomic rename guarantees, consider write‑then‑move with manifest guard.
- **Skewed symbols**: Very active symbols may produce large per‑symbol parts; mitigate by row group sizing and, if needed, multiple parts per day per symbol.
- **Flag semantics drift**: Lock `filter_version='v1'` and document; any future change increments version.

---

## 13) Work Items & Milestones

- **W1 — Spec Freeze (this doc)**: Confirm root path and engine.
- **W2 — Scaffolding**: Modules, config, logging enforcer, CLI stub.
- **W3 — Reader + TZ utils**: Polars scan + NY floor‑second + session.
- **W4 — Aggregator**: VWAP/mean/count/volume per (symbol, second). Sparse output.
- **W5 — Writer**: Staging writer (per‑day parts) + master compaction writer; temp→atomic rename; ZSTD/row group.
- **W6 — Manifests + DB**: JSON manifest create/update; DB repositories and registration.
- **W7 — Tests**: Unit + golden + integration. Add sample fixtures.
- **W8 — Bench & Tune**: Validate performance within 16GB; adjust where needed.
- **W9 — Cron & Docs**: Add cron example, README updates.

---

## 14) Open Items (please confirm)

- All items confirmed in `PLANS/plans3.md` (v1 locked):
  - **Sale condition exclusion set**: Exclude `CANCEL`, `CORRECTION`, `CORR`; keep `ODD_LOT`, `EXTENDED_HOURS`.
  - **Session windows**: pre [04:00, 09:30), regular [09:30, 16:00), after [16:00, 20:00) NY.
  - **Staging retention**: Keep 7 days after successful compaction; then prune.
  - **Dedupe policy**: Drop duplicates by `(trade_id, exchange_ts, symbol)`.
  - **Master file size**: Single yearly master only; no mid‑year splits.

Once confirmed, I will start scaffolding per sections W2–W5 and add corresponding tests (W7).
