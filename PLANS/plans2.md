# IEXScoper — Plan v2 (Synthesis + Defaults)

This document synthesizes answers from `PLANS/plans-1-q.md` and proposes defaults where unspecified. It enumerates open confirmations and a milestone plan. No code yet—planning only.

---

## 1) Decisions Locked (from plans-1-q)

- **Scope & Objective**
  - IEX-only (TOPS/DEEP), Phase 1 converts prior-day PCAP→Parquet (PyArrow), idempotent per day/type.
  - Cover all sessions (pre/regular/after-hours/halts/auctions). Support daily and ad-hoc rebuilds.
  - Success: Parquet valid, schema validated, reasonableness checks to catch partial/corrupt days.
- **Input**
  - Source: SMB-mounted share (root path via `.env`). Input PCAPs are gz.
  - Arrival time: unknown; assume 06:00 EST; add a small detector to probe availability.
  - Version: TOPS 1.6 only. No multi-segment shards per day.
- **Output layout**
  - Directory: monthly folders `YYYY/MM/` only (no `DD/`). Expect ~60 files/month (2 per day).
  - Per-day split: 2 files per day — Confirmed Trades and QuoteUpdate.
  - Partitioning: logical partitioning by symbol is inside the Parquet file, not the directory tree.
  - Atomic writes + manifest per day/month.
- **Schema & Time**
  - Include `schema_version`. Internal UTC; API queries and display in EST.
- **Idempotency**
  - Keyed by (date, dataset_type[, segment]); on rerun overwrite outputs. Store checksums/row counts in DB. Recover from partial via manifests/DB.
- **Perf/Storage**
  - Compression: ZSTD (level TBD empirically). PCAP retention: delete after 30 days; keep Parquet indefinitely.
- **DB & API**
  - DB: Postgres; DSN from `.env`. Track `datasets`, `files`, `runs` (and optionally `partitions`). Index by date range.
  - API (Phase 1): `/status/health`, `/files`, `/stats/daily`, `/discover/backfill/register`. CLI-only for long tasks.
- **Phase 2/3/4 (directional)**
  - OB engine: BBO/L1 first (event-driven), delta-encoded, precompute trade alignment indices.
  - UI: WebSockets, basic controls, dark mode, 30s scrollback.
  - Analytics nightly: effective/realized spread, impact, fill probability, iceberg heuristics; windows +100ms/+1s/+5s; spread buckets 1/2/≥3; exclude crossed/locked; include odd-lots.
- **Infra & Process**
  - Python 3.12 via `uv`. PyArrow required; Polars allowed. Cron for scheduling. Dual-sink logging (Rich console + JSONL file) with startup enforcer. Behind VPN.

---

## 2) Proposed Defaults for Ambiguities (please confirm)

- **File naming (output)**
  - Trades: `YYYYMMDD_IEXTP1_TOPS1.6.parquet`
  - Quotes: `YYYYMMDD_IEXTP1_TOPS1.6_QuoteUpdate.parquet`
  - Rationale: matches example in `PLANS/plans1.md`. If `TP1` varies, we’ll retain it as a segment tag.
- **Manifest spec** (JSON alongside daily files; also roll up monthly)
  - Keys: `date, dataset_type, version, path, size_bytes, rows, min_ts, max_ts, hash_xx64, codec=zstd, zstd_level, row_group_rows, created_at`.
- **Schema discovery first, then freeze v1**
  - Run schema audit over the backfill to enumerate fields/types for Trades and QuoteUpdate; propose v1 with `schema_version='1.0'` (per dataset).
- **Ordering & OOO policy**
  - Sort rows by `(exchange_ts, seq_no, symbol)` where available; also persist a `wire_idx` from parse order for auditing.
- **Crossed/locked handling**
  - Add a boolean/enum flag now; do not filter at write time. Analytics will exclude by default.
- **Validation gates**
  - Hard-fail rows with negative size or zero price (reject/skip file with clear error); soft-warn rare anomalies configurable.
- **Compression & layout**
  - Default ZSTD level = 5 (tune via benchmarks). Target row group size ≈ 500k rows (tune 200k–1M). Enable dictionary encoding for `symbol` and categorical flags. Target per-file size 256–512MB if feasible (given 2 files/day; do not force split unless extremely large).
- **Idempotency mechanics**
  - Single-writer per `(date, type)` at a time; atomic rename from temp path; DB run record with start/end, outcome, rows, hashes.
- **DB uniqueness**
  - At minimum enforce `UNIQUE(path)` to avoid duplicate registrations even if user answered “no” to broader uniqueness.
- **Retry/timeouts**
  - External I/O (SMB; HTTP probes): 10s timeouts, 3 retries with exp backoff+jitter. Log attempts. Keeps with robust error-handling rules.
- **Timezone semantics (API)**
  - Interpret `date` query params in `America/New_York`, map to UTC boundaries internally.
- **.env keys (proposal, not created yet)**
  - `IEX_PCAP_ROOT`, `IEX_PARQUET_ROOT`, `DATABASE_URL`, `DISPLAY_TZ=America/New_York`, `LOG_JSONL_PATH=logs/app.jsonl`.

---

## 3) Clean Architecture Plan (directory-level; no code yet)

- `src/domain/`: entities (DatasetType, FileRecord, RunStatus), value objects (Date, Path), pure logic.
- `src/usecases/`: orchestrators `ingest_prior_day()`, `register_backfill()`, `compute_manifests()`, `schema_audit()`.
- `src/adapters/`:
  - `pcap/reader.py` (custom converter wrapper), `storage/fs_repo.py` (SMB paths, atomic writes), `db/sqlalchemy_*`, `http/api.py`.
- `src/framework/`: config from `.env`, logging bootstrap (dual sinks + enforcer), DI/wiring, CLI entry.
- `tests/`: golden-day tests (INTC, AMD on 2025-01-01, 2025-02-01), property checks, perf smoke.
- `utils/`: one-off scripts (ping-availability probe, compression bench), if needed.

---

## 4) Milestones (Phase 1)

- **M0 — Spec freeze**: Confirm file naming, manifest spec, OOO policy, uniqueness, retries, timezone semantics.
- **M1 — Backfill registration**: Scanner reads existing Parquet under `IEX_PARQUET_ROOT`, writes `files` + `datasets`, computes manifests; no rewrites yet.
- **M2 — Schema audit**: Detect actual columns/types, propose `schema_version` v1 per dataset; capture drift report (2017→2025-03).
- **M3 — Pipeline prototype (1 day)**: Convert one day both datasets end-to-end (temp paths), write manifests, DB registration.
- **M4 — Bench & tune**: ZSTD levels {3,5,7,9}, row-group sizes {200k,500k,1M}; pick defaults.
- **M5 — API skeleton**: `/status/health`, `/files`, `/stats/daily`, `/discover/backfill/register` read-only.
- **M6 — Scheduling**: Cron for prior-day at 06:10 EST (post-availability check). PCAP cleanup after 30 days.
- **M7 — Acceptance**: Run golden-day tests; finalize Phase 1 sign-off.

---

## 5) Open Confirmations (please reply in-line here or update `plans-1-q.md`)

1) **File naming**: Approve `YYYYMMDD_IEXTP1_TOPS1.6.parquet` and `..._QuoteUpdate.parquet`. (YES)
2) **Monthly vs `DD/`**: You answered “no `DD/`”; confirm monthly-only `YYYY/MM/` is final (the `YYYY/MM/DD` mention looked accidental). (Monthly only, when creating example.env please put an arg that specifies the root folder for the files)
3) **OOO policy**: Approve sort by `(exchange_ts, seq_no, symbol)` and retain `wire_idx`. (yes)
4) **DB uniqueness**: Approve at least `UNIQUE(path)` in `files` table. (yes)
5) **Retries**: Approve minimal retries/timeouts for SMB/HTTP. (yes)
6) **Timezone**: Approve EST param handling with internal UTC mapping. (yes)
7) **Manifest**: Approve JSON keys proposed. (yes)
8) **.env keys**: Approve names above; share SMB roots when ready. (yes, there is no dsn for the smb share since it is already mounted per server)
9) **Staleness detection**: Prefer computing in analytics (not write-path)? (yes)

---

## 6) Risks & Mitigations

- **Corrupt/partial PCAP days**: Reasonableness checks + fail-fast with crisp logs; run status recorded, day marked for manual review.
- **Network share hiccups**: Timeouts + limited retries; atomic renames avoid partial files being read.
- **Schema drift vs backfill**: Schema audit + `schema_version` column; normalize minimally.
- **Large QuoteUpdate volume**: Event-driven L1 and compaction; performance tuning of ZSTD/row groups.

---

## 7) Phase 1 TODO (planning)

- Define file naming, manifest spec, OOO policy, uniqueness, retries, timezone semantics (M0).
- Draft `.env.example` contents (keys only, no secrets) for review.
- Specify schema audit approach and output format (report).
- Design DB tables (DDL sketch) and indexes by date; confirm constraints.
- Outline compression benchmark protocol and acceptance thresholds.
- Specify backfill scanner behavior and safeguards (read-only; no rewrites yet).

Update this doc with confirmations; I will iterate a v3 plan or start drafting specs once you approve M0 items.



# Important feature request mefore M0

please write a utility that can aggregate the confirmed trade parquet files into a master pq file per year. because it is an aggragate, do not delete the source pq files. what i wish to do is, based on a year i specify, i want you, day over day take the average price of each confirmed trade per symbol and per second of the trading day and write it to a new parquet file. the objective is to get an easy to use PQ file that has every ticker for further analysis. the partition on the pq should be tickers.