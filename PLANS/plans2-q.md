# IEXScoper — Leading Questions: Yearly Per‑Second Trade Aggregator

Based on `PLANS/plans2.md` confirmations and your feature request. I propose defaults inline; please confirm or adjust. [CA][PA][REH][CMV]

---

## 1) Scope & Semantics

- **[dataset]** Confirm source is IEX TOPS 1.6 Trades only (exclude QuoteUpdate for this job). Default: Trades only.
- **[definition: confirmed trade]** Which trade flags/conditions qualify? Default: include all executed prints except cancels/corrections; include odd‑lots (per Phase 2 analytics note: include odd‑lots). Please confirm. (yes)
- **[sessions]** Include pre/regular/after‑hours and auction/halts? Default: include all sessions; add a `session` column for downstream filtering. (yes)

## 2) Time & Binning

- **[timestamp to bucket]** Use `exchange_ts` as the authoritative timestamp? Default: bucket by `exchange_ts`.
- **[timezone]** “Per second of the trading day” — bin in `America/New_York` or UTC? Default: bin in NY time (DST‑aware), store UTC boundary columns for joins.
- **[binning rule]** Second boundary = floor to second. OK? Any need for alignment offset? Default: floor.
- **[no‑trade seconds]** Emit rows for seconds with zero trades? Options: (option A)
  - A) emit none (sparse) — Default
  - B) emit rows with `avg_price = null`, `trade_count = 0`

## 3) Aggregation Logic

- **[measure]** “Average price per second” — arithmetic mean or volume‑weighted (VWAP second)? Default: VWAP per second.
- **[price field]** Which field to average? Default: raw trade price from Trades schema; no corporate‑action adjustments.
- **[filters]** Exclude non‑regular prints or outliers? Default: no special exclusions beyond cancels/corrections; we’ll surface counts so you can filter later.
- **[outputs per second]** Proposed columns:
  - `symbol, ts_second_ny, ts_second_utc, session, vwap, mean_price, trade_count, share_volume`
  - Keep both `vwap` and `mean_price` for flexibility. Accept? (yes)

## 4) Output Grain & Year Semantics

- **[row grain]** Each row = (symbol, day, second) aggregate, then all days for the year appended into one yearly dataset. Confirm this (vs a single cross‑day average per second). (yes)
- **[year scope]** Year = calendar year in NY time. Confirm. (yes)

## 5) Layout, Naming, Partitioning

- **[dataset vs single file]** You asked for “master PQ file per year” and “partition on tickers”. Partitioning implies a Parquet dataset (many files). Which do you prefer?
  - A) Parquet dataset folder per year, partitioned by `symbol` — Default (yes)
  - B) Single monolithic file (no partitioning)
- **[path spec]** Proposal:
  - Root: `${IEX_PARQUET_ROOT}/derived/trades_per_second/year=${YYYY}/`
  - Partition: `symbol=XXXX/part-*.parquet`
  - Confirm path and naming.
- **[file naming]** If we must produce a single file: `YYYY_IEX_TOPS1.6_trades_persec.parquet`. Confirm only if choosing single‑file mode. (yes)

## 6) Performance & Engine

- **[engine]** Prefer Polars for groupby+binning with streaming, write via PyArrow. OK? Default: Polars+PyArrow.
- **[row group & codec]** Use ZSTD level=5, row group ≈ 500k rows, dict‑encode `symbol` and small enums. Confirm. (yes)
- **[throughput strategy]** Daily aggregates → append to yearly dataset incrementally (idempotent per day). OK? (yes)
- **[resources]** Any memory/CPU limits or target runtime per year? Provide constraints if any. (16GB ram)

## 7) Idempotency, Manifests, DB

- **[idempotency key]** Yearly dataset idempotent by `(year, dataset_type='trades_per_second', filter_version)`. Reruns overwrite affected partitions atomically. Confirm. (yes)
- **[temp→atomic rename]** Write to temp path then atomic rename. Confirm. (yes)
- **[manifest]** Emit yearly manifest JSON with: `year, dataset_type, path, files, rows, min_ts, max_ts, hash_xx64, codec, zstd_level, created_at`. Confirm keys. (yes)
- **[DB registration]** Register dataset/files in Postgres `datasets`/`files` tables with `UNIQUE(path)`. Confirm. (yes)

## 8) Scheduling & Ops

- **[when to run]** Nightly post M3/M6 pipeline (e.g., 06:30–07:00 ET) after day ingestion. Confirm window. (yes)
- **[recovery]** On failure, retry per global rules (10s timeouts, 3 retries with backoff+jitter). Confirm. (yes)
- **[logging]** Dual‑sink logging with startup enforcer; structured fields include `event='aggregate_per_second'`, `year`, `symbol`, `day`. Confirm. (yes)

## 9) QA & Validation

- **[sanity checks]** Per day/symbol validate: `trade_count>0` ⇒ `vwap>0`, `mean_price>0`. Tolerate zero rows when no trades. (yes)
- **[coverage report]** Emit metrics by day: symbols processed, rows/sec density, top N sparse symbols. Include in manifest. Confirm. (yes)
- **[tests]** Add golden tests on sample days (e.g., `INTC`, `AMD`) to verify binning and VWAP. OK? (yes)

## 10) Extras (Optional Now)

- **[session splits]** Also emit per‑session aggregates (separate datasets or a `session` column only)? Default: column only. (yes)
- **[symbol mapping]** Any normalization (e.g., handle historical renames)? Default: raw IEX symbol, no mapping. (no)
- **[downstream]** Any consumers needing a row for every second (dense) or current sparse output is fine? Default: sparse. (sparse)

---

Reply inline here; I’ll lock defaults and convert into a spec + task breakdown. [RAT]
