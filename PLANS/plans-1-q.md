# IEXScoper — Planning Questions (Phases 1–4)

Instructions: Please respond inline under each question with concise answers. Where applicable, choose an option or add details. If a question is not applicable, write N/A.

---

## 0) Meta & Objectives

- **Primary objective confirmation**: Is Phase 1 strictly “convert prior-day IEX TOPS/DEEP PCAP → Parquet (PyArrow) into a YYYY/MM/ folder layout, with idempotent reruns,” plus FastAPI + SQLAlchemy master DB of file metadata? Answer: Yes
- **Scope of exchanges/data**: Are we strictly IEX-only (TOPS/DEEP) for now? Any plan to add other venues later? Answer: No, IEX is the only free exchange that provides this.
- **Universe of symbols**: What symbol set should we process (all IEX-listed and traded, any exclusions)? How do we source/refresh symbol master? Answer: All IEX-listed and traded.
- **Trading sessions**: Include premarket/after-hours, auctions, halts? If exclusions, specify. Answer: All.
- **Time horizon**: Daily job for prior day only, or support ad-hoc historical ranges (rebuild periods)? Answer: Both.
- **Success criteria (Phase 1)**: What are the minimal acceptance checks (e.g., Parquet valid, schema validated, row counts vs message counts, API returns file paths)? Answer: Parquet Valid, Schema Validated, and a reasonableness test (there have been some edge cases where i wasn't able to fully parse the pcap because the file was corrupted and trades ended at 1pm EST)

## 1) Input Data (PCAP) & Ingestion

- **Source location**: Where do the daily PCAPs land (local FS path, NFS, S3, other)? Provide example path(s). Answer: smb server that is mounted so we should add that root path to .env in case
- **Arrival schedule**: When does the prior-day PCAP become available? Any SLA/variance? Answer: i don't know, we can write a very simple tester that pings the iex endpoint to see when it happens, but lets assume 6AM est?
- **File naming pattern (input)**: Provide exact patterns for TOPS and DEEP PCAPs (include version tags like “TOPS1.6”). Answer: yes, i will help you look through the iex endpoints
- **Compression/archival**: Are PCAPs compressed (gz/zst)? If encrypted, what method? Answer: gz, i will show you the custom pcap converter later
- **Data scale (daily)**: Approximate daily size (GB) and message counts for TOPS and DEEP separately. Answer: rougly min 5GB each + 10GB upper bound, but in days where there is an unbelievable amount of data it can be as high as 20GB like during the april 2025 tarrifs scandal
- **Multiple segments**: Can a single day have multiple PCAP shards per feed (e.g., TP1/TP2, rolling segments)? How to merge? Answer: No.
- **Clock fields**: Do messages contain both exchange timestamp and receipt timestamp? Any monotonic guarantees or seq numbers? Answer: i think so but we need to chech. i believe there are monotonic guarantees but we should sample and test.
- **Version variability**: Expect multiple schema versions (e.g., TOPS1.6 vs 1.7) across the backfill/new data? Answer: no its just 1.6
- **Existing backfill confirmation**: You noted backfill 2017→2025-03. Where does it live (path/bucket)? Can we read-only scan it to register into DB? Answer: its in the path that should be in .env (please build a .env.example)

## 2) Output Layout & File Naming (Parquet)

- **Canonical directory layout**: You proposed `YYYY/MM/` folders. Should we also include day granularity (`YYYY/MM/DD/`) to avoid very large monthly dirs? Answer: no, the backfilled folders provide enough clarity since we should really expect ~60 files per month.
- **Partitioning by ticker**: You believe existing files partition by ticker. Confirm target layout: folders by date then by dataset type then by symbol? Or a single file per day containing all symbols? Answer: YYYY/MM/DD/ is JUST for the .parquet file, the partitions are inside the file like how you would declare when creating it.
- **Per-day file split**: You mentioned two files per day in the final month folder: `TOPS1.6.parquet` and `TOPS1.6_QuoteUpdate.parquet`. Confirm the rule for all days/datasets. Answer: yes
- **Dataset taxonomy**: Enumerate output dataset types you want now (e.g., Trades/QuoteUpdate/BBO events/Other). Answer: no its just confirmed trades and quotes for now.
- **File naming (output)**: Confirm exact pattern. Example: `YYYYMMDD_IEXTP1_TOPS1.6.parquet` and `YYYYMMDD_IEXTP1_TOPS1.6_QuoteUpdate.parquet`. Any venue/segment in name? Answer: confirm file name
- **Atomic writes**: OK to write to temp path then atomic-rename to final (for idempotency)? Answer: yes
- **Manifests**: Do you want a `_SUCCESS` or manifest JSON per day/month for completeness checks? Answer: yes

## 3) Parquet Schema & Data Types

- **Schema per dataset**: Define required columns per event type (Trades, QuoteUpdate, BBO, etc.). Minimum fields for Phase 1? Answer: i need to check the schema inside the files first.
- **Price/size types**: Use decimals with scale (e.g., 1e-4) or integers in ticks? Provide desired scale. Answer: i need to check the schema inside the files first.
- **Timestamps**: Use `timestamp[us]` UTC? Do you want separate `exchange_ts` and `ingest_ts`? Answer: i need to check the schema inside the files first.
- **Symbol encoding**: Dictionary-encode `symbol`? Any uppercasing/normalization rules? Answer: i need to check the schema inside the files first.
- **IDs & sequencing**: Include `seq_no`, `event_id` (e.g., hash of ts+symbol+seq+type) for dedup/auditing? Answer: i need to check the schema inside the files first.
- **Message flags**: Include condition codes, trade qualifiers, quote flags (eg. non-displayed, retail, intermarket sweep)? Which ones? Answer: i need to check the schema inside the files first.
- **Venue fields**: Keep venue code even if IEX-only (future-proofing)? Answer: i need to check the schema inside the files first.
- **Nullability**: Any non-null constraints required (e.g., price/size cannot be null for trades)? Answer: i need to check the schema inside the files first.
- **Schema versioning**: Do you want a `schema_version` column and registry for evolutions? Answer: yes

## 4) Time, Ordering, and Quality

- **Time zone**: Treat all timestamps as UTC internally? Display/localization needs later? Answer: its UTC internally but you need to display EST and the querying should be in EST.
- **Ordering guarantees**: Should we enforce sort within files by (`exchange_ts`,`seq_no`,`symbol`)? Answer: not sure
- **Out-of-order handling**: If messages arrive OOO, do we sort or preserve wire order + include `wire_idx`? Answer: yes
- **Duplicates**: How to detect and handle duplicates (drop vs mark)? Answer: there shouldn't be duplicates
- **Crossed/locked filters**: Should we detect/label crossed/locked BBO episodes now or defer to analytics? Answer: not sure
- **Data validation**: Must-fail conditions (e.g., negative sizes, zero price) vs warn-and-continue? Answer: yes negative sizes and zero price should fail

## 5) Idempotency & Rerun Semantics

- **Rerun key**: Is idempotency keyed by (date, dataset_type, segment) so reruns produce identical file names/paths? Answer: yes
- **Existing outputs**: On rerun, overwrite, skip, or version-suffix (v2) if outputs already exist? Answer: overwrite
- **Partial outputs**: How to recover from partial day (some files done)? Detect via DB/manifests? Answer: yes
- **Checksums**: Store content hash/row counts in DB for idempotency verification? Answer: yes
- **Concurrency control**: Allow multiple workers per day/symbol or single-writer? Answer: yes

## 6) Performance, Compression, and Storage

- **Compression codec**: Preference for ZSTD vs Snappy vs None? Target compression level? Answer: ZSTD, not sure, we need to compute the best compression level for each file
- **Row group size**: Target rows per row group (e.g., 200k–1M) to balance scan vs write? Answer: not sure
- **Dictionary encoding**: Enable for categorical columns (symbol, condition codes)? Answer: not sure
- **File size targets**: Desired file size budget (e.g., 128–1024MB) for OLAP friendliness? Answer: not sure
- **Hardware budget**: Cores/RAM/IOPS available for daily batch? Answer: 20 cores, 16GB ram
- **Parallelism**: Max concurrency per machine (I/O vs CPU bound)? Answer: there should only be 2 files that should be processed daily
- **Storage**: Final storage is local, network share, or cloud bucket? Any lifecycle/retention policy? Answer: yes it is a network share, retain the pq forever but drop the pcap since it takes too much space

## 7) Master Database (SQLAlchemy) & Metadata

- **DB engine**: Postgres preferred? If not, which (SQLite for local, MySQL, etc.)? Answer: Postgres (THE DB IS NOT FOR THE TICK DATA BECAUSE MY DB WONT HAVE ENOUGH SPACE)
- **Connection details**: Will `.env` provide DSN? Any SSL requirements? Answer: yes, i will provide the .env
- **Tables**: Confirm entities to track: `datasets` (type, version), `files` (path, date, type, symbol optional, size, rows, min/max ts, hash), `runs` (status, started/ended, logs), `partitions`? Answer: yes
- **Uniqueness constraints**: e.g., unique on (date, type, segment [, symbol]) per file. Answer: no
- **Indexes**: Which query patterns to optimize (by date range, by symbol, by type)? Answer: by date range
- **Migrations**: Alembic for schema versioning OK? Answer: yes

## 8) API (FastAPI) Surface

- **Public endpoints (Phase 1)**: Which routes do you want now? Examples:
  - `/status/health`
  - `/files?date=YYYY-MM-DD&type=QuoteUpdate`
  - `/stats/daily?date=...`
  - `/discover/backfill/register`
  Provide your desired list. Answer: `/status/health`, `/files?date=YYYY-MM-DD&type=QuoteUpdate`, `/stats/daily?date=...`, `/discover/backfill/register`
- **Response shape**: Prefer JSON with path + metadata; any pagination needed? Answer: JSON with path + metadata
- **Long-running tasks**: Trigger reruns via API or keep batch as CLI-only for now? Answer: CLI-only
- **CORS/UI**: Any immediate UI consumption in Phase 1, or API-only? Answer: API-only

## 9) Reconciling Existing Backfill (2017 → 2025-03)

- **Registration**: Should we scan existing Parquet and register all files into the master DB (no rewrites) as a first step? Answer: yes
- **Layout alignment**: If existing layout deviates from target, do we (a) accept heterogeneity and map it in DB, or (b) re-pack to the new layout? Answer: (b)
- **Schema drift**: If historical schema differs, do we normalize minimally (add missing columns with nulls) or keep as-is with per-version schemas? Answer: yes
- **Audit**: Any must-have integrity checks during registration (e.g., no duplicate dates, min/max ts sanity)? Answer: yes

## 10) Order Book Engine (Phase 2)

- **Scope of reconstruction**: BBO only, L5, or L10? Start with L1 then iterate? Answer: BBO only since i have IEX TOPS data but would want to ingest DEEPS in the future but that will take a lot more space
- **Resolution strategy**: Confirm preferred base:
  - Event-driven L1 for best bid/ask changes
  - Event-driven L5/L10 (delta-encoded)
  - Time-sliced (e.g., 10ms/50ms) with last-state compaction
  Choose defaults. Answer: yes
- **Delta encoding**: OK to store snapshots as “prev + deltas” for efficiency? Answer: yes
- **Replay alignment**: Need exact book state at each trade (`t−`, `t+`) for analytics—should we precompute alignment indices? Answer: yes
- **Queue position proxies**: Compute during reconstruction or defer to analytics jobs? Answer: yes 
- **Staleness detection**: Where to compute (engine vs analytics)? Answer: which is better?
- **Persistence**: Persist reconstructed book to Parquet/Arrow or generate on-the-fly from QuoteUpdate? Answer:  persist to parquet if it doesnt take too much space, otherwise generate on the fly (we should test this first)
- **Backpressure**: If UI requests a heavy period, should server throttle or stream partial segments? Answer: stream parallel

## 11) Web UI (FastAPI + Next.js)

- **Transport**: WebSockets for live/replay streams, or HTTP chunked responses? Answer: WebSockets
- **Controls**: Must-have controls (play/pause, speeds, jump to next print, lock to trade events)? Answer: all of these
- **Default levels**: L1, L5, L10 toggles; default level per liquidity bucket? Answer: L1
- **Scrollback**: Cap scrollback to 30–60s by default with lazy-load older slices? Answer: yes, 30s
- **Visuals**: Heatmap/ladder preferences, color scheme, accessibility needs (dark mode)? Answer: dark mode

## 12) Analytics (Phase 3–4)

- **Computation timing**: Precompute nightly or compute on-demand? Which metrics must be cached? Answer: precompute nightly
- **Key study priorities**: From the list (effective/realized spread, impact, fill probability, iceberg heuristics, etc.), which 3–5 are top priority to prototype first? Answer: effective/realized spread, impact, fill probability, iceberg heuristics
- **Windows**: Default Δt windows (e.g., +100ms, +1s, +5s) to ship initially? Answer: +100ms, +1s, +5s
- **Bucketing**: Spread ticks buckets (1,2,≥3), imbalance buckets, size buckets—confirm exact bins. Answer: 1,2,≥3
- **Trade classification**: Use price vs prior mid for aggressor tagging initially? Any alternative rules? Answer: price vs prior mid
- **Quality rules**: How to treat crossed/locked periods in analytics (exclude vs tag)? Answer: exclude
- **Odd-lot handling**: Include odd-lots in all stats or separate lanes? Answer: in all stats

## 13) Infra, Tooling, and Dev Process

- **Python environment**: Confirm we standardize on `uv` runner per global rules. Answer: uv
- **Minimum Python version**: What version should we target (e.g., 3.11/3.12)? Answer: 3.12
- **Dependencies**: PyArrow required. Is Polars acceptable for transforms if IO is Arrow-backed? Answer: yes
- **Orchestration**: For later: cron vs systemd timer vs Airflow—preference? Answer: cron
- **Observability**: Adopt dual-sink logging (Rich console + JSONL to `logs/app.jsonl`) with startup enforcer per rules? Answer: yes
- **Metrics**: Any preference (Prometheus) or keep simple counters in logs initially? Answer: no
- **Storage budget**: Any hard cap or lifecycle rules (e.g., delete raw PCAPs after N days)? Answer: delete raw PCAPs after 30 days

## 14) QA, Tests, and Acceptance

- **Golden samples**: Provide 1–2 specific dates/tickers to use as golden test days for correctness/perf. Answer: 2025-01-01 and 2025-02-01 (use INTC, AMD)
- **Acceptance tests**: Define must-pass checks (row counts, ts monotonicity, schema fields present, successful DB registration). Answer: yes all
- **Benchmarks**: Target throughput (GB/hour) and max runtime for daily job? Answer: i am not sure, the last time i ran these jobs i think it was roughly 2 hours/file, i need to benchmark
- **Error handling**: Fail-fast vs continue with per-file errors and report? Answer: fail fast
- **Retry policy**: Backoff and cap for transient I/O? Answer: no?

## 15) Security & Access

- **Network exposure**: Even without auth, will the API run behind VPN/firewall? Answer: behind VPN
- **Secrets**: `.env` for local only; any production secret store to plan for? Answer: no
- **PII/Compliance**: Any compliance constraints for storage/processing locations? Answer: no

## 16) Roadmap & Milestones

- **Phase 1 delivery**: What is the target milestone breakdown (Parser + Writer + DB registration + API)? Answer: its all
- **Phase 2 delivery**: Minimal viable playback (L1 event-driven?) plus UI shell. Answer: lets start with OBO and L1
- **Phase 3 priority**: List top analytics to ship first. Answer: effective/realized spread, impact, fill probability, iceberg heuristics
- **De-risking spikes**: Any spikes we should schedule early (e.g., schema version normalization, compression benchmarking)? Answer: compression benchmarking

---

Append any additional constraints, examples, or sample files below. Thank you!
