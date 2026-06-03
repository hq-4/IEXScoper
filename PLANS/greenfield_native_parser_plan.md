# Greenfield Native IEX Parser Plan

## Context

This note captures the side-session discussion about whether a new native IEX parser and Parquet writer path should be built instead of relying on the currently benchmarked Python repos.

## Main Conclusion

A greenfield native implementation can make sense as a long-term ingestion core, but it is not the first thing to do if the immediate goal is simply to choose a replacement parser now.

Short-term:
- Finish the current parity benchmark.
- Use that benchmark to choose the best interim replacement path.
- Likely favor `hq-4/IEXTools` if it continues to win on both throughput and parity.

Long-term:
- A native parser is strategically reasonable if the system will ingest large historical and ongoing IEX TOPS/DEEP archives for years.

## Updated Trigger For Native Work

The later bounded backfill run changed the urgency of this recommendation.

Observed issue:
- `hq-4/IEXTools` remained the fastest benchmarked Python parser.
- But on later 2025-2026 TOPS days it failed on `ProtocolException: Unknown message type: (...)`.
- The failing bytes were not stable. Logged values included `0`, `42`, `45`, `49`, `54`, `64`, `92`, `161`, `173`, `221`, and `244`.

Interpretation:
- This does not look like one clean new TOPS message that simply needs one additional decoder branch.
- It looks more like parser desynchronization after losing framing on the raw stream.

Practical implication:
- A greenfield parser is no longer just a long-term performance idea.
- It is now also the cleanest path to a transport-correct and failure-tolerant historical backfill engine if the current Python parser cannot be hardened quickly enough.

## Why A Native Rewrite Could Make Sense

- The workload is dominated by byte parsing, framing, transport decoding, and sequential transforms.
- Python overhead is expensive in this hot path:
  - packet abstraction layers
  - dict allocation
  - `Decimal`
  - `datetime`
  - repeated field conversion
- A native implementation would allow:
  - direct integer timestamp and price handling
  - tighter memory control
  - predictable buffering
  - more efficient streaming pipelines
  - direct Arrow/Parquet batch emission

## Why Not Do It First

- Correctness and parity are harder than raw speed.
- Existing repos already encode a lot of protocol behavior and failure cases.
- A rewrite still needs a full validation harness.
- The benchmark/parity work does not go away just because the implementation language changes.

## Language Recommendation

Preferred order:

1. Rust
2. C++20
3. C++17 only if toolchain constraints require it

Rationale:

- Rust is the best default if safety, concurrency, and long-term maintainability matter.
- C++20 is a strong option if there is already strong C++ experience and infrastructure.
- C++26 is not a sensible default target for a greenfield production codebase unless deployment and toolchains are tightly controlled.

## Naive Speedup Expectations

Very rough expectations:

- Versus a slow Python + Scapy style path:
  - `8x` to `20x` is plausible
- Versus a cleaner Python binary parser like `IEXTools`:
  - `2x` to `6x` is a more realistic expectation
- End-to-end pipeline speedup including Parquet writing:
  - roughly `3x` to `8x` may be a realistic planning range if the pipeline is well designed

The exact upside depends on whether the current bottleneck is:
- Python object churn
- gzip decompression
- network I/O
- Parquet writing

## Arrow / Parquet Tooling

A native rewrite should not reimplement Arrow or Parquet encoders.

Custom code should cover:
- `.pcap.gz` streaming
- transport framing
- TOPS/DEEP message decode
- schema mapping
- batching logic

Existing tooling should handle:
- Arrow array construction
- record batch layout
- Parquet encoding
- compression codecs
- row group writing

Practical architecture:
- native parser core
- decode directly into compact integer-first structs
- build Arrow record batches
- write Parquet through existing Arrow/Parquet libraries

## Why `IEXTools` Looks Faster So Far

The benchmark discussion suggested that `hq-4/IEXTools` is dramatically faster than `rob-blackbourn/iex_parser` because:

- `IEXTools` parses raw bytes more directly using `struct`
- `rob-blackbourn/iex_parser` pays Scapy overhead
- `rob-blackbourn` creates heavier Python objects earlier
- `IEXTools` exposes more parity-friendly native fields such as integer timestamps and integer prices

Important caveat:
- faster does not automatically mean more correct
- final parity and message-coverage checks still matter

## What Testing Should Look Like

If a greenfield parser is built, testing and failure instrumentation should come first.

### 1. Spec-level unit tests

- one test per message type
- explicit binary payload fixtures
- expected field decode
- minimum/maximum length handling
- unknown/appended-field tolerance

### 2. Golden sample tests

- small known-good pcaps
- expected decoded message sequences
- expected message-family counts
- expected first/last records
- expected Parquet schema and metadata

### 3. Differential tests

Compare:
- native parser output
- existing parser outputs
- immutable reference Parquet outputs

Check:
- quote vs non-quote split
- row counts by type
- field equality on sample rows
- timestamp handling
- schema compatibility

### 4. Failure localization

For days that fail:
- isolate packet number / stream offset
- log:
  - file
  - packet index
  - stream offset
  - declared message length
  - message type
  - transport header data
  - surrounding bytes in hex
- classified reason code

This is the key missing debug loop for corrupted or non-conforming days.

Additional requirement from the observed `IEXTools` failures:
- distinguish `unknown-but-well-framed message` from `lost framing / corrupt length`
- unknown message types should be captured as structured artifacts instead of aborting the whole day immediately
- hard failure should be threshold-based when unknowns or framing faults exceed a safety limit

### 5. Fuzz / property tests

- malformed lengths
- truncated payloads
- bad gzip streams
- unknown message types
- appended fields
- partial transport frames

The parser should fail with structured classified errors instead of crashing or silently misparsing.

### 6. Archive regression suite

Keep a set of historically problematic days and rerun them on every parser revision.

Each failure should be classified as one of:
- parser bug
- archive corruption
- recoverable malformed packet
- unsupported spec growth

## How AI Agents Help Now

Modern agents can help materially if the problem is framed correctly.

Good workflow:

1. Provide the IEX transport and TOPS/DEEP spec PDFs
2. Provide the exact failing `.pcap.gz` day
3. Provide structured error logs and byte dumps
4. Have the agent correlate failure offsets to the spec
5. Patch decoder logic and tests
6. Rerun regression days

Agents can help with:
- extracting message layouts from PDFs
- generating decoder tests from spec tables
- diffing parser outputs
- explaining mismatches against the spec
- helping localize parser failures to the smallest offending payload

Agents do not remove the need for:
- deterministic tests
- structured failure artifacts
- differential validation against trusted references

## Recommended Native Architecture

- parse PCAP records explicitly instead of scanning the decompressed byte stream for a header signature
- extract UDP payloads explicitly before transport parsing
- decode IEX transport frames incrementally from validated packet boundaries
- parse messages into compact structs using integer fields
- separate parser, normalization, and Arrow/Parquet emission stages
- emit batches instead of row-by-row objects
- keep a strict mode and a salvage/recovery mode

Why this matters:
- the current `IEXTools` approach hard-codes a byte-pattern search for the IEX-TP header, which is fast but brittle
- a transport-aware parser can recover from malformed packets, classify framing faults correctly, and quarantine forward-compatibility events without treating random body bytes as new message types

## Recommended Next Steps

1. Finish the current benchmark and parity report.
2. Decide whether `IEXTools` is good enough as an interim replacement.
3. If native rewrite is still justified, design the test and instrumentation harness first.
4. Choose Rust unless existing C++20 infrastructure clearly dominates.
5. Build a minimal parser for one day and one feed variant before attempting full protocol coverage.
