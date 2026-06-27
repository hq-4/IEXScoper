# Changelog

## Unreleased

- feat: add standalone IEX parser parity benchmark harness and report generator `[CA][PA][CDiP]`
- feat: add HIST index parser and bounded `IEXTools` TOPS backfill workflow `[CA][RM][PA][CDiP]`
- feat: quarantine unknown parser message types with threshold-based runner failure `[REH][PA][CDiP]`
- feat: add IEXTools day-level parse/normalize debug probe for missing-day RCA `[CA][REH][PA]`
- feat: add backfill failure classification, summary generation, and resume/retry artifacts `[REH][CDiP][PA]`
- feat: retry corruption-style backfill day failures after scratch cleanup and fresh HIST link refresh `[REH][RM][PA][CDiP]`
- feat: add TOPS symbol stability audit for ticker-era continuity classification `[CA][KBT][PA][CDiP]`
- feat: add symbol-era outputs that split ticker observations at major gaps for point-in-time analysis keys `[CA][KBT][PA][CDiP]`
- feat: add resumable daily confirmed-trade OHLCV bars keyed by symbol era `[CA][PA][RM][CDiP]`
- feat: add stable long-window ticker-era universe report from confirmed daily bars `[CA][PA][KBT][CDiP]`
- feat: add stable universe daily-bar quality report before backtest panel construction `[REH][PA][KBT][CDiP]`
- feat: add cached OpenFIGI enrichment for symbol-stability review triage `[CA][IV][REH][SFT][PA][CDiP]`
- feat: add local IEX entity snapshot diff and enrichment layer for ticker-era listing evidence `[CA][KBT][PA][CDiP]`
- feat: add stable confirmed-trade daily panel with entity metadata and quality flags `[CA][PA][KBT][CDiP]`
- feat: add stable daily panel validation report for structural contract checks `[REH][PA][CDiP]`
- feat: add stable raw returns table with clean/dirty return flags `[CA][PA][KBT][CDiP]`
- feat: add streaming IEXTools price-column repair utility for pre-fix Parquet outputs `[REH][RM][PA][CDiP]`
- docs: record complete HIST TOPS day coverage through 2026-06-22 and note unreadable parquet follow-up `[CDiP][KBT]`
- fix: raise unknown-message quarantine thresholds for TOPS forward-compatibility and enable faulthandler in the runner `[REH][PA]`
- fix: align backfill unknown-message defaults with the hardened runner and add explicit failed-gz cleanup/retention controls `[REH][RM][PA][CDiP]`
- fix: extract UDP payload streams from pcap-ng inputs before IEXTools parsing and fail zero-message parses explicitly `[CA][REH][PA][CDiP]`
- fix: support TOPS 1.5 backfill inputs by extracting classic pcap UDP payloads and passing the HIST protocol version into IEXTools `[CA][REH][PA][CDiP]`
- fix: serialize parallel backfill HIST refreshes and write HIST index downloads atomically `[REH][RM][PA][CDiP]`
- fix: preserve hq-4 computed slot price fields during normalization so float price columns are populated with lossless values `[REH][PA][CDiP]`
- fix: prefer full-size duplicate TOPS HIST records and patch hq-4 TOPS 1.5 TradeBreak decoding `[REH][KBT][PA][CDiP]`
- fix: skip unreadable Parquet days during symbol-stability reporting and list them in outputs `[REH][KBT][CDiP]`
- fix: add explicit TOPS Parquet replacement repair mode with scratch headroom checks and atomically repair nine unreadable published main Parquet days `[REH][RM][CA][CDiP]`
- fix: regenerate seven latent corrupt main Parquet days blocking confirmed-trade daily bars `[REH][PA][KBT]`
- fix: preserve runner exception details and classify parser short-buffer failures `[REH][CDiP]`
- docs: record backfill parser RCA and recommend transport-aware parser replacement path `[REH][AS][CDiP]`
