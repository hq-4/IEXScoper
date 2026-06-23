# Changelog

## Unreleased

- feat: add standalone IEX parser parity benchmark harness and report generator `[CA][PA][CDiP]`
- feat: add HIST index parser and bounded `IEXTools` TOPS backfill workflow `[CA][RM][PA][CDiP]`
- feat: quarantine unknown parser message types with threshold-based runner failure `[REH][PA][CDiP]`
- feat: add IEXTools day-level parse/normalize debug probe for missing-day RCA `[CA][REH][PA]`
- feat: add backfill failure classification, summary generation, and resume/retry artifacts `[REH][CDiP][PA]`
- feat: retry corruption-style backfill day failures after scratch cleanup and fresh HIST link refresh `[REH][RM][PA][CDiP]`
- feat: add TOPS symbol stability audit for ticker-era continuity classification `[CA][KBT][PA][CDiP]`
- feat: add cached OpenFIGI enrichment for symbol-stability review triage `[CA][IV][REH][SFT][PA][CDiP]`
- feat: add streaming IEXTools price-column repair utility for pre-fix Parquet outputs `[REH][RM][PA][CDiP]`
- fix: raise unknown-message quarantine thresholds for TOPS forward-compatibility and enable faulthandler in the runner `[REH][PA]`
- fix: align backfill unknown-message defaults with the hardened runner and add explicit failed-gz cleanup/retention controls `[REH][RM][PA][CDiP]`
- fix: extract UDP payload streams from pcap-ng inputs before IEXTools parsing and fail zero-message parses explicitly `[CA][REH][PA][CDiP]`
- fix: support TOPS 1.5 backfill inputs by extracting classic pcap UDP payloads and passing the HIST protocol version into IEXTools `[CA][REH][PA][CDiP]`
- fix: serialize parallel backfill HIST refreshes and write HIST index downloads atomically `[REH][RM][PA][CDiP]`
- fix: preserve hq-4 computed slot price fields during normalization so float price columns are populated with lossless values `[REH][PA][CDiP]`
- fix: prefer full-size duplicate TOPS HIST records and patch hq-4 TOPS 1.5 TradeBreak decoding `[REH][KBT][PA][CDiP]`
- fix: skip unreadable Parquet days during symbol-stability reporting and list them in outputs `[REH][KBT][CDiP]`
- fix: preserve runner exception details and classify parser short-buffer failures `[REH][CDiP]`
- docs: record backfill parser RCA and recommend transport-aware parser replacement path `[REH][AS][CDiP]`
