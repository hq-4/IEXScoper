# Changelog

## Unreleased

- feat: add standalone IEX parser parity benchmark harness and report generator `[CA][PA][CDiP]`
- feat: add HIST index parser and bounded `IEXTools` TOPS backfill workflow `[CA][RM][PA][CDiP]`
- feat: quarantine unknown parser message types with threshold-based runner failure `[REH][PA][CDiP]`
- feat: add IEXTools day-level parse/normalize debug probe for missing-day RCA `[CA][REH][PA]`
- feat: add backfill failure classification, summary generation, and resume/retry artifacts `[REH][CDiP][PA]`
- feat: retry corruption-style backfill day failures after scratch cleanup and fresh HIST link refresh `[REH][RM][PA][CDiP]`
- fix: raise unknown-message quarantine thresholds for TOPS forward-compatibility and enable faulthandler in the runner `[REH][PA]`
- fix: preserve runner exception details and classify parser short-buffer failures `[REH][CDiP]`
- docs: record backfill parser RCA and recommend transport-aware parser replacement path `[REH][AS][CDiP]`
