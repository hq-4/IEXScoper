Remaining items to finalize
[sale-condition filter set] Provide exact strings to exclude cancels/corrections from "Sale Condition". Example candidates to confirm or replace:
Exclude: CANCEL, CORRECTION, CORR
Keep: ODD_LOT, EXTENDED_HOURS
I’ll codify as filter_version='v1' once you list the exact tokens.
(agree with these selections)

[session windows] Confirm NY session boundaries for session column:
pre: [04:00, 09:30)
regular: [09:30, 16:00)
after: [16:00, 20:00)

(agree with these selections)

[staging retention] Default proposal: keep 7 days after successful compaction, then prune. Confirm keep/prune and duration. (agree)
[dedupe policy] Any risk of duplicate prints across inputs? If yes, dedupe by (Trade ID, Exchange Timestamp, Symbol); else we skip. Confirm. (agree)
[master file size tolerance] If a single yearly Parquet becomes too large for common tools, OK to support mid-year compactions (e.g., H1/H2) as an option? (no)