# Dead Ticker Resolution Workflow

## Evidence-Delta V2 Program

The canonical workflow is now fact-oriented and dry-run first. It snapshots the complete
`26,184`-era review population and stores identity, event, observation, resolver-attempt,
and research-decision records independently under `data/resolution/`. A research closure
never implies a verified identity or event, and no single resolved flag can hide a gap.

Run the network-capable dry run, then apply the exact completed stage without issuing new
requests:

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/run_dead_ticker_resolution_program.py

SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/run_dead_ticker_resolution_program.py --apply
```

For migration, cached rescoring, queue generation, and reconciliation without public-source
requests, use `--local-only`; its matching apply command is `--apply --local-only`. Local and
network stages are deliberately distinct, so a completed local audit never suppresses a later
network dry run. The default network budget is `25,000`, timeout is ten seconds, delay is
`0.25s`, and retry/circuit behavior is bounded.

Canonical files:

- `identity_facts.jsonl`: date-scoped issuer/entity/instrument identity evidence.
- `event_facts.jsonl`: verified events and explicitly separate `event_candidate` leads.
- `observation_facts.jsonl`: local-feed boundaries, continuity, and gap explanations.
- `research_decisions.jsonl`: independent identity, instrument, event, observation, and
  research statuses.
- `resolver_attempts.jsonl`: harvested V1 closures plus resolver/evidence fingerprints.
- `historical_ticker_identities_projection.csv`: backward-compatible rows only when both
  identity and event facts are verified.

The initial migration preserves all `364` historical identities, splits their events into
`127` verified facts and `237` candidates, promotes `454` SEC-verified identity-only holds,
keeps `3,916` market-data artifacts plus `1,778` parent links as research closures, and marks
all `5,659` lifecycle rows attempted under `lifecycle_v1`. Cached V2 semantic date rescoring
currently adds `18` evidence-gated event facts without asserting events for the remaining
known identities.

Evidence authority is deliberately bounded: exact date-scoped SEC `display_names` or
Inline XBRL `dei:TradingSymbol`, FINRA Daily List, NasdaqTrader, NYSE notices, and issuer-hosted
or SEC-attached releases. SEC ticker selection is discovery only. Syndicated releases,
text-only mentions, multiple CIKs, interval conflicts, and prospective symbol-change language
without post-effective same-CIK confirmation remain held.

The resolver searches annual SEC slices near the observed boundaries with pagination and a
shared negative cache. Historical negatives remain immutable until the resolver version
changes; recent eras may refresh after 30 days and the current directory after 24 hours.
An unchanged `(symbol_era_id, resolver, resolver_version, evidence_fingerprint)` is never run
twice. A strategy stops only after the minimum sample and two consecutive batches fall below
both one verified fact per 1,000 requests and `0.01%` incremental lane trade impact.

Reports under `reports/dead-ticker-review/resolution-v2/` include independent identity,
event, instrument, observation, and research-action queues plus a reconciliation summary.
Legacy closures are omitted from the research-action queue but remain visible in fact-gap
queues. Local reconciliation currently forms `128` parent-CIK/action-window derivative groups
but imports no child without exact ticker/security-class evidence. If bounded public-primary
coverage later plateaus, a licensed historical security master remains the explicit
higher-coverage alternative. [CA][REH][SFT][PA][KBT][AS]

Live run note: the SEC-backed dry run using the approved User-Agent stopped at the
`network_transport_circuit_breaker` for SEC EFTS after `7,033` source requests and `10`
cache hits. The completed stage was applied, adding 30 verified events and 2 verified
identities. The applied canonical summary now reports `820` identities, `412` events,
`26,184` observations, and `26,184` research decisions. A second unchanged dry run made
zero new source requests and added no duplicate facts. [REH][RM][KBT]

This workflow turns the unresolved ticker-era queue into auditable manual identity overrides.

## Goal

Resolve high-impact `historical_identity_unresolved` rows by proving the historical issuer and terminal event for a specific `symbol_era_id`.

Manual overrides must stay keyed by `symbol_era_id`, not raw ticker text. Reused tickers can represent unrelated issuers across time.

## Generate The Worklist

Refresh the unresolved priority queue:

```bash
uv run python utils/build_dead_ticker_priority_queue.py
```

Create a fillable research template from the top priority rows:

```bash
uv run python utils/build_dead_ticker_resolution_template.py --limit 100
```

This writes:

- `reports/dead-ticker-review/manual_resolution_template.csv`

The template carries rank, symbol-era metadata, current SEC/IEX hints, and blank proposed override fields.

## Instrument Heuristic Audit

The dead-ticker review queue also writes a local-only first-pass instrument audit:

- `reports/dead-ticker-review/instrument_heuristic_audit.csv`
- `reports/dead-ticker-review/instrument_heuristic_audit_summary.json`

The audit keeps the backward-compatible `instrument_hint` column and adds `instrument_type`
plus `instrument_reason`. The richer type separates likely preferreds, warrants, units,
rights, dot-suffix share classes, funds/trusts, operating companies, and ambiguous
patterns before downstream SEC research chooses an evidence path.

These labels are heuristics from ticker syntax, IEX product hints, and issuer-name hints.
They do not prove issuer identity or final disposition.

The review queue and manual template also carry routing fields:

- `research_route`
- `recommended_evidence`
- `routing_reason`

Current route meanings:

- `operating_company_sec_event`: use operating-company SEC/event evidence such as 8-K, merger proxy/S-4, 25-NSE, 15-12B, or issuer/acquirer release.
- `fund_or_trust_closure`: use fund liquidation, closure, merger, trust termination, or sponsor notice evidence.
- `preferred_redemption_or_delisting`: use preferred redemption, exchange, delisting, prospectus, or issuer notice evidence.
- `warrant_unit_right_security_action`: use warrant/unit/right redemption, separation, expiration, or SPAC action evidence.
- `share_class_corporate_action`: use share-class rename, ADR/common share conversion, merger, or delisting evidence.
- `manual_syntax_review`: inspect the symbol syntax before choosing a research path.

## EDGAR Lookup Lead

Use EDGAR as a lead source, not as final proof. The SEC ticker directory is current-biased, so it can miss dead tickers or point at a reused ticker.

Run a current ticker/CIK lookup for the template symbols:

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/lookup_edgar_tickers.py
```

To include recent filing metadata for current CIK matches:

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/lookup_edgar_tickers.py --fetch-submissions
```

Outputs:

- `reports/dead-ticker-review/edgar-lookup/edgar_ticker_lookup.csv`
- `reports/dead-ticker-review/edgar-lookup/edgar_ticker_lookup.parquet`
- `reports/dead-ticker-review/edgar-lookup/edgar_ticker_lookup_summary.json`

The script requires `--user-agent` or `SEC_USER_AGENT` before any SEC request. Do not use a fake contact string.

If the current ticker directory misses the batch, run the broader EDGAR full-text lead search:

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/search_edgar_full_text.py
```

This writes:

- `reports/dead-ticker-review/edgar-full-text/edgar_full_text_leads.csv`
- `reports/dead-ticker-review/edgar-full-text/edgar_full_text_leads.parquet`
- `reports/dead-ticker-review/edgar-full-text/edgar_full_text_raw.jsonl`
- `reports/dead-ticker-review/edgar-full-text/edgar_full_text_summary.json`
- `reports/dead-ticker-review/edgar-full-text/edgar_full_text_search.jsonl`

Full-text hits are noisy leads, especially for short tickers. Verify the CIK, issuer, filing date, and event before importing an override.

The search log is fresh per run by default so stale `ERROR` rows from older failed attempts do not pollute status checks. Pass `--append-log` only when cumulative JSONL history is intentional.

The full-text helper mirrors the stable SEC search page request shape: event terms in `q`, ticker text in `entityName`, `dateRange=custom` with era date bounds, and no custom `size` parameter. By default it does not send a `forms` filter because SEC EFTS has repeatedly returned 500s for form-filtered ticker/date searches. Pass `--use-form-filter` only for targeted probes where the narrower filing set is worth the extra SEC failure risk. The helper retries transient SEC 5xx responses and transport failures at debug level, then tries less brittle request variants: without forms, without date bounds, issuer aliases from `data/manual_overrides/edgar_full_text_aliases.csv`, ticker `AND` event terms directly in `q`, and finally quoted ticker text directly in `q`. Zero-hit responses also advance to the next variant so historical tickers like `XLNX` can resolve through issuer aliases like `Xilinx`. If all request shapes fail, the row is retained as `search_error`, the console logs a warning instead of a traceback, and the batch continues.

The default full-text pass uses `q=merger` only. Avoid broad `OR` queries across many event terms; SEC EFTS has returned repeated 500s for that shape. Run separate passes when needed, for example:

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/search_edgar_full_text.py --event-terms acquisition --output-root reports/dead-ticker-review/edgar-full-text-acquisition
```

After full-text collection, reduce the noisy raw hit set into a compact local triage file:

```bash
uv run python utils/edgar_full_text_triage.py
```

This writes:

- `reports/dead-ticker-review/edgar-full-text/edgar_full_text_triage.csv`
- `reports/dead-ticker-review/edgar-full-text/edgar_full_text_triage.parquet`
- `reports/dead-ticker-review/edgar-full-text/edgar_full_text_triage_summary.json`

The triage reducer is local-only. It ranks hit rows by filing form strength, filing date relation to the ticker era, issuer/entity text match, and original SEC hit rank. Buckets are `high_confidence_lead`, `medium_confidence_lead`, and `manual_review_lead`; they are prioritization labels only, not verified identity evidence. Short symbols still need manual issuer and CIK review because ticker text can match unrelated filings.

For separate event-term runs, pass explicit paths:

```bash
uv run python utils/edgar_full_text_triage.py \
  --leads-path reports/dead-ticker-review/edgar-full-text-acquisition/edgar_full_text_leads.parquet \
  --output-csv reports/dead-ticker-review/edgar-full-text-acquisition/edgar_full_text_triage.csv \
  --output-parquet reports/dead-ticker-review/edgar-full-text-acquisition/edgar_full_text_triage.parquet \
  --summary-path reports/dead-ticker-review/edgar-full-text-acquisition/edgar_full_text_triage_summary.json
```

To turn top-ranked triage rows into a review-ready override candidate file:

```bash
uv run python utils/build_dead_ticker_override_candidates.py
```

This writes `reports/dead-ticker-review/sec_override_candidates.csv`. Rows are intentionally marked `research_status=candidate_needs_review`, not `verified`, so `utils/import_dead_ticker_manual_overrides.py` will not append them until the SEC filing has been reviewed and the row has been completed. The candidate file pre-fills issuer, event-lead type, event date, SEC archive URL, and the triage reason so review can focus on confirming issuer/CIK, final event, successor, and delisting or closing evidence.

Run a dry import before changing the manual override file:

```bash
uv run python utils/import_dead_ticker_manual_overrides.py \
  --template-path reports/dead-ticker-review/sec_override_candidates.csv \
  --dry-run
```

To fetch each candidate filing and score stronger text evidence without importing anything:

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/verify_sec_override_candidates.py
```

This writes:

- `reports/dead-ticker-review/sec_override_candidates_verified_triage.csv`
- `reports/dead-ticker-review/sec_override_candidates_verified_triage_summary.json`

The verifier fetches the SEC archive index, chooses a filing HTML document, and scores issuer-name, symbol, event-language, completion-language, delisting-language, and going-private-form signals. Output buckets are `strong_review_candidate`, `moderate_review_candidate`, `weak_review_candidate`, and `fetch_error`. It does not mark rows `verified`; rows still require filing review before import.

Build a focused review batch from the verifier output:

```bash
uv run python utils/build_sec_verified_review_batch.py
```

This writes `reports/dead-ticker-review/sec_strong_review_batch.csv`, containing only `strong_review_candidate` rows sorted by verifier score and priority rank. Use `--min-bucket moderate_review_candidate --output-path reports/dead-ticker-review/sec_strong_plus_moderate_review_batch.csv` when the strong batch is exhausted. Batch rows retain `research_status=candidate_needs_review`; dry-run import should report zero verified rows until a reviewer updates completed rows.

## Resolution Ledger

The full unresolved count is larger than the manual identity queue because it includes
preferreds, warrants, units, rights, share classes, sparse intermittent feed eras, and
operating-company terminal events. Use the resolution ledger for workflow dispositions
that should leave the remaining-work queue without asserting a verified historical issuer.

Ledger file:

- `data/manual_overrides/ticker_era_resolution_ledger.csv`

Build route-specific lane files and backlog profiles:

```bash
uv run python utils/build_dead_ticker_resolution_lanes.py
```

This writes:

- `reports/dead-ticker-review/resolution-lanes/resolution_lanes.csv`
- `reports/dead-ticker-review/resolution-lanes/backlog_profile.csv`
- one CSV per `resolution_lane`
- `reports/dead-ticker-review/resolution-lanes/resolution_lanes_summary.json`

Build the impact-weighted workplan from the lane file:

```bash
uv run python utils/build_dead_ticker_resolution_workplan.py
```

This writes:

- `reports/dead-ticker-review/resolution-workplan/workplan_summary.json`
- `reports/dead-ticker-review/resolution-workplan/workplan_high_impact_operating.csv`
- `reports/dead-ticker-review/resolution-workplan/workplan_operating_lifecycle_search.csv`
- `reports/dead-ticker-review/resolution-workplan/workplan_derivative_parent_resolution.csv`
- `reports/dead-ticker-review/resolution-workplan/workplan_low_materiality_bulk_disposition.csv`
- `reports/dead-ticker-review/resolution-workplan/workplan_manual_review_hold.csv`
- `reports/dead-ticker-review/resolution-workplan/workplan_low_materiality_ledger_candidates.csv`
- `reports/dead-ticker-review/resolution-workplan/workplan_sec_document_graph.csv`

The workplan ranks rows by descending `trade_rows`, adds cumulative impact fields,
and routes work into high-impact operating SEC review, operating lifecycle search,
parent/root security disposition, low-materiality bulk disposition, or manual hold.
The latest local run covered `16,605` unresolved priority eras; the high-impact
operating bucket has `1,992` eras and the low-materiality candidate file has `3,916`
dry-run-only ledger candidates.

Low-materiality rows are workflow dispositions, not issuer identity overrides. Review
and import them through the ledger importer only after dry-run validation:

```bash
uv run python utils/import_ticker_era_resolution_ledger.py \
  --candidates-path reports/dead-ticker-review/resolution-workplan/workplan_low_materiality_ledger_candidates.csv \
  --summary-path reports/dead-ticker-review/resolution-workplan/workplan_low_materiality_ledger_import_dry_run_summary.json \
  --dry-run
```

The optional SEC document graph scoring pass consumes local verifier output when it
exists, keeps up to five document rows per `symbol_era_id`, and scores issuer/symbol,
event, completion, delisting, and terminal-date proximity signals. It does not fetch
SEC data and does not auto-import rows.

Build local parent/root security disposition candidates for preferreds, warrants, units,
rights, and share classes:

```bash
uv run python utils/build_parent_security_resolution_candidates.py
```

This writes:

- `reports/dead-ticker-review/resolution-lanes/parent_security_resolution_candidates.csv`
- `reports/dead-ticker-review/resolution-lanes/parent_security_resolution_summary.json`

Dry-run and import ledger candidates:

```bash
uv run python utils/import_ticker_era_resolution_ledger.py --dry-run
uv run python utils/import_ticker_era_resolution_ledger.py
```

Then regenerate review artifacts:

```bash
uv run python utils/build_dead_ticker_review_queue.py
uv run python utils/build_dead_ticker_priority_queue.py
uv run python utils/build_dead_ticker_resolution_template.py --limit 100
```

Ledger rows with terminal statuses move out of the unresolved priority queue through
`resolution_workflow_status=ledger_terminal_disposition`. They do not become
`manual_verified_historical_identity`; only verified identity override rows do that.

## Terminal Event Search Windows

For `operating_terminal_event` rows, avoid searching the full ticker era when the era
spans many years. Full-era searches can match old mergers for reused or ambiguous
symbols. Build a terminal-window search batch around `last_day` instead:

```bash
uv run python utils/build_terminal_event_search_batch.py \
  --input-path reports/dead-ticker-review/resolution-lanes/operating_terminal_event.csv \
  --output-path reports/dead-ticker-review/resolution-lanes/operating_terminal_event_top250_terminal_window.csv \
  --summary-path reports/dead-ticker-review/resolution-lanes/operating_terminal_event_top250_terminal_window_summary.json \
  --limit 250
```

Run SEC full-text with strict date bounds so every fallback keeps the terminal window:

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/search_edgar_full_text.py \
  --template-path reports/dead-ticker-review/resolution-lanes/operating_terminal_event_top250_terminal_window.csv \
  --output-root reports/dead-ticker-review/edgar-operating-terminal-top250-terminal-window \
  --event-terms merger \
  --strict-date-bounds
```

After triage, candidate building, and SEC verifier scoring, run strict terminal review:

```bash
uv run python utils/build_strict_terminal_review.py \
  --verifier-path reports/dead-ticker-review/edgar-operating-terminal-top250-terminal-window/sec_override_candidates_verified_triage.csv \
  --output-path reports/dead-ticker-review/edgar-operating-terminal-top250-terminal-window/strict_terminal_review.csv \
  --auto-verified-path reports/dead-ticker-review/edgar-operating-terminal-top250-terminal-window/strict_terminal_auto_verified.csv \
  --summary-path reports/dead-ticker-review/edgar-operating-terminal-top250-terminal-window/strict_terminal_review_summary.json
```

The strict auto-verified output is intentionally narrow. It requires terminal-close
quality evidence such as a terminal exit form or completion language filed very close
to `original_last_day`. Strong text matches without close timing stay in review buckets.

For strong rows that still need close/completion review, build a focused queue:

```bash
uv run python - <<'PY'
import csv
from pathlib import Path

p = Path("reports/dead-ticker-review/edgar-operating-terminal-top250-terminal-window/strict_terminal_review.csv")
out = Path("reports/dead-ticker-review/edgar-operating-terminal-top250-terminal-window/strong_needs_close_review.csv")
with p.open(newline="") as f:
    rows = [
        r for r in csv.DictReader(f)
        if r.get("strict_terminal_bucket") == "strong_needs_close_or_completion_review"
    ]
rows.sort(key=lambda r: (
    abs(int(r.get("days_filed_to_original_last") or 999999)),
    -int(float(r.get("trade_rows") or 0)),
))
with out.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
PY
```

Then run the close-evidence second pass:

```bash
uv run python utils/build_close_evidence_review.py \
  --input-path reports/dead-ticker-review/edgar-operating-terminal-top250-terminal-window/strong_needs_close_review.csv \
  --output-path reports/dead-ticker-review/edgar-operating-terminal-top250-terminal-window/close_evidence_review.csv \
  --auto-verified-path reports/dead-ticker-review/edgar-operating-terminal-top250-terminal-window/close_evidence_auto_verified.csv \
  --summary-path reports/dead-ticker-review/edgar-operating-terminal-top250-terminal-window/close_evidence_review_summary.json
```

This pass is narrower than manual review but broader than terminal exit forms. It
auto-verifies only strong `8-K` or `425` rows filed within five calendar days of
`original_last_day`, without parenthetical ticker collisions, and with issuer,
event, and completion or delisting language in the fetched SEC document.

For proxy, S-4, 425, and other high-impact terminal candidates where the filing
date or form alone is too weak, extract short terminal-event text evidence from
the exact SEC document URL captured by the verifier:

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/build_sec_terminal_text_evidence.py \
  --verifier-path reports/dead-ticker-review/edgar-high-impact-terminal-window-top250-closing/sec_override_candidates_verified_triage.csv \
  --output-dir reports/dead-ticker-review/edgar-high-impact-terminal-window-top250-closing
```

This writes `terminal_text_evidence_review.csv`,
`terminal_text_auto_verified.csv`, and `terminal_text_evidence_summary.json`.
It stores only bounded snippets, never full filing text. Auto-ready rows require
credible issuer/ticker identity, no parenthetical ticker collision, same-snippet
completion or delisting language, and an explicit terminal date within 14
calendar days of `original_last_day`. The output is a candidate feed only; ledger
or manual-override import remains a separate dry-run/apply step.

If terminal text is strong but the exact verifier document lacks a nearby explicit
date, run the CIK-based follow-up pass. This hits SEC submissions for each
questionable row, fetches candidate `8-K`, `25-NSE`, `15-12B`, `425`, proxy, and
S-4 documents around `original_last_day`, and rescans the fetched documents for
auto-ready terminal text/date evidence:

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/build_sec_terminal_followup_evidence.py \
  --input-path reports/dead-ticker-review/edgar-high-impact-terminal-window-top250-closing/terminal_text_high_signal_missing_date_review.csv \
  --output-dir reports/dead-ticker-review/edgar-high-impact-terminal-window-top250-closing
```

This writes `terminal_followup_evidence_review.csv`,
`terminal_followup_auto_verified.csv`, and
`terminal_followup_evidence_summary.json`. It still does not import anything.

For larger batches, use the end-to-end batch runner instead of hand-running each
stage. It builds a terminal-window input from the unresolved priority queue,
runs SEC full-text search, triage, candidate verification, strict/close/text
evidence passes, CIK follow-up, conservative import-candidate generation, and a
manual override import dry-run:

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/run_sec_terminal_resolution_batch.py \
  --limit 500 \
  --output-root reports/dead-ticker-review/sec-terminal-batch/top500
```

The runner is dry-run by default. It writes `terminal_import_candidates.csv` and
`terminal_import_summary.json` under the output root. Add `--apply-import` only
after the dry-run summary shows the intended `symbol_era_id` additions.

To keep moving without reprocessing the same unresolved top names, run the
iterative wrapper. It skips rows attempted earlier in the run, invokes the full
batch runner for each slice, applies only conservative candidates when
`--apply-import` is present, and regenerates the review/priority queues after
successful imports:

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/run_sec_terminal_resolution_iterations.py \
  --batch-size 500 \
  --max-iterations 10 \
  --output-root reports/dead-ticker-review/sec-terminal-iterations/run-001 \
  --apply-import
```

For a non-mutating probe, omit `--apply-import`. The wrapper writes
`sec_terminal_iterations_summary.json` with attempted, candidate, and verified
counts per iteration.

## Operating Lifecycle Search

After the terminal lane has low yield, switch to `operating_lifecycle_search`
instead of widening the terminal gate. This lane searches both ends of each
operating era: a first-day window for trading commencement/name/ticker lifecycle
evidence and a last-day window for terminal or delisting evidence.

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/run_sec_lifecycle_resolution_iterations.py \
  --batch-size 250 \
  --max-iterations 10 \
  --output-root reports/dead-ticker-review/sec-lifecycle-iterations/run-001 \
  --apply-import
```

For a non-mutating probe, omit `--apply-import`. The runner writes per-iteration
SEC search, triage, verifier, lifecycle text review, auto-ready, import summary,
and `sec_lifecycle_iterations_summary.json` outputs.

Lifecycle auto-ready rows require issuer or direct ticker identity, no
parenthetical ticker conflict, an explicit date within 14 calendar days of the
searched anchor, and anchor-specific language. First-day auto-ready evidence
requires direct ticker context such as trading under the era symbol. Last-day
auto-ready evidence requires terminal language such as completed merger,
ceased trading, delisting, suspension, Form 25, or registration termination.
Only bounded snippets are stored; full SEC filing text is not persisted.

The lifecycle runner also runs a CIK/submissions follow-up pass after direct text
review. It targets missing-date, date-mismatch, first/last-day review,
prospective-language, and symbol-collision buckets, fetches candidate filings
around the relevant `original_first_day` or `original_last_day`, and rescans
those documents with the same conservative lifecycle text gate.

Follow-up outputs are written beside the direct lifecycle text outputs:

- `lifecycle_followup_evidence_review.csv`
- `lifecycle_followup_auto_verified.csv`
- `lifecycle_followup_evidence_summary.json`

The runner combines direct and follow-up auto-ready rows into
`lifecycle_import_candidates.csv` before invoking the manual override importer.

## Identity-First High-Impact SEC Resolution

The high-impact operating lane now has one resumable command:

```bash
SEC_USER_AGENT="IEXScoper research admin@example.com" \
uv run python utils/run_sec_high_impact_identity_resolution_iterations.py
```

The default is a dry run. Add `--apply-import` only after reviewing
`high_impact_import_candidates.csv`. The runner snapshots the input CSV and its
SHA-256, scans every existing `edgar_full_text_raw.jsonl` before making a
request, and resumes from `run_state.json` when the input hash and workflow
version match. `--force-reprocess` deliberately starts a new state within the
selected output root.

Identity and event proof are separate gates. An identity is ready only when the
era ticker appears in the SEC filer's parenthetical ticker list, the evidence
date is within the configured era window, and every date-scoped direct match
has one CIK. Multiple names on that CIK are aliases; multiple CIKs are a hard
collision hold. Text mentions in another filer's document never establish the
target's identity.

Resolution buckets have these meanings:

- `terminal_verified_ready`: anchored-CIK filing proves an actual terminal
  event with a date within 14 days of the observed last day.
- `symbol_change_verified_ready`: the same CIK explicitly proves an old-to-new
  ticker transition and near date; this takes precedence over generic cessation
  language.
- `active_or_data_gap_hold`: submissions metadata and the SEC ticker directory
  both still match, with filing activity after the observed last day.
- `identity_only_hold`: historical identity is proved but no conservative event
  disposition is proved.
- `no_identity_found`: no unique, date-scoped filer identity was established.
- `fetch_error_hold`: bounded retries were exhausted.

Only the first two buckets enter the manual historical-identity importer.
Existing override IDs are removed from the candidate file and reported as
`already_imported`; the importer itself continues to reject duplicates. On an
apply run, import occurs once after all batches, followed by one regeneration
of the review queue, priority queue, resolution lanes, and workplan.

The workplan's `automation_status` is operational state, not historical truth.
`automation_exhausted` means this workflow consumed its bounded evidence and
attempt budget; it does not mean the issuer or event was resolved. SEC transport
outages open a resumable circuit breaker and return non-zero rather than
silently accepting a partial run. Full filing text is never written; only SEC
metadata and bounded evidence snippets are retained.

The same engine can process the manual-hold, lifecycle, and unknown-instrument
CSVs using `--input-path` and a distinct `--output-root`. Do not replay the old
event-first lifecycle runner. Derivative securities use
`utils/derivative_identity_resolution.py`: parent syntax alone is always held,
and verified results target the resolution ledger rather than historical
identity overrides.

## Evidence Standard

Prefer primary or near-primary sources:

- SEC filings: merger 8-K, going-private filings, S-4, 425, 25-NSE, 15-12B.
- Company or acquirer press releases.
- Exchange delisting notices.
- Reputable wire/news only when primary filings are unavailable or hard to map.

Record at least one strong source URL in `primary_source_url`. Use `secondary_source_url` for corroboration.

## Import Verified Overrides

After research, mark completed rows in the template:

- `research_status`: `verified`
- `proposed_historical_identity_status`: for example `manual_verified_acquired_delisted`
- `proposed_historical_issuer_name`: historical issuer name
- `proposed_historical_event_type`: for example `acquired_delisted`, `taken_private_delisted`, `bankruptcy_delisted`
- `proposed_historical_event_date`: event date in `YYYY-MM-DD`
- `proposed_historical_successor`: acquirer/successor when applicable; leave blank if none
- `primary_source_url`: strongest source URL
- `research_note`: concise rationale

Validate the completed template without mutating the override file:

```bash
uv run python utils/import_dead_ticker_manual_overrides.py --dry-run
```

Append verified rows into:

- `data/manual_overrides/historical_ticker_identities.csv`

```bash
uv run python utils/import_dead_ticker_manual_overrides.py
```

Required override fields:

- `symbol`
- `symbol_era_id`
- `historical_identity_status`
- `historical_issuer_name`
- `historical_event_type`
- `historical_event_date`
- `historical_successor`
- `source_url`
- `source_note`

The importer only accepts rows with `research_status=verified`, rejects missing required evidence, and refuses duplicate `symbol_era_id` values already present in the manual override file.

Then regenerate review artifacts:

```bash
uv run python utils/build_dead_ticker_review_queue.py
uv run python utils/build_dead_ticker_priority_queue.py
uv run python utils/build_dead_ticker_resolution_template.py --limit 100
```

Resolved rows should move from `historical_identity_unresolved` to `manual_verified_historical_identity`.

## Caveats

- EDGAR current ticker matches are leads only.
- Current CIK evidence does not prove that an old `symbol_era_id` belonged to the same issuer.
- Funds, warrants, units, bankruptcies, acquisitions, and ticker reuse need separate treatment.
- Never overwrite an existing manual override without preserving the source rationale.
