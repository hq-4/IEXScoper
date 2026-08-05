# Archived: narrative-first SEC resolution lane

This is the code behind months of dead-ticker RCA/retry work recorded in
`docs/TASK_LIST.md` (roughly June–August 2026): three iteration runners and their EDGAR
full-text search, SEC filing-text scoring, workplan, and ledger stage modules. It moved
out of `utils/` on 2026-08-05, not because it stopped working, but because
`docs/EVENT_CATALOG_RESOLUTION_PLAN.md` measured its yield and found it plateaued:

- Terminal lane: `7,500` attempted → `37` verified (~0.5%).
- Lifecycle lane: `2,500` → `25`, then `861` → `9` (~1%).
- High-impact identity: `1,992` rows → `552` verified, `1,440` `no_identity_found`.

That plateau is exactly why the OpenFIGI identity pillar (`utils/openfigi_identity_core.py`
and friends, still at the top of `utils/`) was built afterward — it covers ~89% of the
unresolved baseline in one keyed pass instead of one ticker at a time through SEC search.

**This code is not dead.** It produced all `818` SEC-grade `verified` identity facts in
`data/resolution/identity_facts.jsonl` — the highest-assurance tier the OpenFIGI facts are
themselves measured against — plus the terminal workflow ledger
(`data/manual_overrides/ticker_era_resolution_ledger.csv`). It is still wired together, its
tests still pass, and README.md and `docs/DEAD_TICKER_RESOLUTION.md` still document how to
run it (now at `utils/legacy/...py`). It moved here because it is no longer the primary,
default resolution path — the OpenFIGI pillar is — not because it was deleted or superseded
in the sense of being wrong.

## What's here

- **Iteration runners** (composition roots, still runnable):
  `run_sec_high_impact_identity_resolution_iterations.py`,
  `run_sec_lifecycle_resolution_iterations.py`,
  `run_sec_terminal_resolution_iterations.py` (+ `run_sec_terminal_resolution_batch.py`,
  the single-pass batch each iteration wraps).
- **EDGAR full-text search**: `lookup_edgar_tickers.py`, `search_edgar_full_text.py`,
  `search_edgar_full_text_types.py`, `edgar_full_text_client.py`, `edgar_full_text_schema.py`,
  `edgar_full_text_targets.py`, `edgar_full_text_outputs.py`, `edgar_full_text_triage.py`,
  `edgar_full_text_triage_schema.py`.
- **SEC filing-text evidence scoring**: `sec_terminal_text_evidence.py`,
  `sec_lifecycle_text_evidence.py`, `sec_terminal_followup_sources.py`,
  `sec_document_graph_scoring.py`, `sec_candidate_verifier_schema.py`,
  `verify_sec_override_candidates.py`.
  (The two generic helpers these depend on, `extract_date`/`normalize`, now live in
  `utils/sec_identity_evidence.py` instead — the core V3 evidence gates need them too via
  `utils/derivative_identity_resolution.py`, so they couldn't move here without breaking
  the current pipeline.)
- **High-impact identity resolver internals**: `sec_high_impact_events.py`,
  `sec_high_impact_logging.py`, `sec_high_impact_outputs.py`,
  `sec_high_impact_resolution.py`, `sec_high_impact_runtime.py`, `sec_high_impact_state.py`,
  `sec_high_impact_workflow.py`.
- **Terminal/lifecycle evidence batch stages**: `build_close_evidence_review.py`,
  `build_strict_terminal_review.py`, `build_terminal_event_search_batch.py`,
  `build_lifecycle_event_search_batch.py`, `build_lifecycle_override_candidates.py`,
  `build_sec_terminal_text_evidence.py`, `build_sec_terminal_followup_evidence.py`,
  `build_sec_lifecycle_text_evidence.py`, `build_sec_lifecycle_followup_evidence.py`,
  `build_sec_verified_review_batch.py`, `build_dead_ticker_override_candidates.py`,
  `build_parent_security_resolution_candidates.py`.
- **Workplan/ledger reporting**: `dead_ticker_resolution_workplan.py`,
  `build_dead_ticker_resolution_workplan.py`, `build_dead_ticker_resolution_lanes.py`,
  `dead_ticker_workplan_automation.py`, `dead_ticker_workplan_ledger.py`,
  `dead_ticker_workplan_outputs.py`, `import_ticker_era_resolution_ledger.py`.
- **Unrelated stray**: `debug_iextools_day.py` — a one-off IEXTools parse/normalize
  isolation probe from the TOPS parser RCA (`docs/TASK_LIST.md`, ~2026-06-03), not part of
  the SEC resolution lane at all. It was never imported anywhere and has no test; it's here
  because it's genuinely a leftover diagnostic script, which none of the SEC lane files are.

## How this was decided, not guessed

Every file above was placed here only after confirming, by parsing the actual `import`
statements and `subprocess`/CLI-string references across `utils/`, `tests/`, and the docs,
that nothing in the current pipeline (`run_dead_ticker_resolution_program.py`, the OpenFIGI
pillar, `build_dead_ticker_review_queue.py`/`build_dead_ticker_priority_queue.py`, and the
derived-dataset/ingest pipeline) depends on it. One real cross-boundary dependency turned up
during that check — `utils/derivative_identity_resolution.py` (a live V3 evidence gate)
needed two generic helpers from `sec_terminal_text_evidence.py` — and was resolved by moving
those two functions into the shared `utils/sec_identity_evidence.py` module rather than by
keeping the whole terminal-lane module out of `legacy/`.

## Running it

Nothing changed functionally — only the path. Where docs previously said
`uv run python utils/run_sec_terminal_resolution_iterations.py`, it is now
`uv run python utils/legacy/run_sec_terminal_resolution_iterations.py`. All three iteration
runners still require an `SEC_USER_AGENT`, still write dry-run-first output, and still feed
`data/manual_overrides/` the same way they always did.
