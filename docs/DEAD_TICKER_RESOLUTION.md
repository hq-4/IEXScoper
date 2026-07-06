# Dead Ticker Resolution Workflow

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

Full-text hits are noisy leads, especially for short tickers. Verify the CIK, issuer, filing date, and event before importing an override.

The full-text helper mirrors the SEC search page request shape: event terms in `q`, ticker text in `entityName`, `dateRange=custom` with era date bounds, and no custom `size` parameter. It retries transient SEC 5xx responses. If a symbol still fails after retries, the row is retained as `search_error` and the batch continues.

The default full-text pass uses `q=merger` only. Avoid broad `OR` queries across many event terms; SEC EFTS has returned repeated 500s for that shape. Run separate passes when needed, for example:

```bash
SEC_USER_AGENT="IEXScoper research your-email@example.com" \
uv run python utils/search_edgar_full_text.py --event-terms acquisition --output-root reports/dead-ticker-review/edgar-full-text-acquisition
```

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
