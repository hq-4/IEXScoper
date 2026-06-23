# Security

- Benchmark inputs are local archived files under `/media/tn/iex`; no live IEX HIST download is part of the benchmark path.
- The harness clones parser source repos only when the local benchmark cache is missing.
- Generated benchmark outputs are confined to the configured temp output root.
- OpenFIGI enrichment reads the API key from `OPENFIGI_API_KEY` by default and never logs the key value.
- OpenFIGI cache files contain public mapping response metadata, not secrets. Keep them in report roots rather than source-controlled fixtures unless intentionally curated.
- OpenFIGI ticker-only mapping is a triage aid, not proof of historical issuer identity. Treat CUSIP/ISIN/security-master joins as the higher-assurance path for reused or acquired tickers.
