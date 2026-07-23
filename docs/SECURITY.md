# Security

- Benchmark inputs are local archived files under `/media/tn/iex`; no live IEX HIST download is part of the benchmark path.
- The harness clones parser source repos only when the local benchmark cache is missing.
- Generated benchmark outputs are confined to the configured temp output root.
- OpenFIGI enrichment reads the API key from `OPENFIGI_API_KEY` by default and never logs the key value.
- OpenFIGI cache files contain public mapping response metadata, not secrets. Keep them in report roots rather than source-controlled fixtures unless intentionally curated.
- OpenFIGI ticker-only mapping is a triage aid, not proof of historical issuer identity. Treat CUSIP/ISIN/security-master joins as the higher-assurance path for reused or acquired tickers.
## Resolution V2 Evidence Safety

The resolution registry stores public-source request metadata/results, immutable historical
negative results, filing hashes, authority metadata, and at most eight normalized 1,000-character
snippets per document. Complete filing text is held in memory only. Public-source adapters are
limited to SEC, FINRA, NasdaqTrader, NYSE, and issuer-controlled or SEC-attached material;
press-release syndication and general crawling are excluded. SEC contact strings and secrets
must never enter logs or canonical facts. [SFT][RM]
