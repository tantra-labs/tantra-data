# Gap 0017 — Institutional-Flow (FII/DII) Rust Bridge Not Implemented

Severity: Medium · Status: Open · Tracked by: `tantra-agent/docs/plans/0007` Track 2 Phase 4 and plan 0010

## What's Missing

FII/DII daily institutional net-flow ingestion exists Python-side
(`InstitutionalFlow` domain, NSE `fiidiiTradeReact` provider,
`DuckDbInstitutionalFlowStore`, `PythonInstitutionalFlowReader` — gap 0013,
closed) but there is **no Rust bridge**: no parquet projection export and no
`tantra-market-store` projection reader, so the chat `institutional_flows` tool
cannot reach the data.

## Requirements

1. Add an institutional-flow projection to `service/bridge_exporter.py`
   (`{root}/catalog/institutional_flows/*.parquet`, exact Decimal128) mirroring
   the filings/corporate-actions bridge (T5.5/T5.6 pattern).
2. Add `InstitutionalFlowProjectionReader` in `tantra-market-store`.
3. Wire `FeedMarketReader::institutional_flows` in `tantra-runtime` to it.
4. Until then the runtime adapter answers `PortError::unavailable` citing this
   gap — never fabricated flows.

## Why It Matters

FII/DII net flows are a required input to the `institutional_flows` chat tool
(plan 0007 Track 2 Phase 4) and the composite smart-money signal (agent plan
0010).
