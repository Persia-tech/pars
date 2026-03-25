# Bitcoin On-chain Analytics Parser

Local-first Bitcoin parser that ingests blocks from Bitcoin Core RPC and computes daily on-chain metrics:

- Realized Cap
- MVRV
- NUPL
- SOPR
- CDD

## Reliability hardening highlights

- Chunked, resumable parsing with parser checkpoints (`last_processed_height`, `last_aggregated_day`)
- Resume safety check validates DB hash vs RPC hash at checkpoint height
- RPC retries with exponential backoff for transient failures
- Idempotent block processing (already parsed blocks are skipped)
- Atomic UTXO consume (`UPDATE ... RETURNING`) to reduce race windows
- Unspendable `nulldata` outputs excluded from UTXO accounting

## Assumptions

- Bitcoin Core node is available and synced.
- Price history is loaded in `price_history` with one row per UTC day.
- Spot price for a block/day is resolved from `price_history` on that UTC day.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
```

Initialize schema:

```bash
python -m btc_analytics.cli.main init-db
```

Load prices (repeat for each needed day):

```bash
python -m btc_analytics.cli.main load-price --date 2009-01-03 --price 0.0
```

Initial sync:

```bash
python -m btc_analytics.cli.main sync-initial --start-height 0
```

Resume sync:

```bash
python -m btc_analytics.cli.main sync-resume
```

Daily incremental update:

```bash
python -m btc_analytics.cli.main daily-update
```

Recompute metrics for a date range:

```bash
python -m btc_analytics.cli.main recompute-metrics --from-date 2020-01-01 --to-date 2020-12-31
```

Check status:

```bash
python -m btc_analytics.cli.main status
```

## Extension points (phase 3 readiness)

`src/btc_analytics/extensions/interfaces.py` contains cohort plugin protocols and TODO stubs.
