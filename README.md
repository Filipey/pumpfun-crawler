# pumpfun-crawler

A data collection pipeline for **pump.fun** (Solana memecoin launchpad) supporting academic research on decentralized pump-and-dump schemes, memecoin taxonomy, creator influence networks, and trader behavior modeling via Inverse Reinforcement Learning.

> **Institution**: UFMG — Departamento de Ciência da Computação, LOCUS (Social Computing Laboratory)

---

## Table of Contents

1. [Research Motivation](#research-motivation)
2. [Architecture Overview](#architecture-overview)
3. [Project Structure](#project-structure)
4. [Installation](#installation)
5. [Configuration](#configuration)
6. [Primary Collection: Automated Dune Script](#primary-collection-automated-dune-script)
7. [Parallel Collection with Multiple API Keys](#parallel-collection-with-multiple-api-keys)
8. [Secondary Backend: Bitquery (Continuous)](#secondary-backend-bitquery-continuous)
9. [Source-Agnostic Phases](#source-agnostic-phases)
10. [Unified Output Schema](#unified-output-schema)
11. [Data Output Reference](#data-output-reference)
12. [Mapping Data to Research Axes](#mapping-data-to-research-axes)
13. [pump.fun Technical Constants](#pumpfun-technical-constants)
14. [Rate Limits and Cost](#rate-limits-and-cost)
15. [Troubleshooting](#troubleshooting)

---

## Research Motivation

This pipeline collects data to support three interrelated research axes:

| Axis | Goal | Key Data |
|------|------|----------|
| **1. Multi-label taxonomy** | Classify memecoins by narrative (political, NSFW, viral, celebrity, etc.) and correlate label combinations with P&D success | Token metadata: name, ticker, image, description |
| **2. Creator influence network** | Measure how coin-dev identity and trader co-occurrence networks explain token outcomes | Bipartite graph: creator→token, trader→token edges |
| **3. IRL trader profiling** | Infer implicit reward functions of trader archetypes (insider, sniper, jeeter, bag-holder) using Inverse Reinforcement Learning | Per-trade data: wallet, price, slot, direction, amount |

All three axes require complete historical data from pump.fun's launch (January 2024) onward.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│               collect_dune_auto.py  (PRIMARY)                   │
│                                                                 │
│  Checkpointed · Credit-budgeted · Parallel multi-key workers   │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Dune Analytics  (decoded SQL on full pump.fun history)  │   │
│  │  dune_runner.py ── queries.py ── checkpoint.py           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                         │                                       │
│              data/parquet/dune/                                 │
│     creations_YYYY-MM.parquet                                   │
│     trades_YYYY-MM-DD.parquet                                   │
│     migrations_YYYY-MM.parquet                                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│               pipeline.py  (SECONDARY / CONTINUOUS)             │
│                                                                 │
│  --backend bitquery  (real-time window, last few hours)        │
│  --backend dune      (per-token or per-wallet queries)         │
│  --only network      (build influence graph)                   │
│  --only metadata     (fetch IPFS/Arweave descriptions+images)  │
└─────────────────────────────────────────────────────────────────┘

        Both write to the same unified Parquet schema (schemas.py)
```

---

## Project Structure

```
pumpfun-crawler/
│
├── collect_dune_auto.py          # ★ PRIMARY: automated bulk collection
├── pipeline.py                   # Secondary CLI (Bitquery, network, metadata)
│
├── config.py                     # Constants, paths, program IDs
├── schemas.py                    # Unified output column definitions
├── client.py                     # Bitquery GraphQL client (rate-limited)
├── utils.py                      # Time windowing, Parquet I/O
│
├── .env.example                  # Template for API keys (copy to .env)
├── requirements.txt              # Python dependencies
│
├── queries/                      # Bitquery GraphQL query strings
│   ├── tokens.py
│   ├── trades.py
│   ├── holders.py
│   ├── migrations.py
│   └── market.py
│
└── collectors/
    ├── token_collector.py        # Bitquery: token creation batch collection
    ├── trade_collector.py        # Bitquery: per-token and per-wallet trades
    ├── holder_collector.py       # Bitquery: holder snapshots
    ├── migration_collector.py    # Bitquery: graduation events
    ├── network_builder.py        # Bipartite graph construction
    ├── metadata_fetcher.py       # Off-chain metadata (image + description)
    │
    └── dune/
        ├── queries.py            # Parameterized SQL queries
        ├── dune_runner.py        # Dune SDK wrapper
        ├── bulk_collector.py     # Low-level day-by-day collector
        └── checkpoint.py         # Thread-safe progress + credit tracking
```

---

## Installation

```bash
git clone <your-repo-url>
cd pumpfun-crawler

python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

pip install -r requirements.txt

cp .env.example .env
# Edit .env with your API keys (see Configuration)
```

### Dependencies

| Package | Purpose |
|---------|---------|
| `dune-client` | Dune Analytics SDK |
| `requests` | HTTP client for Bitquery |
| `pandas` | DataFrame manipulation |
| `pyarrow` | Parquet I/O |
| `networkx` | Influence graph construction |
| `tqdm` | Progress bars |
| `ratelimit` | Bitquery rate limiting |
| `backoff` | Exponential retry |
| `python-dotenv` | `.env` loading |

---

## Configuration

### Single key (default)

Copy `.env.example` to `.env` and fill in:

```bash
# Dune Analytics — get your key at https://dune.com/settings/api
DUNE_API_KEY=your_dune_api_key_here

# Bitquery — get your key at https://account.bitquery.io
BITQUERY_TOKEN=your_bitquery_token_here

# Data output directory (default: ./data)
DATA_DIR=./data
```

### Multiple keys (for parallel collection)

Create a `keys.txt` file — one Dune API key per line. Blank lines and `#` comments are ignored:

```
# keys.txt — NEVER commit this file to git
dune_key_aaaa...
dune_key_bbbb...
dune_key_cccc...
```

> `keys.txt` is already in `.gitignore`. Never commit API keys.

---

## Primary Collection: Automated Dune Script

`collect_dune_auto.py` is the main tool for bulk historical collection. It collects **token creations, all trades, and graduation migrations** from any date range, with:

- **Checkpoint**: every completed window is saved immediately to `data/dune_checkpoint.json`. Kill the process anytime and restart — it resumes exactly where it stopped.
- **Credit budgeting**: tracks estimated Dune API usage per key per calendar month. Automatically sleeps until the 1st of the next month when the budget is exhausted, then resumes.
- **Truncation detection**: if a daily trade window returns ≥990k rows (Dune's silent cap), the window is automatically split into 12-hour halves and retried, up to two levels deep (day → 12h → 6h).
- **Window strategy**: creations and migrations use monthly windows (~17 queries total); trades use daily windows (~500 queries for 2025–2026).

### Basic usage

```bash
# Collect everything from Jan 2025 to May 2026
python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13

# Collect only specific data types
python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13 --types trades
python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13 --types creations migrations

# Check progress without running anything
python collect_dune_auto.py --status
```

### Credit budget

```bash
# Free tier default (2,000 conservative estimate; actual limit is 2,500/month)
python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13

# Dune Plus (~$50/month, 50k credits) — finishes ~5x faster
python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13 --budget 50000
```

### Estimated collection time (single key)

| Data type | Windows | Queries | Free tier time |
|-----------|---------|---------|----------------|
| Creations | 17 monthly | ~17 | 1 run |
| Migrations | 17 monthly | ~17 | 1 run |
| Trades | ~500 daily | ~500 | ~2 months |
| **Total** | | **~534** | **~2 months** |

### Cron setup (recommended)

Run every 4 hours on any machine. The checkpoint ensures no work is duplicated:

```bash
# Single key
0 */4 * * * cd /path/to/pumpfun-crawler && python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13 >> logs/dune.log 2>&1

# Multiple keys (see next section)
0 */4 * * * cd /path/to/pumpfun-crawler && python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13 --keys-file keys.txt >> logs/dune.log 2>&1
```

### Raw creations fallback

If the `pump_fun_solana` decoded schema is unavailable on Dune, use the raw instruction fallback:

```bash
python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13 --raw-creations
```

---

## Parallel Collection with Multiple API Keys

Each Dune API key has its own 2,500 credit/month budget. With N keys you get N× the budget **and** N× the wall-clock throughput (Dune queries take 10–60 seconds each, so parallelism gives real speedup).

Each worker gets its own Dune client; they share a single thread-safe checkpoint so no window is ever collected twice.

```bash
# Use all keys in keys.txt as parallel workers
python collect_dune_auto.py \
    --start 2025-01-01 \
    --end 2026-05-13 \
    --keys-file keys.txt

# Check per-key credit usage
python collect_dune_auto.py --status --keys-file keys.txt
```

### Speedup by number of free accounts

| Keys | Budget/month | Estimated time |
|------|-------------|----------------|
| 1 | 2,500 credits | ~2 months |
| 3 | 7,500 credits | ~3 weeks |
| 6 | 15,000 credits | ~10 days |
| 1 (Dune Plus) | 50,000 credits | ~5 days |

### Status output with multiple keys

```
Dune collection progress:
  creations   :   17 windows done  |       412,384 rows
  migrations  :   17 windows done  |         8,921 rows
  trades      :  142 windows done  |   189,234,107 rows

  Credit usage this month (per key):
    ...KEY_AAAA: 1840/2000 (160 remaining)
    ...KEY_BBBB: 1720/2000 (280 remaining)
    ...KEY_CCCC: 1950/2000 (50 remaining)
```

---

## Secondary Backend: Bitquery (Continuous)

The Bitquery backend covers **real-time data** via the Streaming GraphQL API. On the free tier it exposes a rolling window of the last few hours (`dataset: realtime`). Run it every 2–4 hours via cron to avoid gaps.

> Bitquery free tier has **no historical access**. For any retroactive needs, use the Dune script above.

```bash
# Collect everything for a recent window
python pipeline.py --backend bitquery --start 2026-05-11 --end 2026-05-13

# Only token creations
python pipeline.py --backend bitquery --start 2026-05-11 --end 2026-05-13 --only tokens

# Trades for specific tokens
python pipeline.py --backend bitquery --start 2026-05-11 --end 2026-05-13 \
    --only trades --mints mints.txt

# Holder snapshots
python pipeline.py --backend bitquery --start 2026-05-11 --end 2026-05-13 \
    --only holders --max-tokens 200

# Graduation events
python pipeline.py --backend bitquery --start 2026-05-11 --end 2026-05-13 \
    --only migrations
```

### Cron for continuous ingestion

```cron
# Every 3 hours — collects the last 4 hours of data (overlap avoids gaps)
0 */3 * * * cd /path/to/pumpfun-crawler && \
    python pipeline.py --backend bitquery \
    --start $(date -d "4 hours ago" +\%Y-\%m-\%dT\%H:00:00) \
    --end $(date +\%Y-\%m-\%dT\%H:00:00) \
    >> logs/bitquery.log 2>&1
```

If you miss a window, backfill with Dune — the data is permanent on-chain.

---

## Source-Agnostic Phases

These work on collected Parquet from **any** backend.

### Build Influence Network (Axis 2)

```bash
python pipeline.py --only network
```

Outputs:

| File | Description |
|------|-------------|
| `network/creator_token_edges.parquet` | Creator → Token bipartite edges |
| `network/trader_token_edges.parquet` | Trader → Token edges (volume, trade count) |
| `network/wallet_cooccurrence.parquet` | Wallet pairs sharing tokens (bundle/insider detection) |

### Fetch Token Metadata (Axis 1)

```bash
python pipeline.py --only metadata --max-tokens 5000
```

Downloads JSON from IPFS/Arweave URIs and optionally saves images. Adds `description`, `image_url`, `image_local_path` columns — direct input for the multi-label classifier (XLM-R, CLIP/SigLIP).

---

## Unified Output Schema

All backends output Parquet files conforming to the schemas in `schemas.py`.

### Token Creations (`TOKEN_CREATION_COLS`)

| Column | Type | Description |
|--------|------|-------------|
| `block_time` | str | ISO datetime of creation tx |
| `block_slot` | Int64 | Solana slot number |
| `creator` | str | Wallet that signed the create tx |
| `tx_signature` | str | Transaction signature |
| `mint_address` | str | Unique token identifier |
| `name` | str | Token name |
| `symbol` | str | Token ticker |
| `uri` | str | Metadata URI (IPFS/Arweave) |
| `decimals` | Int64 | Always 6 for pump.fun |
| `source` | str | `"dune"` or `"bitquery"` |

### Bulk Raw Trades (`RAW_TRADE_COLS`) — from `collect_dune_auto.py`

| Column | Type | Description |
|--------|------|-------------|
| `block_time` | str | ISO datetime |
| `block_slot` | Int64 | Solana slot |
| `tx_signature` | str | Transaction signature |
| `trader_address` | str | Wallet that traded |
| `token_bought_mint_address` | str | Mint of the token received |
| `token_sold_mint_address` | str | Mint of the token sent |
| `token_bought_symbol` | str | Symbol of the token received |
| `token_sold_symbol` | str | Symbol of the token sent |
| `token_bought_amount` | float64 | Amount received |
| `token_sold_amount` | float64 | Amount sent |
| `amount_usd` | float64 | USD value at trade time |
| `source` | str | `"dune"` |

> To derive trade direction: if `token_bought_mint_address != SOL_MINT` → buy; if `token_sold_mint_address != SOL_MINT` → sell.

### Normalized Trades (`TRADE_COLS`) — from Bitquery or per-token Dune queries

| Column | Type | Description |
|--------|------|-------------|
| `block_time` | str | ISO datetime |
| `block_slot` | Int64 | Solana slot |
| `tx_signature` | str | Transaction signature |
| `trader_address` | str | Wallet |
| `trade_type` | str | `"buy"` or `"sell"` |
| `token_mint` | str | Mint address |
| `token_symbol` | str | Ticker |
| `amount_token` | float64 | Memecoin amount |
| `amount_sol` | float64 | SOL side |
| `amount_usd` | float64 | USD value |
| `price_usd` | float64 | Per-token price |
| `source` | str | `"dune"` or `"bitquery"` |

### Migrations (`MIGRATION_COLS`)

| Column | Type | Description |
|--------|------|-------------|
| `block_time` | str | ISO datetime of graduation |
| `block_slot` | Int64 | Solana slot |
| `tx_signature` | str | Transaction signature |
| `token_mint` | str | Graduated token's mint |
| `creator` | str | Transaction signer |
| `source` | str | `"dune"` or `"bitquery"` |

---

## Data Output Reference

```
data/
├── dune_checkpoint.json          # Collection progress (safe to keep, small)
│
├── raw/
│   ├── dune_sql/                 # Generated SQL files (manual mode)
│   └── dune_csv/                 # Place manually-exported Dune CSVs here
│
└── parquet/
    ├── dune/                     # Output from collect_dune_auto.py
    │   ├── creations_2025-01.parquet
    │   ├── creations_2025-02.parquet
    │   ├── ...
    │   ├── trades_2025-01-01.parquet
    │   ├── trades_2025-01-02.parquet
    │   ├── ...
    │   ├── migrations_2025-01.parquet
    │   └── migrations_2025-02.parquet
    │
    ├── tokens/                   # Bitquery: token creations + metadata
    ├── trades/                   # Bitquery: per-token trades, OHLCV
    ├── holders/                  # Bitquery: holder snapshots
    ├── migrations/               # Bitquery: graduation events
    └── network/                  # Source-agnostic: influence graph
        ├── creator_token_edges.parquet
        ├── trader_token_edges.parquet
        └── wallet_cooccurrence.parquet
```

> The `data/` directory is in `.gitignore` (can be TB-scale). Only `data/dune_checkpoint.json` is worth backing up manually.

---

## Mapping Data to Research Axes

### Axis 1: Multi-label Memecoin Taxonomy

```bash
# 1. Collect token creations (monthly windows, cheap)
python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13 --types creations

# 2. Fetch off-chain metadata (name, description, image)
python pipeline.py --only metadata --max-tokens 50000
```

```python
tokens = pd.read_parquet("data/parquet/tokens/tokens_with_metadata.parquet")
# tokens.name, tokens.symbol, tokens.description → text embeddings (XLM-R, BGE-M3)
# tokens.image_local_path                         → image embeddings (CLIP, SigLIP)
# → multi-label classification head (sigmoid per label)
```

### Axis 2: Creator Influence Network

```bash
# Collect creations and trades, then build graph
python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13
python pipeline.py --only network
```

```python
from collectors.network_builder import NetworkBuilder
import networkx as nx

G = NetworkBuilder().build_networkx_graph()
# → degree centrality, Louvain community detection
# → causal analysis: dev identity on token outcomes
```

### Axis 3: Inverse RL for Trader Profiling

```bash
# Bulk trades for full MDP trajectory extraction
python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13 --types trades
```

```python
import glob, pandas as pd

# Load all daily trade files
files = sorted(glob.glob("data/parquet/dune/trades_*.parquet"))
trades = pd.concat([pd.read_parquet(f) for f in files])

# Derive buy/sell direction
SOL_MINT = "So11111111111111111111111111111111111111112"
trades["trade_type"] = trades["token_bought_mint_address"].apply(
    lambda m: "buy" if m != SOL_MINT else "sell"
)
trades["token_mint"] = trades.apply(
    lambda r: r["token_bought_mint_address"] if r["trade_type"] == "buy"
              else r["token_sold_mint_address"], axis=1
)

# Each row maps to an MDP transition:
#   state  = (price, BCP, volume, position, unrealized_pnl, ...)
#   action = trade_type × amount_bucket
# Feed to MaxEnt IRL / GAIL to recover reward functions per trader archetype
```

---

## pump.fun Technical Constants

| Constant | Value |
|----------|-------|
| Program ID | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` |
| PumpSwap Program | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` |
| SOL Mint | `So11111111111111111111111111111111111111112` |
| Total token supply | 1,000,000,000 |
| Tokens on bonding curve | ~800,000,000 |
| Token decimals | 6 |
| Graduation market cap | ~$69,000 (~85 SOL) |
| Bonding curve | `y = 1,073,000,191 − 32,190,005,730 / (30 + x)` |
| BCP formula | `100 - ((balance − 206,900,000) × 100 / 793,100,000)` |
| Creation fee | ~0.02 SOL |
| Dev bonus on graduation | 0.5 SOL |
| Platform launch | January 2024 |

---

## Rate Limits and Cost

### Dune Analytics

| Resource | Free tier | Paid (Plus ~$50/mo) |
|----------|-----------|---------------------|
| IDE queries | Unlimited | Unlimited |
| API credits | 2,500/month | 50,000/month |
| Estimated credits/query | ~10 | ~10 |
| Programmatic queries/month | ~250 | ~5,000 |
| Row limit per result | ~1,000,000 | ~1,000,000 |

`collect_dune_auto.py` tracks credit usage per key in the checkpoint file and sleeps until the next calendar month when the budget is exhausted, so it is safe to leave running unattended indefinitely.

### Bitquery

| Resource | Free tier |
|----------|-----------|
| Rate limit | ~10 requests/min |
| Data window | Last few hours only |
| Historical access | Not available |

The pipeline auto-throttles via `ratelimit` and retries with exponential backoff.

---

## Troubleshooting

**`dune-client` not installed**
```bash
pip install dune-client
```

**`--status` shows 0 windows on a fresh run**
That is correct — the checkpoint starts empty. Once you run a collection it will populate.

**Dune query returns empty results**
- Verify the date range (pump.fun launched January 2024)
- Try `--raw-creations` if the `pump_fun_solana` decoded schema is unavailable
- Check Dune status at [dune.com/status](https://dune.com/status)

**Truncation warning in logs**
`collect_dune_auto.py` automatically splits the window. No action needed — the split sub-windows are collected transparently.

**Credit budget exhausted mid-run**
The script sleeps until the 1st of next month and resumes automatically. With `--keys-file`, other workers continue while the exhausted key sleeps.

**Bitquery 429 (rate limited)**
The pipeline auto-retries. Reduce `RATE_LIMIT_CALLS` in `config.py` if it persists. Use Dune to backfill any missed windows.

**Parquet files from different backends have mismatched columns**
```python
from schemas import coerce_schema, TRADE_COLS
df = coerce_schema(df, TRADE_COLS)
```

**Running out of disk space**
- Each daily trade file is ~50–500 MB (peak days up to 1.5 GB)
- 500 days of trades ≈ 100–500 GB total
- Use `--types creations migrations` first to collect the cheap data while planning storage for trades

---

## License

Intended for academic research. All on-chain data is publicly available. Wallet addresses should be anonymized in any publication.
