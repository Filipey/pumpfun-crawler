# pumpfun-crawler

A **fully local** data collection pipeline for [pump.fun](https://pump.fun) (Solana memecoin launchpad), supporting academic research on pump-and-dump schemes, memecoin taxonomy, creator influence networks, and trader behavior modeling via Inverse Reinforcement Learning.

> **Institution**: UFMG — Departamento de Ciência da Computação, LOCUS (Social Computing Laboratory)

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Token Classification](#token-classification)
4. [Quick Start](#quick-start)
5. [Configuration](#configuration)
6. [Output Schema](#output-schema)
7. [Running as a Service](#running-as-a-service)
8. [Estimated Scale](#estimated-scale)
9. [Optional: Dune Analytics Cross-Validation](#optional-dune-analytics-cross-validation)

---

## Overview

**Data source**: The unofficial but stable pump.fun REST API (`https://frontend-api.pump.fun`).
No authentication required. No API key needed. No rate-limit quotas to worry about.

**Collection window**: January 1 – May 12, 2026 (132 days).

**What gets collected**:

| Category | Condition | Data collected |
|---|---|---|
| `graduated` | `token.complete == true` | Full metadata + **all trades** |
| `active` | `complete == false`, trade count > 10 | Full metadata + **all trades** |
| `dead` | `complete == false`, trade count ≤ 10 | Metadata row only (creation + final state) |

The threshold of 10 trades balances storage cost against analytical value: tokens with ≤ 10 trades carry almost no behavioral signal for IRL or network analysis.

---

## Architecture

```
collect.py  (main CLI)
│
├── collectors/discovery.py       Paginate /coins DESC; yield tokens in window
├── collectors/api_client.py      HTTP client: rate limiting + exponential backoff
├── collectors/token_processor.py Enrich + classify + fetch trades per token
├── collectors/checkpoint.py      JSONL checkpoint (resume from interruption)
├── collectors/storage.py         Thread-safe incremental Parquet writer
│
config.py                         Constants, env overrides
schemas.py                        Column definitions + dtype casts
```

**Concurrency model**: One discovery loop feeds a bounded `queue.Queue`; `WORKERS` parallel threads each own an independent HTTP session and pull from the queue. The storage layer is protected by a single mutex and auto-flushes every 5 000 rows.

```
Main thread                   Worker threads (WORKERS=4)
────────────────────          ──────────────────────────────
discover_tokens()             worker-0: process_token()
  → yield token dicts           → get_coin(mint)
  → queue.put(token)            → get_all_trades(mint)
                                → store.add_token/add_trades()
                              worker-1 … worker-N (same)
```

---

## Token Classification

pump.fun bonding curve mechanics:

```
y = 1,073,000,191 − 32,190,005,730 / (30 + x)   where x = SOL in pool
BCP = 100 − ((token_reserve − 206,900,000) × 100 / 793,100,000)
```

A token **graduates** (becomes `complete = true`) when the bonding curve fills to ~85 SOL (≈ $69 k market cap at ~$130/SOL), triggering migration to Raydium. About 2 % of all tokens graduate. The remaining 98 % are split:

- **Active non-grads**: had real trading activity (> 10 trades) — essential for modeling failed P&D attempts.
- **Dead tokens**: near-zero activity, created speculatively or as spam — useful for taxonomy but not for behavioral models.

---

## Quick Start

```bash
# 1. Clone and install
git clone <your-repo-url>
cd pumpfun-crawler
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. (Optional) copy env file and override defaults
cp .env.example .env
# edit .env if needed (all settings have sensible defaults)

# 3. Run the crawler
python collect.py

# 4. Check progress at any time (without interrupting the run)
python collect.py --status
```

To resume after interruption, just run `python collect.py` again — the checkpoint skips already-processed tokens automatically.

---

## Configuration

All options have defaults in `config.py`. Override via environment variables or `.env`:

| Variable | Default | Description |
|---|---|---|
| `COLLECT_START` | `2026-01-01` | Start of collection window (ISO date, inclusive) |
| `COLLECT_END` | `2026-05-12` | End of collection window (ISO date, inclusive) |
| `WORKERS` | `4` | Parallel worker threads |
| `DATA_DIR` | `./data` | Root directory for all output |

**Tuning `WORKERS`**:
- 4 workers: ~3–5 req/s sustained, safe for long overnight runs
- 8 workers: ~6–10 req/s, finishes faster, slight risk of hitting informal rate limits
- Do not exceed 16 — the pump.fun API will start returning 429s

---

## Output Schema

All output is written to `data/parquet/`.

### `tokens.parquet`

One row per token mint. Columns (selected):

| Column | Type | Description |
|---|---|---|
| `mint` | str | Token mint address (primary key) |
| `name` | str | Token name |
| `symbol` | str | Token symbol |
| `description` | str | Creator description |
| `image_uri` | str | IPFS/CDN image URL |
| `creator` | str | Creator wallet address |
| `created_timestamp` | Int64 | Unix ms — token creation time |
| `complete` | bool | `True` if graduated to Raydium |
| `raydium_pool` | str | Raydium pool address (if graduated) |
| `usd_market_cap` | float64 | Market cap in USD at collection time |
| `virtual_sol_reserves` | Int64 | SOL in bonding curve (lamports) |
| `virtual_token_reserves` | Int64 | Tokens in bonding curve (raw) |
| `trade_count` | Int64 | Total trade count |
| `category` | str | `"graduated"` / `"active"` / `"dead"` |
| `collected_at` | Int64 | Unix ms when we fetched it |

### `trades_graduated.parquet` / `trades_active.parquet`

One row per trade. Columns:

| Column | Type | Description |
|---|---|---|
| `signature` | str | Transaction signature (primary key) |
| `mint` | str | Token mint address |
| `sol_amount` | Int64 | SOL amount in lamports |
| `token_amount` | Int64 | Token amount in raw units |
| `is_buy` | bool | `True` = buy, `False` = sell |
| `user` | str | Trader wallet address |
| `timestamp` | Int64 | Unix seconds |
| `tx_index` | Int64 | Transaction index in block |
| `username` | str | Optional social handle |
| `profile_image` | str | Optional profile image URL |

---

## Running as a Service

### tmux (interactive, recommended during first run)

```bash
tmux new -s pumpfun
source /path/to/.venv/bin/activate
python collect.py
# Ctrl+B D to detach; tmux attach -t pumpfun to reattach
```

### systemd (persistent background service)

```ini
# /etc/systemd/system/pumpfun-crawler.service
[Unit]
Description=pump.fun data crawler
After=network.target

[Service]
Type=simple
User=filipe
WorkingDirectory=/home/filipe/Pesquisas/Mestrado/pumpfun-crawler
ExecStart=/home/filipe/Pesquisas/Mestrado/pumpfun-crawler/.venv/bin/python collect.py
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable pumpfun-crawler
sudo systemctl start pumpfun-crawler
journalctl -u pumpfun-crawler -f  # follow logs
```

### cron (scheduled, e.g. restart daily)

```bash
# crontab -e
# Restart every day at 6 AM; flock prevents overlapping runs
0 6 * * * flock -n /tmp/pumpfun.lock /home/filipe/Pesquisas/Mestrado/pumpfun-crawler/.venv/bin/python /home/filipe/Pesquisas/Mestrado/pumpfun-crawler/collect.py >> /home/filipe/Pesquisas/Mestrado/pumpfun-crawler/logs/cron.log 2>&1
```

---

## Estimated Scale

| Window | Est. tokens | Graduated | Active non-grad | Dead |
|---|---|---|---|---|
| Jan 1 – May 12, 2026 (132 days) | ~3.4 M | ~66 k (2%) | ~700 k (20%) | ~2.6 M (78%) |

Approximate storage:

| File | Rows | Est. size |
|---|---|---|
| `tokens.parquet` | ~3.4 M | ~2 GB |
| `trades_graduated.parquet` | ~50–200 M | ~20–80 GB |
| `trades_active.parquet` | ~30–100 M | ~12–40 GB |

Runtime at 4 workers (conservative estimate): **3–7 days** for the full window.

---

## Optional: Dune Analytics Cross-Validation

These SQL queries are provided for **one-time manual validation only** — run them in the [Dune IDE](https://dune.com/queries) and compare counts against your local data. No API key or automation needed.

### Graduated tokens (migrations to Raydium) — Jan 1 to May 12, 2026

```sql
-- Run in: https://dune.com/queries (new query)
-- Table: dex_solana.trades (decoded by Dune Spellbook)
SELECT
    DATE_TRUNC('day', block_time)   AS day,
    COUNT(DISTINCT token_bought_mint_address) AS tokens_with_trades,
    COUNT(*)                         AS total_trades,
    SUM(amount_usd)                  AS volume_usd
FROM dex_solana.trades
WHERE project = 'pumpdotfun'
  AND block_time >= TIMESTAMP '2026-01-01'
  AND block_time <  TIMESTAMP '2026-05-13'
GROUP BY 1
ORDER BY 1;
```

### Graduation (migration) events — Jan 1 to May 12, 2026

```sql
-- Uses solana.instruction_calls (raw Solana instructions)
-- pump.fun program: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
SELECT
    DATE_TRUNC('day', block_time)   AS day,
    COUNT(*)                         AS migrations
FROM solana.instruction_calls
WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
  AND data LIKE '0x%'          -- filter for migrate instruction discriminator
  AND block_time >= TIMESTAMP '2026-01-01'
  AND block_time <  TIMESTAMP '2026-05-13'
GROUP BY 1
ORDER BY 1;
```

### Token creation volume — Jan 1 to May 12, 2026

```sql
SELECT
    DATE_TRUNC('day', block_time) AS day,
    COUNT(*) AS tokens_created
FROM solana.instruction_calls
WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
  AND block_time >= TIMESTAMP '2026-01-01'
  AND block_time <  TIMESTAMP '2026-05-13'
  -- "create" instruction discriminator prefix:
  AND SUBSTR(data, 1, 18) = '0x181ec828051c0777'
GROUP BY 1
ORDER BY 1;
```

> **Note**: Dune Spellbook schemas evolve. If a query returns an error, check the [Dune documentation](https://docs.dune.com) or community forums for updated table names.
