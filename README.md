# pump.fun + PumpSwap RPC Crawler

Collects all on-chain events from both pump.fun phases directly from Solana via JSON-RPC and saves them as **day-partitioned Parquet files**:

- **Phase 1 — Bonding curve** (`6EF8…`): token creates, buys, sells, graduations
- **Phase 2 — PumpSwap AMM** (`pAMM…`): pool creation, AMM buys/sells post-graduation

Replicates the bonding curve dataset described in:

> Marino et al. (2026), *"Predicting the success of new crypto-tokens: the Pump.fun case"*, arXiv:2602.14860

> **Institution**: UFMG — Departamento de Ciência da Computação, LOCUS (Social Computing Laboratory)

---

## How it works

```
┌─ pump.fun launchpad (6EF8…) ──────────────────┐
│ getSignaturesForAddress → paginate backward    │
│ getTransaction (batched 100/call)              │
│ Anchor decoder: CreateEvent, TradeEvent,       │
│                 CompleteEvent                  │
└───────────────────────────────────────────────┘
                    │
                    ▼
┌─ PumpSwap AMM (pAMM…) ────────────────────────┐
│ getSignaturesForAddress → paginate backward    │
│ getTransaction (batched 100/call)              │
│ Anchor decoder: CreatePoolEvent,               │
│                 BuyEvent, SellEvent            │
└───────────────────────────────────────────────┘
                    │
                    ▼
         DayStore (dedup + merge)
                    │
                    ▼
  data/parquet/transactions_YYYY-MM-DD.parquet
```

The crawler paginates **newest → oldest** for each program, flushing a day's Parquet file as soon as the cursor crosses into the previous day. You can analyse completed days while collection is still running.

Both programs share the same output files — the `source` column (`"pumpfun"` / `"pumpswap"`) distinguishes them.

---

## Project layout

```
collect_rpc.py              Main CLI entry point
config.py                   Constants and env-var overrides
schemas.py                  Column definitions + dtype casts
collectors/
  rpc_client.py             Solana JSON-RPC client (batching + retry + rotation)
  decoder.py                Anchor event decoders for both programs (Borsh + base58)
  day_store.py              Day-partitioned Parquet writer with dedup + enrichment
  checkpoint.py             JSON-backed resume state (two cursors + done days)
data/
  parquet/
    transactions_2026-01-01.parquet   ← one file per day (both sources)
    …
    mint_creators.parquet             ← mint → coin_creator (bonding curve join)
    pool_mints.parquet                ← pool address → mint (PumpSwap join)
  checkpoint.json                     ← pumpfun_cursor, pumpswap_cursor, done days
```

---

## Quick start

```bash
# 1. Clone and install
git clone <your-repo-url>
cd pumpfun-crawler
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure RPC endpoint
cp .env.example .env
# Edit .env — set RPC_ENDPOINTS (see below)

# 3. Smoke-test with a single day
python collect_rpc.py --day 2026-05-01

# 4. Full run (both programs, Jan 1 – May 12 2026)
python collect_rpc.py
```

---

## Getting a free RPC endpoint

The public endpoint (`https://api.mainnet-beta.solana.com`) is heavily rate-limited. For real collection you need at least one dedicated endpoint.

**Helius** (recommended) — free tier gives ~100 req/s:
1. Sign up at https://dev.helius.xyz
2. Create a Mainnet API key
3. Add to `.env`:
   ```
   RPC_ENDPOINTS=https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
   ```

Multiple endpoints (comma-separated) are rotated round-robin for higher throughput:
```
RPC_ENDPOINTS=https://mainnet.helius-rpc.com/?api-key=KEY1,https://api.mainnet-beta.solana.com
```

---

## Running

```bash
# Full collection — both pump.fun and PumpSwap
python collect_rpc.py

# Check progress without collecting
python collect_rpc.py --status

# Single day only (good for smoke-testing)
python collect_rpc.py --day 2026-05-01

# Only one program
python collect_rpc.py --program pumpfun
python collect_rpc.py --program pumpswap

# Custom date range
python collect_rpc.py --start 2026-03-01 --end 2026-03-31

# More parallel decode workers
python collect_rpc.py --workers 8
```

The crawler is **safe to interrupt** (Ctrl-C). Each program's cursor is saved independently in `data/checkpoint.json`. Restart with the same command to resume where it stopped.

---

## Configuration

All options have defaults in `config.py`. Override via `.env`:

| Variable | Default | Description |
|---|---|---|
| `RPC_ENDPOINTS` | public endpoint | Comma-separated RPC URLs |
| `COLLECT_START` | `2026-01-01` | Start of collection window (inclusive) |
| `COLLECT_END` | `2026-05-12` | End of collection window (inclusive) |
| `WORKERS` | `4` | Parallel decode workers |
| `DATA_DIR` | `./data` | Root directory for all output |

---

## Output schema

One row per on-chain event. Bonding curve columns match the paper (§IV); PumpSwap columns are additive.

| Column | Type | Description |
|---|---|---|
| `signature` | str | Transaction hash |
| `slot` | Int64 | Solana slot number |
| `timestamp` | Int64 | Unix seconds (block time) |
| `date` | str | YYYY-MM-DD (partition key) |
| `source` | str | `"pumpfun"` or `"pumpswap"` |
| `mint` | str | Token mint address |
| `coin_creator` | str | Creator wallet |
| `bonding_curve` | str | Bonding curve PDA (NULL for PumpSwap rows) |
| `pool` | str | PumpSwap pool address (NULL for bonding curve rows) |
| `trader` | str | Wallet executing the trade |
| `tx_type` | str | `create` / `buy` / `sell` / `complete` / `pool_create` |
| `is_buy` | bool | True=buy, False=sell, None=create/complete |
| `in_bonding_curve` | bool | True for bonding curve events, False for PumpSwap |
| `v_sol_in_bonding_curve` | Int64 | SOL reserves after tx (lamports) |
| `v_tok_in_bonding_curve` | Int64 | Token reserves after tx (raw units) |
| `sol_amount` | Int64 | SOL exchanged (lamports) |
| `token_amount` | Int64 | Tokens exchanged (raw units) |
| `is_bot` | bool | True if invoked at depth [1] (direct call, likely bot) |
| `lp_fee` | Int64 | LP fee in lamports (PumpSwap only) |
| `protocol_fee` | Int64 | Protocol fee in lamports (PumpSwap only) |
| `coin_creator_fee` | Int64 | Creator revenue share per trade (PumpSwap only) |

### Side tables

| File | Key | Description |
|---|---|---|
| `mint_creators.parquet` | `mint` | mint → creator wallet; join to fill `coin_creator` in bonding curve trade rows |
| `pool_mints.parquet` | `pool` | pool address → mint; join to fill `mint` in PumpSwap rows where the pool was created outside the collection window |

### Enrichment example

```python
import pandas as pd

day = pd.read_parquet("data/parquet/transactions_2026-03-15.parquet")

# Fill coin_creator on bonding curve trades
creators = pd.read_parquet("data/parquet/mint_creators.parquet")
day = day.merge(creators, on="mint", how="left", suffixes=("", "_mc"))
day["coin_creator"] = day["coin_creator"].fillna(day["coin_creator_mc"])
day.drop(columns=["coin_creator_mc"], inplace=True)

# Fill mint on PumpSwap rows (if pool pre-dates collection window)
pool_mints = pd.read_parquet("data/parquet/pool_mints.parquet")
pumpswap = day["source"] == "pumpswap"
day = day.merge(pool_mints.rename(columns={"mint": "mint_pm"}), on="pool", how="left")
day.loc[pumpswap & day["mint"].isna(), "mint"] = day["mint_pm"]
day.drop(columns=["mint_pm"], inplace=True)
```

---

## Running as a service

### tmux (recommended during initial run)

```bash
tmux new -s pumpfun
source .venv/bin/activate
python collect_rpc.py
# Ctrl+B D to detach; tmux attach -t pumpfun to reattach
```

### systemd (persistent background service)

```ini
# /etc/systemd/system/pumpfun-crawler.service
[Unit]
Description=pump.fun + PumpSwap RPC crawler
After=network.target

[Service]
Type=simple
User=filipe
WorkingDirectory=/home/filipe/Pesquisas/Mestrado/pumpfun-crawler
ExecStart=/home/filipe/Pesquisas/Mestrado/pumpfun-crawler/.venv/bin/python collect_rpc.py
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable pumpfun-crawler
sudo systemctl start pumpfun-crawler
journalctl -u pumpfun-crawler -f   # follow logs
```

---

## Estimated scale (Jan 1 – May 12, 2026)

Based on the paper's September 2025 figures (~2–5M bonding curve trades/day):

| Program | Events/day | 132-day total | Est. Parquet (zstd) |
|---|---|---|---|
| pump.fun bonding curve | ~2–5M | ~260M–660M | ~50–200 GB |
| PumpSwap AMM | ~200k–1M | ~26M–132M | ~5–30 GB |

Approximate runtime at 10 req/s (Helius free tier): **3–10 days** for both programs.

---

## isBot detection

Applies independently to each program:

- `is_bot = True` — program invoked at log depth `[1]` (direct call, no outer program) → likely bot
- `is_bot = False` — invoked at depth `[2]+` (through another program / dApp router) → likely human
- `is_bot = None` — depth not found in logs

---

## Optional: Dune cross-validation

Copy-paste into the [Dune IDE](https://dune.com/queries) to cross-check daily counts against local data.

```sql
-- Bonding curve daily trade counts
SELECT
    DATE_TRUNC('day', block_time) AS day,
    COUNT(*) AS trades
FROM dex_solana.trades
WHERE project = 'pumpdotfun'
  AND block_time >= TIMESTAMP '2026-01-01'
  AND block_time <  TIMESTAMP '2026-05-13'
GROUP BY 1 ORDER BY 1;
```

```sql
-- PumpSwap daily trade counts
SELECT
    DATE_TRUNC('day', block_time) AS day,
    COUNT(*) AS trades
FROM dex_solana.trades
WHERE project = 'pump_amm'
  AND block_time >= TIMESTAMP '2026-01-01'
  AND block_time <  TIMESTAMP '2026-05-13'
GROUP BY 1 ORDER BY 1;
```

---

## Security

- `.env` and `keys.txt` are in `.gitignore` and **must never be committed**.
- RPC API keys belong in `.env` only.
