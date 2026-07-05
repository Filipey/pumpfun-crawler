# pump.fun + PumpSwap historical crawler

Collects pump.fun bonding-curve and PumpSwap AMM history (token creates, trades,
graduations, pool creations) for research into token launch dynamics,
manipulation/insider taxonomy, and post-graduation trading behavior.

Target window: **2025-11-01 → 2026-07-31** (backfill runs up to "today" and is
safely resumable later for the remaining days).

Background reading this schema replicates/extends:

> Marino et al. (2026), *"Predicting the success of new crypto-tokens: the Pump.fun case"*, arXiv:2602.14860

> **Institution**: UFMG — Departamento de Ciência da Computação, LOCUS (Social Computing Laboratory)

---

## Why this isn't a plain RPC crawler

pump.fun's bonding curve alone does ~2–5M trades/day. Backfilling ~9 months by
calling `getSignaturesForAddress`/`getTransaction` one transaction at a time
(the original approach — still in `collect_rpc.py`, kept as a documented
last-resort fallback) means hundreds of millions of individual RPC calls,
which is why Helius and other RPC vendors kept failing/timing out. There is no
free bulk API that covers this exact window either — BigQuery's public Solana
dataset died in March 2025, Flipside shut down in June 2026, and paid vendors
(Bitquery, Dune) charge per GB/row at a volume that adds up fast.

The actual fix: **[SolArchive](https://huggingface.co/datasets/solarchive/solarchive)**
(free, public, Hugging Face, daily Parquet dumps of the full Solana chain,
currently covering Oct 2020 → Dec 5 2025) covers the front slice of our window
for free, and **[Old Faithful](https://old-faithful.net/) + [Jetstreamer](https://github.com/anza-xyz/jetstreamer)**
(Solana Foundation/Anza's permanent chain archive + streaming replay tool)
covers everything from Dec 2025 onward, also for free — both are queried/
streamed and filtered down to just our two program IDs rather than bulk
downloaded, so neither requires paying a data vendor.

```
┌─ Phase A: SolArchive (2025-11-01 .. 2025-12-05) ───────────────┐
│  DuckDB httpfs, column-pruned remote reads, ~300 shards/day     │
│  filtered to accounts containing either program ID              │
└──────────────────────────────────────────────────────────────┘
                              │
┌─ Phase B: Old Faithful / Jetstreamer (2025-12-06 .. today) ────┐
│  epoch-by-epoch replay, custom plugin filters to the same       │
│  two program IDs, emits RPC-shaped tx dicts                     │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
              collectors/decoder.py (unchanged Borsh decoder)
                              │
                              ▼
              EntityStore → data/parquet/<table>/<table>_YYYY-MM-DD.parquet
                              │
                              ▼
              collectors/warehouse.py → data/warehouse.duckdb (SQL layer)
```

Both phases feed the exact same decoder and storage layer, so the rest of the
pipeline (event decoding, is_bot detection, storage, checkpointing) doesn't
care which phase produced a given day.

---

## Project layout

```
collect_solarchive.py        Phase A CLI (SolArchive backfill)
collect_rpc.py                Legacy/fallback: raw-RPC crawler (kept, not primary)
config.py                     Constants and env-var overrides
schemas.py                    Per-entity dtype definitions + dtype casts
collectors/
  solarchive_source.py        Phase A ingestion adapter (DuckDB httpfs + decode)
  decoder.py                  Anchor event decoders for both programs (Borsh + base58)
  day_store.py                EntityStore: batched Parquet writer + compaction
  checkpoint.py                Per-source, per-chunk resume state
  warehouse.py                 DuckDB view/materialization layer over Parquet
  rpc_client.py                Solana JSON-RPC client (kept for legacy path / spot checks)
data/
  parquet/
    tokens/tokens_2025-11-01.parquet
    pools/pools_2025-11-01.parquet
    graduations/graduations_2025-11-01.parquet
    bonding_curve_trades/bonding_curve_trades_2025-11-01.parquet
    pumpswap_trades/pumpswap_trades_2025-11-01.parquet
    …
  warehouse.duckdb            SQL-queryable view/materialized layer over the above
  checkpoint.json             Per-source per-day/epoch status ledger
  logs/                       Rotating log files, one per CLI
```

Each entity table is day-partitioned and deduplicated on flush — safe to
re-run a day without creating duplicates. See **Storage model** below for how
part files and compaction work.

---

## Quick start (Phase A — SolArchive, free, no API keys needed)

```bash
# 1. Clone and install
git clone <your-repo-url>
cd pumpfun-crawler
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Smoke-test: a handful of shards from one day, sequential, no concurrency
python collect_solarchive.py --day 2025-12-01 --max-shards 12

# 3. A full day, concurrently
python collect_solarchive.py --day 2025-12-01 --workers 4

# 4. Full backfill (2025-11-01 .. 2025-12-05, SolArchive's current coverage)
python collect_solarchive.py
```

Run inside `tmux` for anything beyond a single-day test:

```bash
tmux new -s pumpfun-backfill
source .venv/bin/activate
python collect_solarchive.py
# Ctrl+B D to detach; tmux attach -t pumpfun-backfill to reattach
```

Safe to interrupt (Ctrl-C) and rerun — checkpointed per day in
`data/checkpoint.json` under the `"solarchive"` source key.

---

## Configuration

All options have defaults in `config.py`. Override via `.env` (copy
`.env.example` first):

| Variable | Default | Description |
|---|---|---|
| `SOLARCHIVE_START` | `2025-11-01` | Start of Phase A window |
| `SOLARCHIVE_END` | `2025-12-05` | End of Phase A window (SolArchive's current coverage — do not extend past this without checking the dataset for new days) |
| `SOLARCHIVE_WORKERS` | `4` | Concurrent shard fetches. **Do not raise this casually** — see Memory notes below |
| `DATA_DIR` | `./data` | Root directory for all output |
| `RPC_ENDPOINTS` | public endpoint | Only used by the legacy `collect_rpc.py` fallback path |

---

## Memory notes (read this before raising `--workers`)

Two issues surfaced while building this and are both mitigated, but worth
understanding before scaling up on a bigger machine:

1. **Whole-day buffering (fixed).** An earlier version buffered an entire
   day's decoded events (hundreds of thousands of rows across ~300 shards) in
   memory before writing anything, and took the collection machine down under
   memory pressure. `EntityStore` now flushes to disk after **every** shard
   (`--flush-every 1` default) — each flush is a small append-only Parquet
   write, not a read-merge-rewrite, so this is cheap.
2. **Per-connection growth under long-lived DuckDB connections (mitigated,
   root cause unconfirmed).** Even with per-shard flushing, RSS was observed
   climbing steadily (not clearly plateauing) across ~15-20 shards on a single
   thread's reused DuckDB connection querying many distinct remote Parquet
   URLs — possibly `httpfs` internal metadata caching, possibly allocator
   high-water-marking, not conclusively isolated. Mitigation:
   `solarchive_source.py` now recycles each thread's DuckDB connection every
   `_RECYCLE_EVERY` (20) shard queries, which is cheap regardless of the true
   cause and removes the risk either way.

This was all diagnosed and fixed on a 14GB personal machine, which is a much
tighter environment than the intended deployment target. If you're running on
a server with 100GB+ RAM, these fixes still apply (they're not free even at
that scale — 304 shards/day × unbounded growth would eventually matter
regardless of headroom) but you have far more margin for error. Still, watch
`free -h` on the first full-day run at a new `--workers` value before leaving
it unattended in `tmux`.

---

## Storage model

Two layers:

- **Parquet (source of truth)** — day-partitioned, one directory per entity
  table. During ingestion, `EntityStore.flush_batch()` writes append-only part
  files (`<table>_<date>.partNNNNNN.parquet`, no read-merge-rewrite, so
  memory/IO cost is O(batch), not O(day)). Once a day is fully ingested,
  `EntityStore.compact_day()` merges all part files (+ any previously
  compacted file) into one deduplicated canonical file and deletes the parts.
  `collect_solarchive.py` does this automatically at the end of each day.
- **DuckDB (`data/warehouse.duckdb`)** — a SQL layer on top, via
  `collectors/warehouse.py`:
  ```python
  from collectors.warehouse import connect, refresh_views, materialize

  con = connect()
  refresh_views(con)     # cheap VIEWs over the Parquet globs, always current
  con.sql("SELECT count(*) FROM bonding_curve_trades").show()

  materialize(con, ["bonding_curve_trades"])  # slower, once a phase is done — faster repeat queries
  ```

### Entity tables

| Table | Key | Notes |
|---|---|---|
| `tokens` | `mint` | From CreateEvent: name, symbol, uri, creator, bonding_curve |
| `pools` | `pool` | From CreatePoolEvent: mint, creator (coin_creator at graduation) |
| `graduations` | `signature` | From CompleteEvent: mint, slot, block_time |
| `bonding_curve_trades` | `signature, tx_type` | buy/sell on the pump.fun bonding curve |
| `pumpswap_trades` | `signature, tx_type` | buy/sell on PumpSwap post-graduation |

Common trade columns: `signature, slot, block_time, mint, trader, tx_type,
is_buy, v_sol_in_bonding_curve, v_tok_in_bonding_curve, sol_amount,
token_amount, is_bot, coin_creator, coin_creator_fee, source_phase`.
`pumpswap_trades` additionally has `pool, lp_fee, protocol_fee`.

`source_phase` records which ingestion phase produced a row (`solarchive` |
`oldfaithful`), useful for spot-checking one phase against the other on
overlapping dates.

---

## Event decoding — field layout is version-sensitive

`collectors/decoder.py` decodes raw Borsh event bytes from `Program data:` log
lines. **The pump.fun and PumpSwap programs have both changed their event
struct layout multiple times** (verified against the on-chain IDL history in
[pump-fun/pump-public-docs](https://github.com/pump-fun/pump-public-docs) —
14 relevant commits land inside our Nov 2025–Jul 2026 window). All growth
observed so far is purely fields appended at the *end* of each struct, so a
single fixed-prefix reader (read our needed fields in order, ignore whatever
comes after) is correct across the whole window — but this is an empirical
finding, not a protocol guarantee. If `verify_dataset.py` (or manual
inspection) ever shows implausible values (e.g. garbage pubkeys, wildly wrong
reserve amounts) for trades after some date, re-run the discriminator/byte-
length probe described in the `decoder.py` module docstring against a real
transaction from that date before trusting the output further.

---

## isBot detection

Applies independently to each program, using the depth at which the program
was invoked (parsed from transaction logs):

- `is_bot = True` — program invoked at log depth `[1]` (direct call, no outer program) → likely bot
- `is_bot = False` — invoked at depth `[2]+` (through another program / dApp router) → likely human
- `is_bot = None` — depth not found in logs

---

## Legacy: raw-RPC crawler (`collect_rpc.py`)

Kept as a documented last-resort path and as the reference implementation for
the is_bot log-depth logic. Not recommended as a primary data source at this
volume — see "Why this isn't a plain RPC crawler" above. Usage is unchanged
from the original design: `python collect_rpc.py --day YYYY-MM-DD`,
`--status`, `--program {pumpfun,pumpswap}`, `--start`/`--end`, `--workers`.
Requires `RPC_ENDPOINTS` in `.env` (Helius or similar).

---

## Security

- `.env` and `keys.txt` are in `.gitignore` and **must never be committed**.
- Any RPC/API keys belong in `.env` only.
