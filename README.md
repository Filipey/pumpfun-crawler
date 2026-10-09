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

On-chain collection gives us `tokens.uri` — a pointer, not content. **Phase C**
follows those pointers:

```
┌─ Phase C: off-chain metadata + images (any time after A/B) ────┐
│  tokens.uri --CID--> IPFS gateway pool --> token_metadata      │
│                              │                                  │
│                              └--> image --> 384px WebP          │
│                                     └--> Parquet blob shards    │
│  mint --> frontend-api-v3.pump.fun --> token_pumpfun_api        │
└──────────────────────────────────────────────────────────────┘
```

Both phases feed the exact same decoder and storage layer, so the rest of the
pipeline (event decoding, is_bot detection, storage, checkpointing) doesn't
care which phase produced a given day.

---

## Project layout

```
collect_solarchive.py        Phase A CLI (SolArchive backfill)
collect_oldfaithful.py        Phase B CLI (Old Faithful/Jetstreamer backfill)
collect_metadata.py           Phase C CLI (off-chain metadata, images, moderation flags)
collect_rpc.py                Legacy/fallback: raw-RPC crawler (kept, not primary)
config.py                     Constants and env-var overrides
schemas.py                    Per-entity dtype definitions + dtype casts
collectors/
  solarchive_source.py        Phase A ingestion adapter (DuckDB httpfs + decode)
  oldfaithful_source.py        Phase B ingestion adapter (reads plugin JSONL + decode)
  epoch_utils.py                Calendar date <-> Solana epoch estimation for Phase B
  decoder.py                  Anchor event decoders for both programs (Borsh + base58)
  day_store.py                EntityStore: batched Parquet writer + compaction
  checkpoint.py                Per-source, per-chunk resume state
  warehouse.py                 DuckDB view/materialization layer over Parquet
  metadata_source.py           Phase C: gateway walk, image fetch/resize, pump.fun API
  image_headers.py             Phase C: header-only image dimension parser
  blob_store.py                Phase C: content-addressed Parquet image blob shards
  rpc_client.py                Solana JSON-RPC client (kept for legacy path / spot checks)
pumpfun_filter_plugin/        Rust Jetstreamer plugin for Phase B (see its own README —
                               UNCOMPILED/UNVERIFIED, needs a Rust+Clang16+RocksDB toolchain)
data/
  parquet/
    tokens/tokens_2025-11-01.parquet
    pools/pools_2025-11-01.parquet
    graduations/graduations_2025-11-01.parquet
    bonding_curve_trades/bonding_curve_trades_2025-11-01.parquet
    pumpswap_trades/pumpswap_trades_2025-11-01.parquet
    token_metadata/token_metadata_2025-11-01.parquet
    token_images/token_images_2025-11-01.parquet
    token_pumpfun_api/token_pumpfun_api_2025-11-01.parquet
    …
  images/                     Image blob shards (shard_NNNNNN.parquet); IMAGES_DIR
  warehouse.duckdb            SQL-queryable view/materialized layer over the above
  checkpoint.json             Per-source per-day/epoch status ledger
  logs/                       Rotating log files: one per CLI, and one per
                              Phase C stage (metadata.log, images.log, api.log)
```

Each entity table is day-partitioned and deduplicated on flush — safe to
re-run a day without creating duplicates. See **Storage model** below for how
part files and compaction work.

---

## Phase C: off-chain metadata + images

`collect_metadata.py` resolves what the chain only points at: each token's
metadata JSON, its image, and pump.fun's moderation flags — the inputs for
multimodal taxonomy classification. Three independent, separately-checkpointed
stages over the same day partitions:

```
metadata   tokens.uri            -> token_metadata      (~45 req/s)
images     token_metadata.image  -> token_images + image blob shards (~27 tok/s)
api        frontend-api-v3       -> token_pumpfun_api   (unavailable since 2026-09-29)
```

```bash
python collect_metadata.py --status
python collect_metadata.py --stage metadata --limit 10000   # pilot first
python collect_metadata.py --stage metadata                 # then the full range
python collect_metadata.py --stage images
python collect_metadata.py --stage api
```

Run `--stage metadata` to completion before `--stage images`: it's the cheap
half, it yields the description/twitter text features days earlier, and it
tells you how many images actually exist before you commit disk.

**`tokens.uri` is a CID carrier, not a URL.** ~70% of rows point at `ipfs.io`,
which now returns HTTP 429 for everything ("switching to a service worker
gateway only"). The collector extracts the CID and walks a gateway pool
(pump.fun's own Pinata gateway first, then fallbacks). A collector that just
GETs the recorded URI fails on ~70% of tokens.

**Images are downloaded whole and resized locally**, not fetched through the
gateway's server-side transform. The transform is 12x less bandwidth but
resizes before we see any bytes and leaks no source dimensions, so keeping the
original dimensions would cost a second request — at which point downloading
the original is *faster* (26.7 vs 19.6 tok/s) and smaller on disk.
`--image-strategy transform` picks the low-bandwidth path for metered links.

**Image bytes go to Parquet blob shards** (`IMAGES_DIR`, HuggingFace `datasets`
layout), content-addressed by SHA-256 so reused artwork is stored once (~17%
of a sampled 100). `token_images` is the ledger; the filesystem is never
scanned to decide what still needs fetching.

Sizing at ~9.3M tokens: **~145 GB** on disk (~140 GB of it images), ~2.5 TB of
transient download, ~3 days with the gateway pool. Set `IMAGES_DIR` to a
different disk than `DATA_DIR` if needed — everything else is a few GB.

Every measurement behind these choices, and the trade-offs they resolve, is in
**Phase C methodology** below.

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

## Quick start (Phase B — Old Faithful/Jetstreamer, 2025-12-06 onward)

Covers the part of the window SolArchive doesn't reach. **Requires building
the Rust plugin first** — see `pumpfun_filter_plugin/README.md`. This plugin
was written without a Rust toolchain available and has not been compiled or
run against real data; treat the first build/run on the lab server as part
of bringing this phase up, not as a known-working path yet.

```bash
# 1. Build the plugin (needs Clang 16 + RocksDB build deps — see
#    pumpfun_filter_plugin/README.md and jetstreamer's own README)
cd pumpfun_filter_plugin && cargo build --release && cd ..

# 2. Smoke-test: a single epoch (~2 days of chain history)
python collect_oldfaithful.py --epoch 990

# 3. Full backfill (2025-12-06 .. today, resumable per-epoch)
python collect_oldfaithful.py
```

Epoch boundaries for a date range are estimated via `collectors/epoch_utils.py`
(calibrated against a live public RPC's current slot/blocktime, padded by a
few epochs on each side — precision doesn't matter much since every
transaction is bucketed into its correct calendar day using its own
`block_time`, not the epoch it happened to come from). Safe to interrupt and
rerun — checkpointed per epoch under the `"oldfaithful"` source key, same
`data/checkpoint.json`.

---

## Quick start (Phase C — off-chain metadata + images)

Runs against whatever `tokens_*.parquet` already exists, so it can start as
soon as any Phase A/B day is compacted. Needs no API keys.

```bash
# 0. what exists and what's pending, per stage
python collect_metadata.py --status

# 1. pilot: 10k mints/day. Measures throughput and success rate on real data,
#    and deliberately does NOT mark days done, so a real run redoes them fully.
python collect_metadata.py --stage metadata --limit 10000

# 2. metadata to completion — the cheap half (~45 req/s). Do this BEFORE images:
#    it yields the description/twitter text features days earlier, and tells you
#    how many images actually exist before you commit ~140 GB to them.
python collect_metadata.py --stage metadata

# 3. image pilot — confirm the ~17.6 KB mean and ~17% dedup against your data
python collect_metadata.py --stage images --limit 10000

# 4. images (the slow half, ~27 tok/s)
python collect_metadata.py --stage images

# 5. pump.fun moderation flags — CURRENTLY UNAVAILABLE (see below): pump.fun
#    withdrew GET /coins/<mint> on or before 2026-09-29. The stage preflights
#    and refuses to run, writing nothing. Left in place in case it returns.
python collect_metadata.py --stage api
```

Useful flags:

| Flag | Effect |
|---|---|
| `--stage all` | metadata → images → api in sequence |
| `--day YYYY-MM-DD` | one day only |
| `--limit N` | at most N mints/day; days stay pending |
| `--dry-run` | fetch but write nothing; checkpoint untouched |
| `--retry-failed` | re-drive transient failures, skip terminal ones |
| `--image-strategy transform` | ~10× less download for ~1.5× the wall clock |
| `--no-dedup` | drop the dedup indexes (~1–2 GB RSS at full scale) |
| `--reset YYYY-MM-DD …` | revert day(s) of `--stage` to pending |
| `--force-accept` | finalize a day even under the 50% success floor |

The window defaults to the earliest and latest day that actually has a tokens
file, so a run never silently skips days collected past a hard-coded constant.

### Running stages in parallel

`metadata` is fast and `images` is slow, so the useful arrangement is to let
images work through the days metadata has already finished while metadata
keeps moving forward:

```bash
# terminal 1 — works forward through the whole window
python collect_metadata.py --stage metadata

# terminal 2 — point it at the whole window; it handles the rest
python collect_metadata.py --stage images
```

No window juggling is needed. `images` may only touch days `metadata` has
marked `done`, which is exactly the condition under which that day's
`token_metadata` file is complete (metadata marks a day done only after
`compact_day()`). Days that aren't ready are **left pending**, the run reports
how many, and a later run picks them up:

```
Stage 'images': 2 day(s) left pending because 'metadata' has not finished them
yet — rerun this command once 'metadata' has advanced and they will be picked up.
```

So if images catches up with metadata, just rerun it when metadata has moved
on — nothing is lost and nothing is double-collected. `--status` shows each
stage's frontier.

Concurrency itself is safe: checkpoint writes take an inter-process lock
around a re-read/merge/write, and each stage compacts only the table it owns.

Note the rate limiters are **per process**, so two stages running together
allow `GATEWAY_RATE_LIMIT` each against the same gateway pool. Halve it if you
want to keep the combined rate where it was.

`--force-accept` bypasses the prerequisite gate. Only useful if a complete
`token_metadata` exists for days the checkpoint does not record as done — a
lost `checkpoint.json`, say.

### Logs

Each Phase C stage writes its own rotating log under `$DATA_DIR/logs/`:
`metadata.log`, `images.log`, `api.log` (50 MB × 10 backups, appended across
runs). Separate files because the stages run at very different speeds —
metadata takes hours, images days — and `--stage all` would otherwise
interleave a multi-day image crawl with everything else.

Every day logs a **status breakdown**, not just an ok count:

```
[2025-11-01] metadata status breakdown: ok=27 (79.4%)  unsupported_uri=5 (14.7%)  no_uri=2 (5.9%)
[2025-11-01] images status breakdown: ok=27 (100.0%)
[2025-11-01] images source formats: WEBP=16  PNG=6  JPEG=5
[2025-11-01] blob store: {'distinct_blobs': 15, 'open_shard': 'shard_000000.parquet', ...}
```

This is the audit trail for collection quality. A day at 90% ok with the rest
in `unsupported_uri` is healthy; the same 90% with the rest in `timeout` means
a gateway was struggling and the day is worth re-driving with `--retry-failed`.
Those two are indistinguishable from the ok ratio alone.

`data/checkpoint.json` is the machine-readable counterpart — it records
`attempted`, `ok` and per-table row counts for every (stage, day), which is
what to cite for collection provenance rather than re-deriving it from logs.

Safe to interrupt and rerun: checkpointed per day per stage under the
`"metadata"`, `"images"` and `"api"` source keys, and mints already carrying a
terminal status are skipped. A day whose success ratio falls below 50% is
**not** marked done — that is a gateway outage mid-run, not a day of bad
tokens, and finalizing it would hide the gap permanently.

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

Phase C (`collect_metadata.py`):

| Variable | Default | Description |
|---|---|---|
| `IMAGES_DIR` | `$DATA_DIR/images` | Image blob shards. **Deliberately separate from `DATA_DIR`** — ~140 GB at full scale while everything else is a few GB, so it often belongs on another disk |
| `METADATA_GATEWAYS` | 4-gateway pool | Comma-separated, tried in order. Default: `pump.mypinata.cloud`, `ipfs.filebase.io`, `ipfs.raribleuserdata.com`, `gateway.pinata.cloud` |
| `IMAGE_STRATEGY` | `original` | `original` (download + resize locally) or `transform` (gateway resizes server-side) |
| `IMG_TARGET_PX` | `384` | Max side of stored images. **Changing this means re-collecting** |
| `IMG_WEBP_QUALITY` | `85` | |
| `IMG_MAX_ORIG_BYTES` | `4194304` | Above this the original is not downloaded in full |
| `IMG_HEADER_PREFIX` | `4096` | Bytes read for the header-only dimension parse |
| `IMG_SHARD_ROWS` | `10000` | Rows per blob shard (~160 MB) |
| `METADATA_WORKERS` / `IMAGE_WORKERS` / `API_WORKERS` | `32` | Concurrency per stage |
| `GATEWAY_RATE_LIMIT` | `30` | req/s **per gateway host** |
| `PUMP_API_RATE_LIMIT` | `50` | req/s against `frontend-api-v3.pump.fun` |
| `HTTP_TIMEOUT` / `HTTP_RETRIES` | `30` / `3` | |
| `FLUSH_EVERY_MINTS` | `5000` | Flush buffered rows to disk every N mints |
| `CRAWLER_UA` | project UA | **Put a real contact address here before a multi-day run** |

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

## Data schemas

Every table is day-partitioned Parquet, zstd-compressed, one directory per
table under `data/parquet/`. Types below are the pandas dtypes enforced by
`schemas.apply_dtypes()`; `Int64`/`bool` are nullable extension dtypes, so a
missing value is `<NA>`, never a silent `0` or `False`.

Ordering of columns matches `schemas.py`, which is the single source of truth —
`ENTITY_TABLES` maps each table to its dtype dict and dedup key, and everything
downstream (`EntityStore`, `warehouse.refresh_views()`) iterates that mapping
generically.

### Relationships

```
tokens (mint) ─┬─< bonding_curve_trades (mint)
               ├─< graduations (mint)
               ├─< pools (mint) ─< pumpswap_trades (pool, mint)
               ├─1 token_metadata (mint) ─┐
               ├─1 token_images (mint) ───┼─> images/shard_*.parquet (sha256)
               └─1 token_pumpfun_api (mint)
```

`tokens.mint` is the join key for the entire dataset. The three Phase C tables
are 1:1 with `tokens` (one row per mint, whatever the outcome — a failed fetch
still writes a row carrying its `status`, so absence in the table means "not
attempted", never "attempted and failed").

---

### `tokens` — one row per token launch

From `CreateEvent` on the pump.fun bonding-curve program. Dedup key: `mint`.

| Column | Type | Description |
|---|---|---|
| `mint` | `str` | Token mint address. **Primary key for the whole dataset.** |
| `coin_creator` | `str` | Wallet entitled to creator-fee revenue. Usually equals the submitting wallet, but not always — the program added a distinct `creator` field separate from `user` |
| `bonding_curve` | `str` | Bonding-curve PDA address |
| `name` | `str` | On-chain token name |
| `symbol` | `str` | On-chain ticker |
| `uri` | `str` | Off-chain metadata pointer. **Not a reliable URL** — see Phase C |
| `created_slot` | `Int64` | Solana slot of the create transaction |
| `created_at` | `Int64` | Unix seconds, from the block's `block_time` (not the event's own `timestamp` field, for consistency across phases) |
| `created_signature` | `str` | Transaction signature of the create |

### `pools` — one row per PumpSwap AMM pool

From `CreatePoolEvent`. Dedup key: `pool`.

| Column | Type | Description |
|---|---|---|
| `pool` | `str` | Pool address (primary key) |
| `mint` | `str` | FK → `tokens.mint` |
| `creator` | `str` | `coin_creator` at the time of pool creation |
| `created_slot` | `Int64` | Slot of pool creation |
| `created_at` | `Int64` | Unix seconds |
| `created_signature` | `str` | Transaction signature |

### `graduations` — one row per bonding-curve completion

From `CompleteEvent` — the moment a token leaves the bonding curve for the AMM.
Dedup key: `signature`.

| Column | Type | Description |
|---|---|---|
| `mint` | `str` | FK → `tokens.mint` |
| `signature` | `str` | Transaction signature (primary key) |
| `slot` | `Int64` | Solana slot |
| `block_time` | `Int64` | Unix seconds |

### `bonding_curve_trades` — buys/sells on the pump.fun curve

From `TradeEvent` on the bonding-curve program. Dedup key: `signature, tx_type`
(one transaction can contain both a buy and a sell).

| Column | Type | Description |
|---|---|---|
| `signature` | `str` | Transaction signature |
| `slot` | `Int64` | Solana slot |
| `block_time` | `Int64` | Unix seconds |
| `mint` | `str` | FK → `tokens.mint` |
| `trader` | `str` | Wallet executing the trade (the event's `user` field) |
| `tx_type` | `str` | `buy` \| `sell` |
| `is_buy` | `bool` | `True` for buy, `False` for sell |
| `v_sol_in_bonding_curve` | `Int64` | Virtual SOL reserves **after** the trade, in lamports |
| `v_tok_in_bonding_curve` | `Int64` | Virtual token reserves after the trade, raw units |
| `sol_amount` | `Int64` | SOL exchanged, lamports |
| `token_amount` | `Int64` | Tokens exchanged, raw units (6 decimals) |
| `is_bot` | `bool` | Invocation-depth heuristic — see **isBot detection** below. `<NA>` when depth could not be determined |
| `coin_creator` | `str` | Creator wallet as reported by the event |
| `coin_creator_fee` | `Int64` | Creator revenue share paid on this trade, lamports |
| `source_phase` | `str` | `solarchive` \| `oldfaithful` — which ingestion phase produced this row |

### `pumpswap_trades` — buys/sells on the post-graduation AMM

Same 15 columns as `bonding_curve_trades`, plus:

| Column | Type | Description |
|---|---|---|
| `pool` | `str` | FK → `pools.pool` |
| `lp_fee` | `Int64` | LP fee paid, lamports |
| `protocol_fee` | `Int64` | Protocol fee paid, lamports |

`mint` is frequently absent from the on-chain PumpSwap event (which carries only
`pool`). `EntityStore` backfills it from a `pool → mint` index built from
`pool_create` events and from already-written `pools` Parquet files, so a trade
against a pool created on an earlier day still resolves its mint.

Note the `v_sol_in_bonding_curve` / `v_tok_in_bonding_curve` columns are reused
here for the AMM's reserve fields. The names are a legacy of the flat schema;
post-graduation they are pool reserves, not bonding-curve reserves.

---

### `token_metadata` — resolved off-chain metadata JSON (Phase C)

One row per mint. Dedup key: `mint`.

| Column | Type | Description |
|---|---|---|
| `mint` | `str` | FK → `tokens.mint` |
| `uri` | `str` | The raw `tokens.uri` this row was resolved from |
| `fetched_url` | `str` | The URL actually hit, after CID extraction and gateway re-routing |
| `gateway` | `str` | Host that served it — use this to audit gateway reliability per day |
| `meta_name` | `str` | Name from the JSON. **May differ from on-chain `tokens.name`** — the disagreement is itself a signal |
| `meta_symbol` | `str` | Symbol from the JSON |
| `description` | `str` | Free-text description. Primary text feature for taxonomy |
| `image_url` | `str` | Image pointer from the JSON — what Phase C's image stage follows |
| `image_cid` | `str` | IPFS CID extracted from `image_url`; `<NA>` for non-IPFS hosts |
| `twitter` | `str` | Usually an x.com profile or a specific status URL |
| `telegram` | `str` | |
| `website` | `str` | |
| `coin_community` | `str` | Newer pump.fun field; absent on older tokens |
| `video_url` | `str` | Rare (~1%) |
| `banner_url` | `str` | Rare (~1%) |
| `show_name` | `bool` | pump.fun display flag |
| `created_on` | `str` | Almost always `https://pump.fun`; other values indicate a third-party launchpad |
| `extra_json` | `str` | Any JSON keys not modelled above, verbatim, so nothing is lost |
| `status` | `str` | `ok` \| `no_uri` (column was empty) \| `unsupported_uri` (not http(s) and no extractable CID — e.g. `ar://`, `data:`; **terminal**, nothing was or ever will be attempted) \| `http_error` \| `timeout` \| `invalid_json` \| `all_gateways_failed` \| `crashed` (unexpected worker exception; retryable) |
| `http_status` | `Int64` | HTTP status of the last attempt |
| `attempts` | `Int64` | How many candidate URLs were tried before this outcome |
| `resolved_at` | `Int64` | Unix seconds when resolved |

### `token_images` — the image ledger (Phase C)

One row per mint; the **ledger**, not the bytes. Dedup key: `mint`.
Resumption, dedup and "what still needs fetching" are all answered from this
table — the filesystem is never scanned.

| Column | Type | Description |
|---|---|---|
| `mint` | `str` | FK → `tokens.mint` |
| `image_url` | `str` | The `token_metadata.image_url` this row was fetched from |
| `image_cid` | `str` | Extracted CID; `<NA>` for non-IPFS hosts |
| `sha256` | `str` | SHA-256 of the **stored** (re-encoded) bytes. **Join key into the blob shards** |
| `shard` | `str` | Which shard file holds the blob, e.g. `shard_000123.parquet` |
| `status` | `str` | `ok` \| `no_url` \| `unsupported` (not http(s) and no CID) \| `http_error` \| `timeout` \| `decode_error` \| `too_large` \| `crashed` (unexpected worker exception; retryable) |
| `http_status` | `Int64` | HTTP status of the successful (or last) attempt |
| `strategy` | `str` | `original_local` (downloaded and resized here) \| `gateway_transform` (server-side resize) \| `cid_dedup` (inherited from an already-stored identical CID) |
| `orig_bytes` | `Int64` | Size of the source image before re-encoding |
| `orig_width` | `Int64` | **Source** dimensions, before our resize |
| `orig_height` | `Int64` | |
| `orig_format` | `str` | `PNG` \| `JPEG` \| `WEBP` \| `GIF` \| `AVIF` \| `HEIF` \| `BMP` |
| `is_animated` | `bool` | `True` for animated GIF/APNG/WebP. The animation is **not** stored — only frame 0 — but the fact is recorded here |
| `n_frames` | `Int64` | Frame count of the source |
| `stored_bytes` | `Int64` | Size of the stored WebP |
| `stored_width` | `Int64` | Stored dimensions (≤ 384 on the long side) |
| `stored_height` | `Int64` | |
| `fetched_at` | `Int64` | Unix seconds |

### Image blob shards — the bytes (Phase C)

`$IMAGES_DIR/shard_NNNNNN.parquet`, ~10,000 rows each (~160 MB), in the layout
HuggingFace `datasets` expects for an image column. Content-addressed, so
reused artwork is stored once.

| Column | Type | Description |
|---|---|---|
| `sha256` | `str` | Primary key; joins to `token_images.sha256` |
| `bytes` | `BINARY` | The WebP image |
| `content_type` | `str` | `image/webp` |
| `width` | `int32` | |
| `height` | `int32` | |

```python
import duckdb
con = duckdb.connect()
con.sql("""
  SELECT t.mint, t.symbol, m.description, a.nsfw, a.is_banned, b.bytes
  FROM 'data/parquet/tokens/tokens_*.parquet' t
  JOIN 'data/parquet/token_metadata/token_metadata_*.parquet' m USING(mint)
  JOIN 'data/parquet/token_images/token_images_*.parquet' i USING(mint)
  LEFT JOIN 'data/parquet/token_pumpfun_api/token_pumpfun_api_*.parquet' a USING(mint)
  JOIN 'data/images/shard_*.parquet' b ON b.sha256 = i.sha256
  WHERE i.status = 'ok'
""")
```

### `token_pumpfun_api` — pump.fun moderation and market state (Phase C)

One row per mint, from `frontend-api-v3.pump.fun/coins/<mint>`. Dedup key: `mint`.

> ⛔ **Unavailable as of 2026-09-29: pump.fun withdrew `GET /coins/<mint>`.**
> The route answers `Cannot GET /coins/…` for every mint — including one taken
> from pump.fun's own listing endpoint a second earlier — and
> `/api/coins/<mint>` now returns `401`. The listing endpoint still carries
> these fields but caps out somewhere between offset 1000 and 3000, so it
> cannot be swept for a full token set. The stage preflights on one mint and
> refuses to run rather than writing a day of worthless rows; `metadata` and
> `images` are unaffected. It worked on 2026-09-22, so the window for this
> collection has closed unless a replacement surface turns up.
>
> ⚠️ **`nsfw` and `is_banned` are mutable moderation labels read at
> `fetched_at`, not attributes of the token at creation time.** Collecting in
> late 2026 for a Nov 2025–Jul 2026 window yields the *current* label. For the
> paper this must be described as "label as of collection date", and
> `fetched_at` is stored precisely so that date is recoverable per row.

| Column | Type | Description |
|---|---|---|
| `mint` | `str` | FK → `tokens.mint` |
| `nsfw` | `bool` | pump.fun's NSFW flag. **Observed near-zero positive rate** — verify before using as a label |
| `is_banned` | `bool` | pump.fun delisted the token |
| `reply_count` | `Int64` | Comments on the token's pump.fun page — a social-engagement proxy |
| `market_cap` | `float64` | Current market cap, USD |
| `ath_market_cap` | `float64` | All-time-high market cap, USD |
| `ath_market_cap_timestamp` | `Int64` | Unix **milliseconds** (the API's own unit; not normalised) |
| `total_supply` | `Int64` | Raw units |
| `is_currently_live` | `bool` | Livestreaming at fetch time |
| `hide_banner` | `bool` | |
| `api_image_uri` | `str` | pump.fun's own resolved image, often a CDN mirror on a different host than the metadata's `image` |
| `api_metadata_uri` | `str` | pump.fun's record of the metadata URI |
| `status` | `str` | `ok` \| `not_found` \| `endpoint_gone` (route withdrawn upstream; **not** terminal, since it describes pump.fun's API rather than the mint) \| `http_error` \| `timeout` \| `crashed` |
| `http_status` | `Int64` | |
| `fetched_at` | `Int64` | Unix seconds — **the timestamp the mutable labels refer to** |

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

This replicates the heuristic in Marino et al. (2026), §IV. It is a proxy, not
ground truth: a sophisticated bot routing through an aggregator is classified
human, and a power user calling the program directly is classified bot. Report
it as "direct-invocation share", not as a bot census.

---

## Phase C methodology — off-chain metadata and images

Everything in this section is empirical, measured 2026-09-22 against a sample
of n=100 real pump.fun tokens drawn across several pagination offsets of
`frontend-api-v3.pump.fun/coins`. These are the numbers to cite in a methods
section; re-run the measurement if collection happens much later, since the
gateway landscape moves.

### `tokens.uri` is a CID carrier, not a URL

Host distribution of `metadata_uri` in the sample:

| Host | Share |
|---|---|
| `ipfs.io` | 70% |
| `metadata.j7tracker.io` | 19% |
| `pump.mypinata.cloud` | 4% |
| `meta.uxento.io` | 3% |
| `m.rapidlaunch.io`, `gateway.pinata.cloud`, `metadata.pumper.ink`, `md.sdfgsdfsdf.uk` | 1% each |

**`ipfs.io` — the host on ~70% of rows — no longer serves content.** It returns
`HTTP 429` with a notice that it is "switching to a service worker gateway
only". `dweb.link` (same operator) likewise. A collector that GETs the recorded
URI fails on ~70% of tokens.

The collector therefore extracts the IPFS CID (`/ipfs/<cid>`, `ipfs://<cid>`,
`ipfs://ipfs/<cid>`) and walks a gateway pool. Note a bare `ipfs://<cid>` URI
has **no host at all** — its only routes are the gateways, and it must never
be handed to an HTTP client directly. Gateway availability as measured:

| Gateway | CIDv0 | CIDv1 | Notes |
|---|---|---|---|
| `pump.mypinata.cloud` | 200 | 200 | pump.fun's own Pinata gateway — primary |
| `ipfs.filebase.io` | 200 | 200 | fastest (0.77 s) |
| `ipfs.raribleuserdata.com` | 200 | 200 | |
| `gateway.pinata.cloud` | 200 | 200 | works, slow (7.0 s) |
| `ipfs.io`, `dweb.link` | 429 | 429 | dead |
| `w3s.link`, `nftstorage.link`, `4everland.io` | 301/302 | | redirect-only |
| `cf-ipfs.com`, `flk-ipfs.xyz` | conn. fail | | dead |

Re-routing to `pump.mypinata.cloud` alone resolved 93/100. The 7 failures were
all `403` — CIDs pinned by third-party launchpads, not present in pump.fun's
own Pinata account. **All 7 recovered on the first fallback.** Non-IPFS hosts
are fetched directly.

**End-to-end validation on the 100-token sample: 100/100 metadata resolved** —
68 via `pump.mypinata.cloud`, 7 via the `ipfs.filebase.io` fallback, 25 from
non-IPFS hosts.

### Metadata JSON field coverage

Small documents (p50 294 B, max 639 B). Coverage over 93 resolved JSONs:

| Key | Coverage | | Key | Coverage |
|---|---|---|---|---|
| `name`, `symbol`, `image` | 100% | | `website` | 42% |
| `createdOn` | 99% | | `coin_community` | 32% |
| `description` | 96% | | `telegram` | 8% |
| `showName` | 75% | | `video`, `banner` | 1% |
| `twitter` | 58% | | | |

### Image characteristics

99/100 downloaded (the one failure was a `gmgn.ai` CDN 403 — non-IPFS, not
content-addressed, so unrecoverable).

- **Formats**: WEBP 34, JPEG 34, PNG 29, GIF 1 (animated, 44 frames), AVIF 1
- **Colour modes**: RGB 72, RGBA 24, P 3 (palette, some with byte transparency)
- **Dimensions**: 512×512 dominates (32%); then 400×400, 500×500, 1024×1024,
  291×207, 824×1200, … Median pixel count 262,144 (= 512²); largest 1254×1254.
  Aspect ratios are mostly but not always square.

Original file sizes (n=99):

| | min | p10 | p25 | **p50** | p75 | p90 | p95 | p99/max | **mean** |
|---|---|---|---|---|---|---|---|---|---|
| bytes | 562 | 8.3 K | 13.6 K | **54.8 K** | 203.5 K | 527.0 K | 814.9 K | 2.08 M | **201.6 K** |

The mean is 3.7× the median — a few multi-MB PNGs and one 2.08 MB animated GIF
dominate. The mean is what drives disk and bandwidth estimates.

### Why images are downloaded whole and resized locally

`pump.mypinata.cloud` is Cloudflare-fronted and honours Cloudflare Images
transform parameters, which cut mean transfer 201.6 KB → 22.1 KB (12.4×):

```
?img-width=384&img-height=384&img-fit=scale-down&img-format=webp&img-anim=false
```

All four parameters are load-bearing. Without `img-anim=false` the animated GIF
returned a 1.575 MB *animated* WebP (with it: 30 KB) and that single image
doubled the sample mean from 22.1 KB to 46.4 KB. `img-width` alone does not
bound the other axis — tall images came back 384×477, 384×427; `img-height` +
`img-fit=scale-down` fixes that. PNG-only resize gave 60.8 KB where WebP gave
29.2 KB.

**But the transform destroys the original dimensions.** The server resizes
before we see any bytes, and the response headers leak no source size —
verified on a 1254×1254 / 488,825 B PNG, where the only size-ish header is
`cf-resized: … l=29184`, the *output* length. Preserving original dimensions
therefore costs a second request. Head-to-head, same 71 IPFS images,
concurrency 32, including local re-encode:

| | throughput | success | dims | stored mean | download |
|---|---|---|---|---|---|
| transform + 4 KB Range probe (2 req) | 19.6 tok/s | 70/71 | 71/71 | 22,093 B | 1.84 MB |
| **original + local resize (1 req)** | **26.7 tok/s** | **71/71** | **71/71** | **17,553 B** | 19.2 MB |

Downloading the original wins on every axis except transfer volume: one round
trip beats two, local WebP q85 beats Cloudflare's higher default quality, every
CID resolves through the fallback pool, AVIF needs no special case (Cloudflare
returns `415` for it), and the original dimensions come free from bytes already
in memory. The cost is ~2.5 TB of transfer instead of ~0.24 TB at full scale.
`--image-strategy transform` selects the low-bandwidth path for metered links.

### Size guard, and the header-only dimension parser

Some originals are pathological (p99 2.08 MB). The guard uses **streaming**
rather than a HEAD probe, so it costs no extra round trip: headers arrive
first, and if `content-length` exceeds `IMG_MAX_ORIG_BYTES` (4 MB default) the
collector reads only the first 4 KB for dimensions, aborts the stream, and
fetches a bounded rendition instead. Under 1% of images take that branch.

That branch needs dimensions from a truncated prefix, and **Pillow cannot parse
a truncated WebP** — libwebp wants the whole file, so `Image.open()` on a 4 KB
prefix failed on 34/34 WebP images. Since WebP is ~1/3 of pump.fun token
images, `collectors/image_headers.py` parses container headers directly.
Validated against full-download ground truth:

| prefix | parsed | **dimensions correct** | format correct |
|---|---|---|---|
| 512 B | 82/99 | 82 | 82 |
| 1 KB | 96/99 | 96 | 96 |
| **4 KB** | **99/99** | **99** | **99** |

It returns `None` rather than guessing — at smaller prefixes it misses JPEGs
(whose SOF marker can sit behind long EXIF segments) but it never returned a
*wrong* answer at any prefix. Where the whole file is in hand, the original
size comes from Pillow instead, which is authoritative.

### Re-encoding

Max 384 px on the long side, WebP q85, aspect ratio preserved, animation
flattened to frame 0. 384 px keeps SigLIP-384, Qwen2-VL and InternVL usable at
native tile size; 224 px would halve disk but lock out the 336/384 encoders,
and this is a dataset you only get to build once. Palette images carrying
transparency are converted via RGBA first (3% of the sample are mode `P`).

Mean stored bytes by variant, over the sample:

| variant | mean | p50 | p90 |
|---|---|---|---|
| originals | 201.6 K | 54.8 K | 527.0 K |
| WebP q80, 512 px | 18.8 K | 13.0 K | 43.2 K |
| **WebP q85, 384 px** | **16.1 K** | 11.4 K | 34.1 K |
| WebP q85, 336 px | 13.4 K | 10.6 K | 27.4 K |
| WebP q85, 224 px | 7.6 K | 6.3 K | 14.9 K |
| JPEG q85, 384 px | 21.6 K | 18.8 K | 42.0 K |

### Deduplication

Images are content-addressed by SHA-256 of the stored bytes. Two levels:

1. **CID-level (pre-download)** — a CID already stored is never downloaded
   again, whichever mint references it. Saves bandwidth. The deduped row
   inherits the image-level facts (dimensions, format, frame count) from the
   first mint that fetched it, so it is a complete row, not a row of NULLs.
2. **Content-level (post-download)** — identical bytes from different URLs
   collapse to one blob.

Measured on the 100-token sample: **83 distinct blobs from 100 tokens = 17.0%
reuse** (recycled scam art, template launches). Reuse should be higher at full
scale than in a 100-token window.

### pump.fun frontend API — withdrawn 2026-09-29

**Measured 2026-09-22, while it still worked**: `GET
frontend-api-v3.pump.fun/coins/<mint>` served `nsfw`, `is_banned`,
`reply_count`, `market_cap`, `ath_market_cap` and `total_supply` at 99 req/s
with concurrency 32, zero 429s and no `Retry-After`. There was no batch
endpoint — `/coins/multi`, `/coins/list` and `/coins/mints` all 404'd, and
`GET /coins?mints=…` returned 200 while silently ignoring the parameter
(asking for one mint returned 50 unrelated coins) — so it was one request per
mint, roughly 1.1 days for a full token set.

**Re-checked 2026-09-29: the route is gone.** It now answers `Cannot GET
/coins/<mint>` for every mint, including one pulled from pump.fun's own
listing endpoint a second earlier, so this is an upstream removal rather than
a data problem. `/api/coins/<mint>` returns `401`, `searchTerm=<mint>` ignores
the mint and returns the default listing, and `frontend-api.pump.fun` and
`advanced-api-v2.pump.fun` both return `530`.

The listing endpoint still serves the fields, but pagination caps out between
offset 1000 and 3000 — a few thousand coins, not millions — so it is not a
substitute. The `api` stage therefore preflights one mint and refuses to run,
writing nothing, rather than recording a whole day as `not_found` (which is
terminal and would need `--reset` to undo).

**For the write-up**: the failure of a third-party API to remain available for
even a week is itself the argument for the rest of the pipeline being built on
content-addressed and archival sources. `nsfw`/`is_banned` are not recoverable
from chain data, so unless a replacement surface appears, any moderation-flag
analysis has to be dropped or sourced elsewhere.

### Throughput and sizing

Against `pump.mypinata.cloud`, warm cache, no 429s at any level:

| concurrency | 8 | 16 | 32 | 64 | 128 |
|---|---|---|---|---|---|
| req/s | 15.2 | 24.8 | 33.1 | 37.2 | 57.9 |

**Cold transforms are much slower** — a first pass over 71 uncached transform
URLs ran at 10 req/s vs 33.5 req/s warm. Plan for cold rates; every token is a
first visit.

Token creation rate, measured by API pagination on `created_timestamp DESC`:
1000 coins spanned 0.70 h → **≈34,300 tokens/day** (2026-09-22). Over a
273-day window that is **N ≈ 9.3M**, but launch rates were not constant — get
the real number from your own data before allocating disk:

```bash
python -c "import duckdb; print(duckdb.sql(\"SELECT count(*) FROM 'data/parquet/tokens/tokens_????-??-??.parquet'\"))"
```

Extrapolated to N = 9.3M:

| component | per item | total |
|---|---|---|
| image blob shards (384 px WebP q85) | 17.6 KB | 163 GB |
| …after ~17% content dedup | | **~139 GB** |
| `token_metadata` parquet | ~200 B | ~1.9 GB |
| `token_images` ledger | ~150 B | ~1.4 GB |
| `token_pumpfun_api` | ~150 B | ~1.4 GB |
| **total** | | **~145 GB** |

Transient download ~2.5 TB (not stored). Wall clock ~7.5 days sequential on a
single gateway, ~3 days round-robining the pool. Stage rates: metadata
~45 req/s, images ~27 tok/s, API ~99 req/s.

---

## Known limitations

For a methods / threats-to-validity section:

1. **`is_bot` is an invocation-depth proxy**, not a bot census — see above.
2. **`nsfw` / `is_banned` are currently uncollectable** — pump.fun withdrew
   `GET /coins/<mint>` on or before 2026-09-29 (it worked on 2026-09-22). Even
   when it worked they were mutable labels read at collection time rather than
   at token creation, `token_pumpfun_api.fetched_at` records when, and `nsfw`
   showed a near-zero positive rate in sampling (0/100, and 0/70 in a
   market-cap-sorted listing even with `includeNsfw=true`), so it was doubtful
   as a label regardless. `is_banned` did produce positives (1/70).
3. **Off-chain metadata text is sanitised, not verbatim.** Token JSON is
   attacker-controlled: anyone can launch a token whose metadata holds a value
   of the wrong type where a string belongs, or an unpaired UTF-16 surrogate
   (half an emoji — `"\ud83d"`), which Python parses happily but cannot encode
   to UTF-8. Non-string values are stringified exactly (integers are not routed
   through float64, which would round a 19-digit id) and containers are stored
   as JSON text; unpaired surrogates are replaced with U+FFFD, while genuine
   emoji survive intact. Both are logged per mint with the fields involved, so
   affected rows are identifiable after the fact.
4. **Off-chain metadata is not immutable in practice.** IPFS content is
   content-addressed and therefore stable, but ~30% of tokens point at ordinary
   HTTP hosts (`j7tracker`, `uxento`, `pumper.ink`, …) that can change or
   disappear. Some already 404. `token_metadata.gateway` and `resolved_at`
   record what was seen and when.
5. **Images are re-encoded, not archived.** Stored bytes are 384 px WebP q85,
   not the original file. `orig_width`/`orig_height`/`orig_format`/`orig_bytes`
   preserve the source characteristics, but the source bytes are gone.
   Animation is flattened to frame 0 (`is_animated` / `n_frames` retained).
6. **Event struct layouts changed during the window.** All observed growth is
   fields appended at the end, so a fixed-prefix reader is correct across the
   window — an empirical finding, not a protocol guarantee. See below.
7. **Phase A day quality varies at the source.** SolArchive's backfill is
   incomplete for some days; `probe_day_log_quality()` samples each day and
   refuses days where too few rows carry real `log_messages`, rather than
   silently collecting a partial day.
8. **Phase B is unverified against real data** — the Rust plugin has not been
   compiled or run here. Treat its first run as bring-up work.

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
