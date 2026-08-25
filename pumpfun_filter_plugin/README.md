# pumpfun-filter-plugin

A [Jetstreamer](https://github.com/anza-xyz/jetstreamer) plugin that replays
historical Solana mainnet blocks from the [Old Faithful](https://old-faithful.net/)
archive and extracts every transaction touching the pump.fun bonding-curve
program (`6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`) or the PumpSwap AMM
program (`pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`). This is the engine
behind Phase B of the crawler's two-phase backfill design (Phase A pulls
already-decoded shards from the SolArchive dataset; Phase B, this plugin,
covers everything SolArchive doesn't by replaying raw chain history directly).

## Status

Compiled, run, and verified end to end in production. Between 2026-08-04 and
2026-08-25 this plugin drove the complete Phase B backfill: every epoch from
roughly 883 to 999 (Nov 2025 – Jul 2026), streamed on a 16-core / 125GB
production host. Combined with Phase A, the two together produced full,
gap-free daily coverage from **2025-10-26 to 2026-07-10** — confirmed at the
Parquet-file level, not just checkpoint bookkeeping — comprising 6,569,510
tokens, 581,991 pools, 62,122 graduations, 533,845,967 bonding-curve trades,
and 3,367,860,350 PumpSwap trades (pipeline-wide totals across both phases;
each row's `source_phase` column records which phase produced it).

## Role in the pipeline

Old Faithful stores every historical Solana block, content-addressed and
indexed by slot, as a public read-only archive. Jetstreamer is a Rust
replay engine that streams blocks out of that archive in slot order across
multiple worker threads and calls user-supplied plugin hooks per transaction
and per block. This crate is that plugin: it does nothing but filter and
serialize — no decoding, no persistence beyond flat files, no ClickHouse (see
below). All Borsh/Anchor decoding of the matched transactions happens
downstream, in Python (`collectors/decoder.py`), the same decoder used for
Phase A. This split exists because the decoder was already written and
tested against Phase A's SolArchive data; re-deriving it in Rust would have
meant maintaining two independent implementations of the same Anchor
instruction/event layouts.

## How it works

### Plugin hooks

Jetstreamer's `Plugin` trait exposes two async hooks, both called once per
worker thread per item as blocks stream past:

- `on_transaction(thread_id, db, tx: &TransactionData)` — called for every
  non-vote transaction in a block, *before* the block itself is finalized.
- `on_block(thread_id, db, block: &BlockData)` — called once per block, after
  all its transactions have been passed through `on_transaction`.

The `db: Option<Arc<clickhouse::Client>>` parameter exists because
Jetstreamer's reference plugin (`program_tracking.rs`) persists to
ClickHouse; this plugin ignores it entirely (see **ClickHouse is
disabled**, below).

### Filtering (`on_transaction`)

1. Vote transactions (`tx.is_vote`) are skipped immediately — they're by far
   the highest-volume transaction type on Solana and never touch either
   target program.
2. `account_keys()` builds the transaction's **full** effective account set:
   the static `account_keys` from the legacy or v0 `VersionedMessage`, plus
   every address resolved from Address Lookup Tables
   (`transaction_status_meta.loaded_addresses.{writable,readonly}`). ALTs let
   a v0 transaction reference accounts that never appear in its own static
   key list, so skipping this step would silently miss any pump.fun/PumpSwap
   transaction that used one.
3. A transaction matches if either program ID string-equals **any** entry in
   that combined key set.

This is deliberately over-inclusive: a program ID appearing in a
transaction's account list does not prove the program was actually invoked
(an unrelated instruction can simply list it as a readonly account). The
plugin does not try to resolve this — it has no Borsh/Anchor decoding logic
at all. Every matched transaction's raw `log_messages` are carried through
unchanged, and the real filter is applied downstream: `collectors/decoder.py`
only emits a decoded event if the actual Anchor instruction discriminator
appears in those logs. False positives here are computationally free to
produce and get silently dropped later; false negatives (under-matching)
would be invisible and permanent. This mirrors the identical
"accounts-array-contains-program-ID" filter Phase A applies against
SolArchive shards, so both phases share the same inclusion criterion.

### Reconstructing `block_time`

`TransactionData` (the type `on_transaction` receives) carries no
`block_time` field — that field only exists on `BlockData`, which the plugin
doesn't see until *after* every transaction in the block has already been
processed. Matched transactions are therefore staged in a global,
slot-keyed map:

```rust
static PENDING_BY_SLOT: Lazy<DashMap<u64, Vec<MatchedTx>>> = Lazy::new(DashMap::new);
```

`on_block` looks up its own slot in this map, stamps `block_time` onto every
staged record for that slot, and only then writes them out. `DashMap` (a
sharded concurrent hash map) is used because slots are processed
concurrently across worker threads but each slot's transactions and its
block arrive on the *same* thread — the map only needs to support concurrent
access across slots, not synchronize a single slot across threads.

### Output — one file per worker thread

```rust
static OUT_FILES: Lazy<DashMap<usize, Mutex<File>>> = Lazy::new(DashMap::new);
```

Jetstreamer partitions the requested slot range across `JETSTREAMER_THREADS`
worker threads (each replaying a contiguous sub-range independently), and
each thread's `write_records` call opens (lazily, once) and appends to its
own `part-<thread_id>.jsonl`. This follows Jetstreamer's own plugin-writing
guidance of aggregating on a thread-local basis rather than funneling every
write through one shared file/lock, which would serialize writes across
threads that are otherwise fully independent.

### ClickHouse is disabled

Jetstreamer's default runtime mode auto-manages a bundled ClickHouse
process for plugins (like the reference `program_tracking` plugin) that
persist through the `db` client. This plugin never touches `db`, but
Jetstreamer's auto-spawn still tried to run regardless and failed in
practice ("Failed to find the PID of the running ClickHouse server"). It's
disabled at invocation time via `JETSTREAMER_CLICKHOUSE_MODE=off` (set by
`collect_oldfaithful.py`, not by this crate) — the plugin's own code has no
ClickHouse-specific branch to maintain.

## Output format

One JSON object per line, one file per thread, appended for the duration of
a single run:

```json
{"slot": 431000000, "tx_index": 12, "signature": "5k3n...", "block_time": 1783200000, "err": false, "log_messages": ["Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [1]", "..."]}
```

| field | type | meaning |
|---|---|---|
| `slot` | u64 | slot the transaction landed in |
| `tx_index` | usize | index of this transaction within its block |
| `signature` | string | base58 transaction signature |
| `block_time` | i64 or null | unix seconds, from the containing block; `null` only if Old Faithful itself has no timestamp for that block (rare) |
| `err` | bool | whether the transaction failed on-chain |
| `log_messages` | string[] | full raw program log output — this is what `decoder.py` pattern-matches against downstream |

If a run is restarted pointed at the same `out_dir`, files are opened in
append mode — a restarted/re-run epoch will duplicate lines from the prior
attempt. This is intentional rather than a gap to fix: every downstream
table dedups on `(signature, tx_type)` (or `mint`/`pool`/`signature` alone
for the identity tables) at compaction time, so re-ingesting the same
transaction twice is a no-op, and `collect_oldfaithful.py` deletes each
epoch's `out_dir` after successful ingestion in the normal path anyway.

## Downstream consumption

`collectors/oldfaithful_source.py::process_output_dir()` reads every
`part-*.jsonl` file in an epoch's output directory, reshapes each record
into the same RPC-`getTransaction`-shaped dict `decoder.py` already
understands (`{"slot", "blockTime", "meta": {"err", "logMessages"}}`), runs
it through `decode_transaction()` + `decode_pumpswap_transaction()`, and
buckets each resulting event into a calendar day using **the record's own
`block_time`**, interpreted as UTC
(`datetime.fromtimestamp(ts, timezone.utc)`) — not the epoch's nominal date
range, since a single epoch's ~432,000 slots span parts of two or three
calendar days. Events are flushed to disk in batches of 50,000 (summed
across all dates touched since the last flush) rather than per file, because
a single epoch's matched-transaction volume (observed: 7–25 million events
per epoch in this dataset's Nov 2025 – Jul 2026 window) is far larger than
anything Phase A ever handled from one SolArchive shard.

## Dependencies and version pinning

```toml
jetstreamer = "0.6"
clickhouse = { version = "=0.14.3", default-features = false }
solana-message = "3"
solana-transaction-status = "3"
solana-signature = { version = "3", default-features = false }
```

The `clickhouse` pin is exact (`=0.14.3`), not a semver range. Jetstreamer's
`Plugin` trait signature includes `Option<Arc<clickhouse::Client>>`
directly, so if this crate's `Cargo.lock` resolved a different `clickhouse`
version than the one `jetstreamer-plugin` itself was built against, Cargo
would treat them as two distinct types and the trait implementation would
fail to type-check across the crate boundary — even though the crate name
and most of the API are identical. `jetstreamer-plugin` resolves
`clickhouse 0.14.3` internally; this had to be discovered by inspecting
`Cargo.lock`, since jetstreamer's own `Cargo.toml` declaration is a loose
semver range that resolves differently depending on when it's built. If a
future `cargo update` reintroduces this class of error, the fix is the same:
`grep -A2 'name = "clickhouse"' Cargo.lock` for duplicate resolved versions
and re-pin to match jetstreamer-plugin's.

`jetstreamer = "0.6"` targets the version actually published to crates.io.
Jetstreamer's own README documents a `JetstreamerInvocation` enum
(`Run`/`ListPlugins` variants) that does not exist in 0.6.0 — that's a
newer, git-HEAD-only API. The 0.6.0 entry point actually used here is
`JetstreamerRunner::default().with_plugin(...).parse_cli_args()?.run()`,
confirmed by reading the published crate's source directly rather than its
README.

## Build

Jetstreamer requires Clang 16 (not 17) and RocksDB's build dependencies —
see [anza-xyz/jetstreamer](https://github.com/anza-xyz/jetstreamer) for full
OS-specific setup.

```bash
cd pumpfun_filter_plugin
cargo build --release
```

## Running

Not meant to be invoked directly for real backfills —
`collect_oldfaithful.py` in the repo root drives it per-epoch, sets the
required environment variables, streams its stdout/stderr live into the
Python logger, checkpoints per epoch, and ingests its output afterward. For
manual/debugging invocation:

```bash
# Whole epoch, by epoch number
PUMPFUN_FILTER_OUT_DIR=./out JETSTREAMER_CLICKHOUSE_MODE=off \
  ./target/release/pumpfun-filter-plugin 990

# Explicit slot range (start:end)
PUMPFUN_FILTER_OUT_DIR=./out JETSTREAMER_CLICKHOUSE_MODE=off \
  ./target/release/pumpfun-filter-plugin 427680000:428112000
```

`JETSTREAMER_THREADS` controls worker-thread count (see **Throughput**,
below); if unset, Jetstreamer auto-sizes based on host hardware.

## Epoch and slot-range selection

Solana epochs are a fixed 432,000 slots
(`collectors/epoch_utils.py::SLOTS_PER_EPOCH`) for the entire range this
plugin was run against. `collect_oldfaithful.py` accepts either a calendar
date range or an explicit slot range (`--slots START:END`, added for fast
bounded smoke-testing without committing to a multi-hour full-epoch run);
for a date range, `epoch_utils.estimate_epoch_range()` converts
`[start_date, end_date]` into `[start_epoch, end_epoch]` by:

1. Empirically calibrating average slot duration via two `getBlockTime`
   RPC calls ~50,000,000 slots apart, rather than trusting Solana's nominal
   400ms/slot target — a first pass using the nominal rate alone was
   measured off by ~2 days over a 7-month span.
2. Converting each boundary date to an estimated slot using that calibrated
   rate, then padding the resulting epoch range by **3 epochs on each side**
   (`PAD_EPOCHS = 3`) before requesting it.

The padding is intentional, not a precision bug: a few epochs of harmless
overlap costs nothing (checkpointing is per-epoch, and every downstream
table dedups on `(signature, tx_type)` at compaction time), while
under-covering the requested range would silently drop data at the
boundary with no signal that it happened. In production this meant a
request for calendar dates 2025-11-01–2025-11-18 actually replayed epochs
covering 2025-10-26 through 2025-12-05, and a request for
2025-12-06–2026-07-04 extended through 2026-07-10 — both confirmed
after the fact by checking which calendar days actually had data on disk,
not by trusting the requested range as ground truth.

## Operational characteristics observed in production

**Transient mid-stream errors are expected and self-healing.** Jetstreamer's
firehose logs errors like:

```
ERROR jetstreamer::firehose::T005 🧯💦🔥 firehose encountered an error at slot 381591001
  in epoch 883 and will roll back one slot and retry: Timeout while waiting for
  operation: seek_to_previous_indexed_slot
```

and, less frequently, `Error reading until block: error decoding response
body`. Both are network-layer hiccups against the Old Faithful CDN; every
occurrence observed during the full production run was followed by a
successful retry and the epoch completing normally (`=== epoch NNN DONE
=== `). These should not be mistaken for plugin bugs — the plugin itself
never sees or handles them; they're entirely internal to Jetstreamer's
firehose replay layer.

**Throughput is I/O-bound, not CPU-bound, and plateaus well below one
thread per core.** Controlled tests on a 16-core host replaying the same
10,000-slot range: 4 threads (Jetstreamer's default) took ~4m55s wall time
with only ~4m04s of CPU time consumed (confirming the bottleneck is waiting
on the network, not computing); 16 threads cut wall time to roughly 3
minutes; 32 threads produced no further improvement (3m09s) despite double
the thread count, and per-thread CPU time actually increased relative to
wall time — evidence of contention/diminishing returns past 16, likely
against Old Faithful's CDN throughput per client rather than local
resources. `JETSTREAMER_THREADS=16` was used for the full production
backfill on this basis. At that setting, a full epoch (432,000 slots) took
roughly 2–3.5 hours depending on transaction volume in that period (later
epochs, further into 2026, consistently took longer — matching the platform's
continued growth: a 10,000-slot slice from June 2026 produced ~880,000–900,000
matched events, versus the ~7–9 million *per full epoch* (not per 10,000
slots) typical of November 2025 — see the epoch-completion counts in
`data/logs/oldfaithful.log` for exact per-epoch, per-date event totals).

## Known limitations

- **No decoding, no validation.** This plugin has no concept of whether a
  matched transaction is a `create`, `buy`, `sell`, `complete`, or
  `pool_create` event, nor whether the Anchor instruction it references
  actually succeeded in the way its logs suggest — all of that lives in
  `collectors/decoder.py`. A bug in the account-key matching logic here
  would silently under-collect data with no local signal; the only check
  against that is the project's `data_quality.py` script, run against the
  final assembled dataset.
- **No dedup, no idempotency guarantee at the file level.** Re-running the
  same slot range into the same `out_dir` produces duplicate lines by
  design (append-mode). Correctness depends entirely on the downstream
  `(signature, tx_type)` dedup key at Parquet-compaction time — this plugin
  makes no attempt to detect or suppress duplicates itself.
- **Coverage is at the mercy of Old Faithful's own completeness.** If
  Old Faithful is missing or has corrupted data for some slot range
  (distinct from the transient retry-recoverable errors described above),
  this plugin has no way to detect or report that gap — it only sees what
  Jetstreamer's firehose successfully replays.
