# pumpfun-filter-plugin

Jetstreamer plugin that replays Solana history from the [Old Faithful](https://old-faithful.net/)
archive and dumps every transaction touching the pump.fun bonding-curve
program (`6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`) or the PumpSwap AMM
program (`pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`) to JSON-Lines files,
for `collectors/oldfaithful_source.py` to pick up.

## Status: UNCOMPILED / UNVERIFIED

This was written by close reference to jetstreamer's actual source (the
`Plugin` trait definition, the real `program_tracking.rs` reference plugin,
and the `TransactionData`/`BlockData` struct definitions — all read directly
from a clone of `anza-xyz/jetstreamer`, not guessed from documentation
alone), but there was no Rust toolchain available in the environment this was
written in, so **it has never been compiled**. Build it here first, fix
whatever the compiler finds, then it's ready for real use.

## Build

Jetstreamer requires Clang 16 (not 17) and RocksDB build dependencies — see
[anza-xyz/jetstreamer's README](https://github.com/anza-xyz/jetstreamer) for
full OS-specific setup (Nix is recommended there for reliability). Once that
toolchain is in place:

```bash
cd pumpfun_filter_plugin
cargo build --release
```

If it fails to compile, the most likely cause is a `solana-*` crate version
mismatch — `Cargo.toml` pins versions copied from jetstreamer's own
`Cargo.toml` at the time this was written; check jetstreamer's current
`Cargo.toml` and match whatever it now depends on (the `TransactionData`/
`BlockData` types this plugin pattern-matches on are defined in terms of
those crates' types, so versions must agree exactly).

## Run

Don't invoke this binary directly for day-to-day use — `collect_oldfaithful.py`
in the repo root drives it per-epoch, handles checkpointing, and ingests its
output. To run it manually for debugging:

```bash
# Replay a single epoch, writing matched transactions to ./out/part-*.jsonl
PUMPFUN_FILTER_OUT_DIR=./out ./target/release/pumpfun-filter-plugin 990

# Or an explicit slot range
PUMPFUN_FILTER_OUT_DIR=./out ./target/release/pumpfun-filter-plugin 427680000:428112000
```

## Output format

One JSON object per line, per `part-<thread_id>.jsonl` file (jetstreamer
processes different slot subranges on different worker threads
concurrently — each thread gets its own output file to avoid write
contention, per jetstreamer's own documented plugin-writing guidance):

```json
{"slot": 431000000, "tx_index": 12, "signature": "...", "block_time": 1783200000, "err": false, "log_messages": ["Program ..."]}
```

`collectors/oldfaithful_source.py` reads these files, reshapes each record
into the same RPC-`getTransaction`-shaped dict `collectors/decoder.py`
already knows how to decode (used for both Phase A and Phase B), buckets
each decoded event into its correct calendar day using the record's own
`block_time`, and feeds `EntityStore`.

## Filtering behavior

Matches if the pump.fun or PumpSwap program ID appears **anywhere** in the
transaction's account list (static message keys + Address Lookup
Table-resolved addresses) — deliberately over-inclusive, mirroring the same
"accounts array contains program ID" filter Phase A uses against SolArchive.
A transaction can match here without actually invoking either program (e.g.
if it merely references the account); that's fine, because the downstream
Borsh decoder only emits an event when the real Anchor discriminator is
present in the transaction's logs. Vote transactions are skipped early.
