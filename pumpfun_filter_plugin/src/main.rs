//! Jetstreamer plugin: replays Old Faithful history and dumps every
//! transaction touching the pump.fun bonding-curve or PumpSwap AMM program
//! to per-thread JSON-Lines files, for collectors/oldfaithful_source.py to
//! pick up and feed through the existing Python decoder/EntityStore.
//!
//! *** UNCOMPILED / UNVERIFIED ***
//! Written by close reference to jetstreamer's actual source (Plugin trait
//! definition in jetstreamer-plugin/src/lib.rs, the real
//! jetstreamer-plugin/src/plugins/program_tracking.rs reference plugin, and
//! TransactionData/BlockData struct definitions in
//! jetstreamer-firehose/src/firehose.rs — all inspected directly, not
//! guessed from the README alone) but there is no Rust toolchain available
//! in the environment this was written in (jetstreamer itself requires
//! Clang 16 + RocksDB to build — see its README). This has NOT been
//! compiled. Build it on the lab server first:
//!
//!     cd pumpfun_filter_plugin
//!     cargo build --release
//!
//! If it fails to compile, the most likely cause is a solana-* crate
//! version drift between this Cargo.toml and jetstreamer's current
//! Cargo.toml (pin to whatever jetstreamer itself now depends on) — this
//! plugin's dependency versions were copied from jetstreamer's own
//! Cargo.toml at the time of writing.
//!
//! Output format: one JSON object per line, per (thread, run) output file
//! (`<out_dir>/part-<thread_id>.jsonl`, appended across the whole run so a
//! restarted run should point at a fresh out_dir or accept duplicate lines —
//! collectors/oldfaithful_source.py dedups downstream via the same
//! signature+tx_type key as every other ingestion path):
//!
//!     {"slot": 431000000, "tx_index": 12, "signature": "...",
//!      "block_time": 1783200000, "err": false, "log_messages": ["..."]}

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use clickhouse::Client;
use dashmap::DashMap;
use futures_util::FutureExt;
use jetstreamer::{
    firehose::{BlockData, TransactionData},
    plugin::{Plugin, PluginFuture},
    JetstreamerRunner,
};
use once_cell::sync::Lazy;
use serde::Serialize;
use solana_message::VersionedMessage;
use std::sync::Mutex;

const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPSWAP_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

#[derive(Serialize)]
struct MatchedTx {
    slot: u64,
    tx_index: usize,
    signature: String,
    block_time: Option<i64>,
    err: bool,
    log_messages: Vec<String>,
}

// Transactions matched in on_transaction are buffered here (keyed by slot,
// globally unique regardless of which worker thread processes them) until
// the corresponding on_block call supplies block_time — TransactionData has
// no block_time field of its own, only BlockData does (verified against the
// actual struct definitions, not assumed).
static PENDING_BY_SLOT: Lazy<DashMap<u64, Vec<MatchedTx>>> = Lazy::new(DashMap::new);

// One append-only output file per worker thread, opened lazily, to avoid a
// shared lock on every write. Matches jetstreamer's own documented guidance
// ("aggregate data on a thread-local, per-thread basis").
static OUT_FILES: Lazy<DashMap<usize, Mutex<File>>> = Lazy::new(DashMap::new);

pub struct PumpFilterPlugin {
    out_dir: PathBuf,
}

impl PumpFilterPlugin {
    pub fn new(out_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&out_dir).expect("failed to create output directory");
        Self { out_dir }
    }

    fn write_records(&self, thread_id: usize, records: &[MatchedTx]) -> std::io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let entry = OUT_FILES.entry(thread_id).or_insert_with(|| {
            let path = self.out_dir.join(format!("part-{thread_id}.jsonl"));
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .expect("failed to open plugin output file");
            Mutex::new(file)
        });
        let mut file = entry.lock().expect("output file mutex poisoned");
        for record in records {
            let line = serde_json::to_string(record)?;
            writeln!(file, "{line}")?;
        }
        Ok(())
    }

    /// Full set of accounts a transaction references: static message keys +
    /// addresses resolved from Address Lookup Tables. Matches the "accounts
    /// array contains program ID" filter used against SolArchive for Phase A
    /// (deliberately over-inclusive — a program can appear here without
    /// actually being invoked; the downstream Python Borsh decoder only
    /// emits an event if the real Anchor discriminator is present, so
    /// false positives here are free to produce, not silently wrong).
    fn account_keys(tx: &TransactionData) -> Vec<String> {
        let message = &tx.transaction.message;
        let mut keys: Vec<String> = match message {
            VersionedMessage::Legacy(msg) => msg.account_keys.iter().map(|k| k.to_string()).collect(),
            VersionedMessage::V0(msg) => msg.account_keys.iter().map(|k| k.to_string()).collect(),
        };
        keys.extend(
            tx.transaction_status_meta
                .loaded_addresses
                .writable
                .iter()
                .map(|k| k.to_string()),
        );
        keys.extend(
            tx.transaction_status_meta
                .loaded_addresses
                .readonly
                .iter()
                .map(|k| k.to_string()),
        );
        keys
    }
}

impl Plugin for PumpFilterPlugin {
    fn name(&self) -> &'static str {
        "pumpfun-filter"
    }

    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        tx: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            if tx.is_vote {
                return Ok(());
            }

            let keys = Self::account_keys(tx);
            let matched = keys.iter().any(|k| k == PUMPFUN_PROGRAM || k == PUMPSWAP_PROGRAM);
            if !matched {
                return Ok(());
            }

            let log_messages = tx
                .transaction_status_meta
                .log_messages
                .clone()
                .unwrap_or_default();
            let err = tx.transaction_status_meta.status.is_err();

            let record = MatchedTx {
                slot: tx.slot,
                tx_index: tx.transaction_slot_index,
                signature: tx.signature.to_string(),
                block_time: None,
                err,
                log_messages,
            };

            PENDING_BY_SLOT.entry(tx.slot).or_insert_with(Vec::new).push(record);
            Ok(())
        }
        .boxed()
    }

    fn on_block<'a>(
        &'a self,
        thread_id: usize,
        _db: Option<Arc<Client>>,
        block: &'a BlockData,
    ) -> PluginFuture<'a> {
        async move {
            let slot = block.slot();
            if let Some((_, mut records)) = PENDING_BY_SLOT.remove(&slot) {
                let block_time = block.block_time();
                for record in records.iter_mut() {
                    record.block_time = block_time;
                }
                self.write_records(thread_id, &records)
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
            }
            Ok(())
        }
        .boxed()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: jetstreamer 0.6.0 (the version actually published to crates.io,
    // confirmed by reading its real source — the README's JetstreamerInvocation
    // enum with Run/ListPlugins doesn't exist here, that's a newer git-only
    // API) has JetstreamerRunner::parse_cli_args(self) -> Result<Self, _>: it
    // just parses env/CLI into self.config and hands the same runner back.
    let out_dir = std::env::var("PUMPFUN_FILTER_OUT_DIR").unwrap_or_else(|_| "./out".to_string());

    JetstreamerRunner::default()
        .with_log_level("info")
        .with_plugin(Box::new(PumpFilterPlugin::new(PathBuf::from(out_dir))))
        .parse_cli_args()?
        .run()
        .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;

    Ok(())
}
