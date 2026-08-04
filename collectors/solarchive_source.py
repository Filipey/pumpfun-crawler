"""
collectors/solarchive_source.py
--------------------------------
Phase A ingestion adapter: pulls pump.fun + PumpSwap transactions out of the
`solarchive/solarchive` Hugging Face dataset (public, CC BY 4.0, daily
Parquet-partitioned full Solana transaction history) without downloading full
day files — DuckDB's httpfs reader does column-pruned, filtered remote reads
per shard.

Each day is split across ~300 shard files (`txs/YYYY-MM-DD/NNNNNNNNNNNN.parquet`).
For each shard we run one filtered query (accounts array contains either
program ID), reshape matching rows into the RPC-`getTransaction`-shaped dict
collectors/decoder.py already knows how to consume, and feed the decoded
events into an EntityStore.

SolArchive currently only covers up to 2025-12-05 — this module has no
knowledge of that cutoff; the caller (collect_solarchive.py) is responsible
for the date range.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import duckdb
from huggingface_hub import HfApi

from collectors.decoder import decode_pumpswap_transaction, decode_transaction
from collectors.day_store import EntityStore
from config import SOLARCHIVE_REPO

logger = logging.getLogger(__name__)

PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

_HF_RESOLVE_BASE = f"https://huggingface.co/datasets/{SOLARCHIVE_REPO}/resolve/main"

_FILTERED_QUERY = """
    SELECT signature, block_slot, block_timestamp, status, err, log_messages
    FROM read_parquet(?)
    WHERE list_contains(list_transform(accounts, x -> x.pubkey), ?)
       OR list_contains(list_transform(accounts, x -> x.pubkey), ?)
"""

MAX_RETRIES = 4
RETRY_BACKOFF = 2.0

# Recycle each thread's DuckDB connection after this many shard queries.
# Observed RSS climbing steadily (not clearly plateauing) across a long-lived
# connection querying many distinct remote Parquet files in one process —
# root cause not confirmed (DuckDB/httpfs internal caching vs. allocator
# high-water-marking), but periodically closing and reopening the connection
# is cheap (httpfs reload is milliseconds) and removes the risk regardless of
# which it is.
_RECYCLE_EVERY = 20

_local = threading.local()


def _connection() -> duckdb.DuckDBPyConnection:
    con = getattr(_local, "con", None)
    count = getattr(_local, "count", 0)

    if con is not None and count >= _RECYCLE_EVERY:
        con.close()
        con = None

    if con is None:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        _local.con = con
        _local.count = 0

    _local.count = getattr(_local, "count", 0) + 1
    return con


def list_day_shards(date: str) -> list[str]:
    """
    Return HF resolve URLs for every shard parquet file for `date`.

    Uses list_repo_tree(path_in_repo=...) to list just this day's ~300-file
    subfolder (~0.3s) rather than list_repo_files(), which enumerates the
    entire multi-hundred-thousand-file repo on every call (~10s+, and only
    getting slower as the dataset grows).
    """
    api = HfApi()
    entries = api.list_repo_tree(SOLARCHIVE_REPO, path_in_repo=f"txs/{date}", repo_type="dataset")
    shards = sorted(e.path for e in entries if e.path.endswith(".parquet"))
    return [f"{_HF_RESOLVE_BASE}/{f}" for f in shards]


def _row_to_tx(row) -> tuple[dict, str]:
    err = None if row.status == "Success" else (row.err or "error")
    tx = {
        "slot": int(row.block_slot),
        "blockTime": int(row.block_timestamp.timestamp()),
        "meta": {
            "err": err,
            "logMessages": list(row.log_messages) if row.log_messages is not None else [],
        },
    }
    return tx, row.signature


QUALITY_PROBE_SAMPLE = 50
QUALITY_PROBE_MIN_RATIO = 0.5


def probe_day_log_quality(date: str) -> tuple[bool, float, int]:
    """
    Cheap pre-check (touches one shard, not a whole day's ~200-300): what
    fraction of matched rows in the first shard have real, populated
    log_messages (more than the single-empty-string placeholder some of
    SolArchive's Nov 2025 backfill has for whole days — confirmed by
    inspection: some Nov 2025 days are internally consistent at 0% real
    log_messages across every shard, even reading a fully-downloaded local
    copy of the file, ruling out a remote-read issue — this is a genuine gap
    in that portion of the archive, not a bug in this pipeline).

    Returns (is_usable, ratio, sample_size). A day with no matches at all in
    the sampled shard is treated as usable-but-inconclusive (ratio=1.0,
    sample_size=0) rather than bad, since that can happen on a slow shard.
    """
    shard_urls = list_day_shards(date)
    if not shard_urls:
        return False, 0.0, 0

    con = _connection()
    query = _FILTERED_QUERY + f"\n    LIMIT {QUALITY_PROBE_SAMPLE}"
    try:
        df = con.execute(query, [shard_urls[0], PUMPFUN_PROGRAM, PUMPSWAP_PROGRAM]).fetchdf()
    except Exception as exc:
        logger.warning(f"[solarchive] {date}: quality probe failed ({exc}), treating as unusable")
        return False, 0.0, 0

    if len(df) == 0:
        return True, 1.0, 0

    real = (df["log_messages"].apply(lambda x: len(x) if x is not None else 0) > 1).sum()
    ratio = real / len(df)
    return ratio >= QUALITY_PROBE_MIN_RATIO, ratio, len(df)


def process_shard(url: str, date: str, store: EntityStore) -> int:
    """Query one shard, decode matching rows, feed EntityStore. Returns event count."""
    con = _connection()
    last_exc: Optional[Exception] = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            df = con.execute(_FILTERED_QUERY, [url, PUMPFUN_PROGRAM, PUMPSWAP_PROGRAM]).fetchdf()
            break
        except Exception as exc:
            last_exc = exc
            wait = RETRY_BACKOFF ** attempt
            logger.warning(f"Shard read failed ({type(exc).__name__}): {exc} — retry {attempt + 1} in {wait:.1f}s")
            time.sleep(wait)
    else:
        logger.error(f"Giving up on shard after {MAX_RETRIES + 1} attempts: {url} ({last_exc})")
        raise RuntimeError(f"shard fetch failed: {url}") from last_exc

    n_events = 0
    for row in df.itertuples(index=False):
        tx, sig = _row_to_tx(row)
        events = decode_transaction(tx, sig) + decode_pumpswap_transaction(tx, sig)
        for ev in events:
            ev["date"] = date
            ev["source_phase"] = "solarchive"
            store.add(date, ev)
        n_events += len(events)

    return n_events


DEFAULT_WORKERS = 4
# Flush the EntityStore's buffer to disk after this many completed shards.
# A single shard mixes ALL Solana traffic, not just ours, and one shard alone
# was observed to yield ~27k matched events (bonding curve + PumpSwap
# combined); flush_every=10 (~270k buffered rows) was enough to spike a 14GB
# machine into swap/OOM territory during testing. Flush after every shard —
# each flush_batch() call is a small append-only Parquet write (no read-
# merge-rewrite), so this costs a bit more I/O, not O(day) memory.
FLUSH_EVERY_SHARDS = 1


def process_day(
    date: str,
    store: EntityStore,
    workers: int = DEFAULT_WORKERS,
    flush_every: int = FLUSH_EVERY_SHARDS,
) -> int:
    """
    Process every shard for `date`, flushing to disk in bounded batches.
    Returns total events decoded. Does NOT compact — call
    EntityStore.compact_day(date) once the day (and any other phase writing
    to the same date) is fully done.
    """
    shard_urls = list_day_shards(date)
    if not shard_urls:
        logger.warning(f"No SolArchive shards found for {date}")
        return 0

    total_events = 0
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(process_shard, url, date, store): url for url in shard_urls}
        for fut in as_completed(futures):
            url = futures[fut]
            try:
                total_events += fut.result()
            except Exception:
                logger.error(f"Shard permanently failed, skipping: {url}", exc_info=True)
            done += 1

            if done % flush_every == 0:
                written = store.flush_batch(date)
                logger.debug(f"[solarchive] {date}: periodic flush -> {written}")

            if done % 25 == 0 or done == len(shard_urls):
                logger.info(f"[solarchive] {date}: {done}/{len(shard_urls)} shards, {total_events:,} events so far")

    store.flush_batch(date)  # flush any remainder
    return total_events
