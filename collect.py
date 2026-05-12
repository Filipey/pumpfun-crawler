#!/usr/bin/env python3
"""
collect.py
----------
Main entry point for the pump.fun local crawler.

Usage:
  python collect.py            # full run: discover + process all tokens
  python collect.py --status   # show checkpoint progress without running
  python collect.py --resume   # same as default (checkpoint skips done tokens)
  python collect.py --flush    # force-flush in-memory buffers to Parquet

Architecture:
  1. Discovery  — paginate /coins DESC by created_timestamp, yield tokens in window
  2. Queue      — feed mints into a thread-safe queue
  3. Workers    — WORKERS parallel threads each with their own HTTP session
  4. Storage    — thread-safe Parquet writer (flushes every FLUSH_EVERY rows)
  5. Checkpoint — JSONL file; completed mints are skipped on resume

Token categories:
  graduated  → token["complete"] == True          → full trade history
  active     → not graduated, trade_count > 10    → full trade history
  dead       → not graduated, trade_count <= 10   → metadata row only

Output files (data/parquet/):
  tokens.parquet
  trades_graduated.parquet
  trades_active.parquet
"""

import argparse
import logging
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from config import WORKERS, COLLECT_START, COLLECT_END, ACTIVITY_THRESHOLD
from collectors.api_client import PumpFunClient
from collectors.checkpoint import Checkpoint
from collectors.discovery import discover_tokens
from collectors.storage import ParquetStore
from collectors.token_processor import process_token

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("collect")


# ── Status command ────────────────────────────────────────────────────────────

def cmd_status() -> None:
    from pathlib import Path
    import pandas as pd
    from config import PARQUET_DIR, CHECKPOINT_PATH

    cp = Checkpoint()
    print(f"\n{'─'*50}")
    print(f"  Checkpoint: {cp.summary()}")
    print(f"  Checkpoint file: {CHECKPOINT_PATH}")

    for fname in ["tokens.parquet", "trades_graduated.parquet",
                  "trades_active.parquet", "trades_dead.parquet"]:
        path = PARQUET_DIR / fname
        if path.exists():
            df = pd.read_parquet(path)
            print(f"  {fname}: {len(df):,} rows")
        else:
            print(f"  {fname}: not yet created")
    print(f"{'─'*50}\n")


# ── Worker thread ─────────────────────────────────────────────────────────────

def worker(
    work_queue: "queue.Queue[Optional[dict]]",
    checkpoint: Checkpoint,
    store: ParquetStore,
    worker_id: int,
    counters: dict,
    counters_lock: threading.Lock,
) -> None:
    client = PumpFunClient()
    try:
        while True:
            item = work_queue.get()
            if item is None:  # sentinel
                break
            try:
                process_token(item, client, checkpoint, store)
                with counters_lock:
                    counters["ok"] += 1
            except Exception:
                with counters_lock:
                    counters["err"] += 1
            finally:
                work_queue.task_done()
    finally:
        client.close()


# ── Main run ──────────────────────────────────────────────────────────────────

def cmd_run() -> None:
    logger.info(
        f"Starting collection: {COLLECT_START} → {COLLECT_END}, "
        f"workers={WORKERS}, activity_threshold={ACTIVITY_THRESHOLD}"
    )

    checkpoint = Checkpoint()
    store      = ParquetStore()
    work_queue: queue.Queue = queue.Queue(maxsize=WORKERS * 4)

    counters      = {"ok": 0, "err": 0, "skip": 0, "discovered": 0}
    counters_lock = threading.Lock()

    # Start worker threads
    threads = []
    for i in range(WORKERS):
        t = threading.Thread(
            target=worker,
            args=(work_queue, checkpoint, store, i, counters, counters_lock),
            daemon=True,
            name=f"worker-{i}",
        )
        t.start()
        threads.append(t)

    # Discovery runs in the main thread; feeds the queue
    discovery_client = PumpFunClient()
    t_start = time.monotonic()
    try:
        for token in discover_tokens(discovery_client):
            mint = token.get("mint", "")
            with counters_lock:
                counters["discovered"] += 1
            if checkpoint.is_done(mint):
                with counters_lock:
                    counters["skip"] += 1
                continue
            work_queue.put(token)  # blocks if queue is full (backpressure)

            # Periodic progress log
            n = counters["discovered"]
            if n % 1000 == 0:
                elapsed = time.monotonic() - t_start
                rate = n / elapsed if elapsed else 0
                logger.info(
                    f"Progress: discovered={n}, ok={counters['ok']}, "
                    f"err={counters['err']}, skip={counters['skip']}, "
                    f"rate={rate:.1f} tokens/s"
                )
    finally:
        discovery_client.close()

    # Send one sentinel per worker to signal shutdown
    for _ in threads:
        work_queue.put(None)

    # Wait for all workers to drain
    for t in threads:
        t.join()

    # Final flush
    store.flush()

    elapsed = time.monotonic() - t_start
    logger.info(
        f"Collection complete in {elapsed:.0f}s — "
        f"discovered={counters['discovered']}, ok={counters['ok']}, "
        f"err={counters['err']}, skip={counters['skip']}"
    )
    logger.info(f"Checkpoint: {checkpoint.summary()}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="pump.fun local crawler — collects token metadata and trades"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show collection progress and exit",
    )
    parser.add_argument(
        "--flush",
        action="store_true",
        help="Force-flush all in-memory buffers to Parquet and exit",
    )
    args = parser.parse_args()

    if args.status:
        cmd_status()
        sys.exit(0)

    if args.flush:
        store = ParquetStore()
        store.flush()
        logger.info("Flush complete")
        sys.exit(0)

    cmd_run()


if __name__ == "__main__":
    main()
