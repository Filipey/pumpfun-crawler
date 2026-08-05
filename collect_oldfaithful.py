"""
collect_oldfaithful.py
------------------------
Phase B CLI: backfill pump.fun + PumpSwap history from Old Faithful via the
custom `pumpfun_filter_plugin` Jetstreamer plugin (2025-12-06 onward — the
day after SolArchive's Phase A coverage ends).

REQUIRES the plugin to be compiled first on a machine with a Rust toolchain,
Clang 16, and the RocksDB build deps jetstreamer itself needs (see
pumpfun_filter_plugin/src/main.rs and https://github.com/anza-xyz/jetstreamer
for setup — not available in the environment this was written in, so the
plugin is UNCOMPILED/UNVERIFIED code):

    cd pumpfun_filter_plugin
    cargo build --release

Usage
-----
    python collect_oldfaithful.py --epoch 990   # single epoch (smoke test)
    python collect_oldfaithful.py               # full OLDFAITHFUL_START..END range
    python collect_oldfaithful.py --status      # checkpoint summary, then exit

Safe to interrupt and rerun — checkpointed per epoch in data/checkpoint.json
under the "oldfaithful" source key. Each epoch's raw JSONL plugin output is
deleted after ingestion into Parquet (an epoch spans ~2 days of full mainnet
traffic filtered down to our two programs — still not something to keep
around raw across ~dozens of epochs).
"""

import argparse
import logging
import logging.handlers
import os
import shutil
import subprocess
import sys
from pathlib import Path

from config import (
    DATA_DIR,
    JETSTREAMER_BIN,
    OLDFAITHFUL_END,
    OLDFAITHFUL_OUT_DIR,
    OLDFAITHFUL_START,
)
from collectors.checkpoint import Checkpoint
from collectors.day_store import EntityStore
from collectors.epoch_utils import epoch_to_slot_range, estimate_epoch_range
from collectors.oldfaithful_source import process_output_dir
from collectors.rpc_client import SolanaRPC

SOURCE = "oldfaithful"

_LOG_FMT = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def _setup_logging() -> None:
    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "oldfaithful.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(_LOG_FMT, _DATE_FMT))

    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=10 * 1024 * 1024, backupCount=10, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(_LOG_FMT, _DATE_FMT))

    root.addHandler(console)
    root.addHandler(file_handler)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("collect_oldfaithful").info(f"Log file: {log_path}")


logger = logging.getLogger("collect_oldfaithful")


def run_slot_range(
    start_slot: int,
    end_slot: int,
    store: EntityStore,
    out_dir: Path,
    label: str,
) -> dict[str, int]:
    """Replay an arbitrary [start_slot, end_slot) range through the compiled plugin binary, ingest its JSONL output, return per-date event counts."""
    if not JETSTREAMER_BIN.exists():
        raise FileNotFoundError(
            f"Jetstreamer plugin binary not found at {JETSTREAMER_BIN}. "
            f"Build it first: cd pumpfun_filter_plugin && cargo build --release"
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [str(JETSTREAMER_BIN), f"{start_slot}:{end_slot}"]
    env = {
        **os.environ,
        "PUMPFUN_FILTER_OUT_DIR": str(out_dir),
        # Our plugin ignores the `_db` parameter entirely (writes straight to
        # JSONL) — jetstreamer's default "auto" mode tries to spawn/manage its
        # own bundled ClickHouse server regardless, which failed in practice
        # ("Failed to find the PID of the running ClickHouse server", likely a
        # stale process or port clash) for something we don't use at all.
        "JETSTREAMER_CLICKHOUSE_MODE": "off",
    }
    logger.info(f"[{label}] running: {' '.join(cmd)} (out_dir={out_dir})")

    # Stream stderr live instead of buffering until exit — a single epoch
    # replay can run long (~2 days of unfiltered mainnet traffic before our
    # plugin filters it down), and with plain subprocess.run(capture_output=
    # True) nothing is visible at all until the process finishes or fails,
    # making a slow-but-healthy run indistinguishable from a hung one.
    proc = subprocess.Popen(
        cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )
    tail_lines: list[str] = []
    for line in proc.stdout:
        line = line.rstrip("\n")
        logger.info(f"[{label}] jetstreamer: {line}")
        tail_lines.append(line)
        tail_lines = tail_lines[-200:]
    returncode = proc.wait()

    if returncode != 0:
        logger.error(
            f"[{label}] plugin exited {returncode}\n"
            f"output(tail):\n" + "\n".join(tail_lines)
        )
        raise RuntimeError(f"{label} replay failed (exit {returncode})")

    counts = process_output_dir(out_dir, store)
    shutil.rmtree(out_dir, ignore_errors=True)
    return counts


def collect(start_date: str, end_date: str, single_epoch: int | None = None) -> None:
    checkpoint = Checkpoint()
    store = EntityStore()

    if single_epoch is not None:
        epochs = [single_epoch]
    else:
        rpc = SolanaRPC(endpoints=["https://api.mainnet-beta.solana.com"])
        try:
            start_epoch, end_epoch = estimate_epoch_range(start_date, end_date, rpc=rpc)
        finally:
            rpc.close()
        epochs = list(range(start_epoch, end_epoch + 1))

    chunk_names = [f"epoch_{e}" for e in epochs]
    todo = checkpoint.pending_chunks(SOURCE, chunk_names)

    logger.info(f"Checkpoint: {checkpoint.summary(SOURCE)}")
    if not todo:
        logger.info("All requested epochs already done. Nothing to do.")
        return
    logger.info(f"{len(todo)}/{len(epochs)} epoch(s) pending: {todo[0]}..{todo[-1]}")

    dates_touched: set[str] = set()

    try:
        for chunk in todo:
            epoch = int(chunk.split("_", 1)[1])
            checkpoint.mark(SOURCE, chunk, "in_progress")
            start_slot, end_slot = epoch_to_slot_range(epoch)
            out_dir = OLDFAITHFUL_OUT_DIR / chunk
            counts = run_slot_range(start_slot, end_slot, store, out_dir, label=f"epoch {epoch}")
            checkpoint.mark(SOURCE, chunk, "done", events=counts)
            dates_touched.update(counts.keys())
            logger.info(f"=== epoch {epoch} DONE — {counts} ===")

    except KeyboardInterrupt:
        logger.info("Interrupted — flushing buffered data …")
        store.flush_all()

    except Exception:
        logger.error("Fatal error", exc_info=True)
        store.flush_all()
        raise

    finally:
        # compact_day is idempotent/safe to call repeatedly as more epochs
        # touching the same day complete in later runs
        for date in sorted(dates_touched):
            compacted = store.compact_day(date)
            logger.info(f"Compacted {date}: {compacted}")


def status(start_date: str, end_date: str) -> None:
    checkpoint = Checkpoint()
    print(f"\nOld Faithful window: {start_date} .. {end_date}")
    print(f"Checkpoint: {checkpoint.summary(SOURCE)}")


def smoke_test_slots(start_slot: int, end_slot: int) -> None:
    """
    Bounded smoke test over an arbitrary small slot range — does NOT touch
    checkpoint state (same spirit as collect_solarchive.py's --max-shards).
    A full epoch is ~432,000 slots and was observed taking multiple hours and
    generating hundreds of GB of matched data; a few thousand slots finishes
    in minutes and still exercises the whole pipeline (stream -> plugin ->
    JSONL -> decode -> EntityStore -> compact) end to end.
    """
    store = EntityStore()
    out_dir = OLDFAITHFUL_OUT_DIR / f"smoketest_{start_slot}_{end_slot}"
    counts = run_slot_range(start_slot, end_slot, store, out_dir, label=f"slots {start_slot}:{end_slot}")
    logger.info(f"Smoke test slots {start_slot}:{end_slot} -> {counts}")
    for date in counts:
        compacted = store.compact_day(date)
        logger.info(f"Compacted {date}: {compacted}")


def main() -> None:
    _setup_logging()

    parser = argparse.ArgumentParser(
        description="Old Faithful/Jetstreamer-based pump.fun + PumpSwap backfill (Phase B)."
    )
    parser.add_argument("--status", action="store_true", help="Print checkpoint summary and exit.")
    parser.add_argument("--epoch", type=int, default=None, help="Single epoch only.")
    parser.add_argument(
        "--slots", metavar="START:END", default=None,
        help="Bounded smoke test over an arbitrary slot range (e.g. 427680000:427690000 — a few "
             "thousand slots, not a whole epoch). Does not touch checkpoint state.",
    )
    parser.add_argument("--start", default=OLDFAITHFUL_START, help=f"Start date (default: {OLDFAITHFUL_START})")
    parser.add_argument("--end", default=OLDFAITHFUL_END, help=f"End date (default: {OLDFAITHFUL_END})")

    args = parser.parse_args()

    if args.status:
        status(args.start, args.end)
        sys.exit(0)

    if args.slots:
        start_slot, end_slot = (int(x) for x in args.slots.split(":"))
        smoke_test_slots(start_slot, end_slot)
        sys.exit(0)

    collect(args.start, args.end, single_epoch=args.epoch)


if __name__ == "__main__":
    main()
