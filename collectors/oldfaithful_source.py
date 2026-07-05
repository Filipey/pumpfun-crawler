"""
collectors/oldfaithful_source.py
----------------------------------
Phase B ingestion adapter: reads the JSONL output produced by the
`pumpfun_filter_plugin` Rust Jetstreamer plugin (see that crate's module
docstring — it is UNCOMPILED/UNVERIFIED, written without a Rust toolchain
available) and feeds matching transactions through the same
collectors/decoder.py + EntityStore pipeline Phase A uses.

Unlike Phase A (one SolArchive day = one self-contained set of shards), one
Jetstreamer epoch run spans ~2 calendar days of transactions mixed together
across several `part-<thread_id>.jsonl` files. This module buckets each
record into its correct calendar day using that record's own `block_time`
(exactly like Phase A does), and flushes in bounded batches — Phase A's
memory-blowup lesson applies here even more, since a single epoch can contain
on the order of 10M+ matched transactions (~2 days worth at pump.fun/PumpSwap
volume), not the few hundred thousand a single SolArchive shard produces.
"""

import json
import logging
from pathlib import Path

from collectors.day_store import EntityStore
from collectors.decoder import decode_pumpswap_transaction, decode_transaction

logger = logging.getLogger(__name__)

SOURCE_PHASE = "oldfaithful"

# Flush every this many decoded-and-routed events (summed across all dates
# touched since the last flush), not per-file — a single part file from one
# epoch can be far larger than anything Phase A ever handled per-shard.
FLUSH_EVERY_EVENTS = 50_000


def _record_to_tx(record: dict) -> tuple[dict, str]:
    err = "error" if record.get("err") else None
    tx = {
        "slot": record["slot"],
        "blockTime": record["block_time"],
        "meta": {
            "err": err,
            "logMessages": record.get("log_messages") or [],
        },
    }
    return tx, record["signature"]


def _date_of(unix_ts: int) -> str:
    import datetime

    return datetime.datetime.fromtimestamp(unix_ts, datetime.timezone.utc).strftime("%Y-%m-%d")


def process_output_dir(out_dir: Path, store: EntityStore) -> dict[str, int]:
    """
    Stream every part-*.jsonl file in `out_dir`, decode matching transactions,
    bucket by calendar day, and flush in bounded batches. Returns event count
    per date touched. Does NOT compact — caller compacts affected dates once
    all epochs touching them are done.
    """
    part_files = sorted(out_dir.glob("part-*.jsonl"))
    if not part_files:
        logger.warning(f"No plugin output found in {out_dir}")
        return {}

    events_by_date: dict[str, int] = {}
    since_flush = 0
    dates_pending_flush: set[str] = set()
    skipped_no_block_time = 0

    def flush_pending():
        nonlocal since_flush
        for d in sorted(dates_pending_flush):
            store.flush_batch(d)
        dates_pending_flush.clear()
        since_flush = 0

    for part_file in part_files:
        with open(part_file, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning(f"Skipping malformed JSON line in {part_file}")
                    continue

                if record.get("block_time") is None:
                    skipped_no_block_time += 1
                    continue

                date = _date_of(record["block_time"])
                tx, sig = _record_to_tx(record)
                events = decode_transaction(tx, sig) + decode_pumpswap_transaction(tx, sig)
                for ev in events:
                    ev["date"] = date
                    ev["source_phase"] = SOURCE_PHASE
                    store.add(date, ev)

                if events:
                    events_by_date[date] = events_by_date.get(date, 0) + len(events)
                    dates_pending_flush.add(date)
                    since_flush += len(events)

                if since_flush >= FLUSH_EVERY_EVENTS:
                    flush_pending()

        logger.info(f"[oldfaithful] processed {part_file.name}, running totals: {events_by_date}")

    flush_pending()

    if skipped_no_block_time:
        logger.warning(
            f"[oldfaithful] {skipped_no_block_time} record(s) had no block_time "
            f"(should not happen for a real produced block — investigate if this is large)"
        )

    return events_by_date
