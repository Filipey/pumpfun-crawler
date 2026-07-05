"""
collect_solarchive.py
----------------------
Phase A CLI: backfill pump.fun + PumpSwap history from the SolArchive Hugging
Face dataset (free, public, currently covers up to 2025-12-05).

Usage
-----
    python collect_solarchive.py --day 2025-12-01     # single day (smoke test)
    python collect_solarchive.py                      # full SOLARCHIVE_START..SOLARCHIVE_END range
    python collect_solarchive.py --status              # checkpoint summary, then exit
    python collect_solarchive.py --workers 4 --max-shards 20   # bounded smoke test

Safe to interrupt (Ctrl-C) and rerun — checkpointed per day, and per-day
ingestion itself flushes in bounded batches (not held in memory for a whole
day) to avoid the memory blowup a full-day, no-flush run caused previously.
"""

import argparse
import datetime
import logging
import logging.handlers
import sys

from config import DATA_DIR, SOLARCHIVE_END, SOLARCHIVE_START, SOLARCHIVE_WORKERS
from collectors.checkpoint import Checkpoint
from collectors.day_store import EntityStore
from collectors.solarchive_source import FLUSH_EVERY_SHARDS, list_day_shards, process_day

SOURCE = "solarchive"

_LOG_FMT = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def _setup_logging() -> None:
    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "solarchive.log"

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
    logging.getLogger("collect_solarchive").info(f"Log file: {log_path}")


logger = logging.getLogger("collect_solarchive")


def _dates_in_range(start: str, end: str) -> list[str]:
    d = datetime.date.fromisoformat(start)
    e = datetime.date.fromisoformat(end)
    days = []
    while d <= e:
        days.append(d.isoformat())
        d += datetime.timedelta(days=1)
    return days


def collect(
    start: str,
    end: str,
    workers: int,
    single_day: str | None = None,
    max_shards: int | None = None,
    flush_every: int = FLUSH_EVERY_SHARDS,
) -> None:
    if single_day:
        start = end = single_day
        logger.info(f"Single-day mode: {single_day}")

    checkpoint = Checkpoint()
    store = EntityStore()

    all_days = _dates_in_range(start, end)
    todo_days = checkpoint.pending_chunks(SOURCE, all_days)

    logger.info(f"Checkpoint: {checkpoint.summary(SOURCE)}")
    if not todo_days:
        logger.info("All requested days already done. Nothing to do.")
        return

    logger.info(f"{len(todo_days)}/{len(all_days)} day(s) pending: {todo_days[0]}..{todo_days[-1]}")

    try:
        for date in todo_days:
            checkpoint.mark(SOURCE, date, "in_progress")
            logger.info(f"=== [{date}] starting ===")

            if max_shards:
                # Bounded smoke-test path: process only the first N shards.
                shard_urls = list_day_shards(date)[:max_shards]
                from collectors.solarchive_source import process_shard

                total_events = 0
                for i, url in enumerate(shard_urls, 1):
                    total_events += process_shard(url, date, store)
                    if i % flush_every == 0:
                        store.flush_batch(date)
                store.flush_batch(date)
                logger.info(f"[{date}] smoke test: {len(shard_urls)} shard(s), {total_events:,} events")
            else:
                total_events = process_day(date, store, workers=workers, flush_every=flush_every)

            compacted = store.compact_day(date)
            checkpoint.mark(SOURCE, date, "done", events=total_events, rows=compacted)
            logger.info(f"=== [{date}] DONE — {total_events:,} events, compacted rows={compacted} ===")

    except KeyboardInterrupt:
        logger.info("Interrupted — flushing buffered data …")
        store.flush_all()
        logger.info("Flushed. Buffered-but-uncompacted days remain 'in_progress' — rerun to resume/compact.")

    except Exception:
        logger.error("Fatal error", exc_info=True)
        store.flush_all()
        raise


def status(start: str, end: str) -> None:
    checkpoint = Checkpoint()
    all_days = _dates_in_range(start, end)
    print(f"\nSolArchive window: {start} .. {end} ({len(all_days)} days)")
    print(f"Checkpoint: {checkpoint.summary(SOURCE)}")
    pending = checkpoint.pending_chunks(SOURCE, all_days)
    print(f"Pending: {len(pending)}/{len(all_days)}")
    if pending:
        print(f"  First pending: {pending[0]}")


def main() -> None:
    _setup_logging()

    parser = argparse.ArgumentParser(description="SolArchive-based pump.fun + PumpSwap backfill (Phase A).")
    parser.add_argument("--status", action="store_true", help="Print checkpoint summary and exit.")
    parser.add_argument("--day", metavar="YYYY-MM-DD", help="Collect a single day only.")
    parser.add_argument("--start", default=SOLARCHIVE_START, help=f"Start date (default: {SOLARCHIVE_START})")
    parser.add_argument("--end", default=SOLARCHIVE_END, help=f"End date (default: {SOLARCHIVE_END})")
    parser.add_argument("--workers", type=int, default=SOLARCHIVE_WORKERS, help="Concurrent shard fetches.")
    parser.add_argument(
        "--max-shards", type=int, default=None,
        help="Only process the first N shards of each day (bounded smoke test; sequential, no concurrency).",
    )
    parser.add_argument(
        "--flush-every", type=int, default=FLUSH_EVERY_SHARDS,
        help=f"Flush buffered rows to disk every N shards (default: {FLUSH_EVERY_SHARDS}).",
    )

    args = parser.parse_args()

    if args.status:
        status(args.start, args.end)
        sys.exit(0)

    collect(
        start=args.start,
        end=args.end,
        workers=args.workers,
        single_day=args.day,
        max_shards=args.max_shards,
        flush_every=args.flush_every,
    )


if __name__ == "__main__":
    main()
