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
from collectors.solarchive_source import (
    FLUSH_EVERY_SHARDS,
    QUALITY_PROBE_MIN_RATIO,
    list_day_shards,
    probe_day_log_quality,
    process_day,
)

SOURCE = "solarchive"

# pump.fun bonding-curve volume alone was ~2-5M trades/day per arXiv:2602.14860
# (Sept 2025 figures) — a day finishing with far fewer events than that is a
# collection failure, not real data (this exact bug happened once: a broken
# duckdb version silently returned 0 rows for every shard with no error, and
# the day still got marked "done"). Refuse to auto-accept a suspiciously low
# count; require --force-accept to override for a genuinely quiet day.
MIN_PLAUSIBLE_DAY_EVENTS = 50_000

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
    force_accept: bool = False,
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

            if not max_shards and not force_accept:
                usable, ratio, sample_n = probe_day_log_quality(date)
                if not usable:
                    checkpoint.mark(SOURCE, date, "partial", quality_probe_ratio=ratio, quality_probe_sample=sample_n)
                    logger.error(
                        f"=== [{date}] SKIPPED — quality probe found only {ratio:.0%} of "
                        f"{sample_n} sampled rows have real log_messages (need >={QUALITY_PROBE_MIN_RATIO:.0%}). "
                        f"SolArchive's backfill for this day appears incomplete/corrupted at the "
                        f"source (confirmed not a read bug — reproduces even reading a fully "
                        f"downloaded local copy). Not spending time on the full ~200-300 shards. "
                        f"Pass --force-accept to process anyway if you believe this is wrong."
                    )
                    continue
                logger.info(f"[{date}] quality probe OK: {ratio:.0%} of {sample_n} sampled rows have real log_messages")

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

            if max_shards:
                # Bounded smoke test — only a subset of shards were processed,
                # so this day is NOT complete. Leave checkpoint at
                # 'in_progress' (non-terminal — still picked up as pending by
                # a future real run) rather than marking it 'done'. A prior
                # version marked smoke-tested days 'done', which permanently
                # skipped them from real backfill runs while only holding a
                # few shards' worth of data (caught in production: 2025-12-01
                # sat at 326,740 events — one smoke test's worth — instead of
                # the ~8-9M a real day has).
                logger.info(
                    f"[{date}] smoke test only ({max_shards} shard(s)) — NOT marked done, "
                    f"still pending for a real run"
                )
            elif total_events < MIN_PLAUSIBLE_DAY_EVENTS and not force_accept:
                checkpoint.mark(SOURCE, date, "partial", events=total_events, rows=compacted)
                logger.error(
                    f"=== [{date}] SUSPICIOUSLY LOW — only {total_events:,} events "
                    f"(expected {MIN_PLAUSIBLE_DAY_EVENTS:,}+). NOT marking done — this "
                    f"almost always means a read failure was silently swallowed (check for "
                    f"duckdb/httpfs errors above, and confirm `duckdb.__version__` is >=1.5.5 — "
                    f"older versions can silently return 0 rows against HuggingFace's Xet "
                    f"storage instead of raising). Rerun this day after investigating; pass "
                    f"--force-accept if it's genuinely correct."
                )
            else:
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


def reset_days(days: list[str]) -> None:
    """Revert the given days to 'pending' — e.g. after a bad run (bug, corrupted read) marked them done incorrectly. Does not touch already-written Parquet files; rerun the day to overwrite/dedup them."""
    checkpoint = Checkpoint()
    for date in days:
        before = checkpoint.status(SOURCE, date)
        checkpoint.reset(SOURCE, date)
        logger.info(f"Reset {date}: {before} -> pending")


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
    parser.add_argument(
        "--reset", metavar="YYYY-MM-DD", nargs="+", default=None,
        help="Revert the given day(s) to pending (e.g. to redo a day wrongly marked done), then exit.",
    )
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
    parser.add_argument(
        "--force-accept", action="store_true",
        help=f"Mark a day 'done' even if under {MIN_PLAUSIBLE_DAY_EVENTS:,} events (bypasses the low-event-count safeguard).",
    )

    args = parser.parse_args()

    if args.reset:
        reset_days(args.reset)
        sys.exit(0)

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
        force_accept=args.force_accept,
    )


if __name__ == "__main__":
    main()
