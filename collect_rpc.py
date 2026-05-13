"""
collect_rpc.py
--------------
Main CLI for the Solana RPC-based pump.fun + PumpSwap crawler.

Two programs are collected, each with its own backward-pagination cursor:

  1. pump.fun launchpad  (6EF8…) — CreateEvent, TradeEvent, CompleteEvent
  2. PumpSwap AMM        (pAMM…) — CreatePoolEvent, BuyEvent, SellEvent

Both write into the same day-partitioned Parquet files (distinguished by the
`source` column: "pumpfun" | "pumpswap").

Usage
-----
    python collect_rpc.py              # full run (both programs)
    python collect_rpc.py --status     # show progress, then exit
    python collect_rpc.py --day 2026-05-01   # single day, both programs
    python collect_rpc.py --program pumpfun  # only bonding curve
    python collect_rpc.py --program pumpswap # only AMM
    python collect_rpc.py --workers 8
"""

import argparse
import datetime
import logging
import logging.handlers
import sys
import time
from typing import Callable

from config import (
    COLLECT_END,
    COLLECT_START,
    PUMPFUN_PROGRAM,
    PUMPSWAP_PROGRAM,
    WORKERS,
)
from collectors.checkpoint import Checkpoint
from collectors.day_store import DayStore
from collectors.decoder import decode_transaction, decode_pumpswap_transaction
from collectors.rpc_client import SolanaRPC

# ── Logging ───────────────────────────────────────────────────────────────────

_LOG_FMT  = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def _setup_logging() -> None:
    from config import DATA_DIR

    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "crawler.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Console — INFO and above
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(_LOG_FMT, _DATE_FMT))

    # Rotating file — DEBUG and above; 10 MB per file, keep last 10 files
    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=10 * 1024 * 1024, backupCount=10, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(_LOG_FMT, _DATE_FMT))

    root.addHandler(console)
    root.addHandler(file_handler)

    # urllib3 debug lines print the full URL including API keys — keep it quiet
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logging.getLogger("collect_rpc").info(f"Log file: {log_path}")


logger = logging.getLogger("collect_rpc")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _date_of(unix_ts: int) -> str:
    return datetime.datetime.fromtimestamp(unix_ts, datetime.timezone.utc).strftime("%Y-%m-%d")


def _dates_in_range(start: str, end: str) -> list[str]:
    d = datetime.date.fromisoformat(start)
    e = datetime.date.fromisoformat(end)
    days = []
    while d <= e:
        days.append(d.isoformat())
        d += datetime.timedelta(days=1)
    return days


# ── Core collection loop (shared by both programs) ────────────────────────────

def _collect_program(
    label: str,
    program: str,
    decoder: Callable,
    get_cursor: Callable,
    set_cursor: Callable,
    rpc: SolanaRPC,
    checkpoint: Checkpoint,
    store: DayStore,
    start_ts: int,
    end_ts: int,
    todo_days: set[str],
) -> int:
    """
    Paginate backward through `program` transactions, decode events, and
    buffer them in DayStore. Flushes completed days as the cursor advances.

    Returns total event count processed.
    """
    cursor = get_cursor()
    if cursor:
        logger.info(f"[{label}] Resuming from cursor {cursor[:16]}…")

    total_events  = 0
    total_pages   = 0
    days_done     = 0
    active_day: str | None = None
    t_start       = time.monotonic()
    t_last_log    = t_start      # rate-limit progress lines to ~every 15s

    while True:
        page = rpc.get_signatures_for_address(address=program, before=cursor)

        if not page:
            logger.info(f"[{label}] No more signatures — reached beginning of history.")
            _flush_pending(label, store, checkpoint, todo_days)
            break

        total_pages  += 1
        oldest_ts = page[-1].get("blockTime") or 0
        newest_ts = page[0].get("blockTime") or 0

        # Page is entirely newer than our window — skip forward
        if oldest_ts and oldest_ts > end_ts:
            cursor = page[-1]["signature"]
            set_cursor(cursor)
            continue

        # Page is entirely older than our window — done
        if newest_ts and newest_ts < start_ts:
            logger.info(f"[{label}] Reached start of collection window.")
            _flush_pending(label, store, checkpoint, todo_days)
            break

        # Filter to window, skip failed transactions
        in_window = [
            r for r in page
            if start_ts <= (r.get("blockTime") or 0) <= end_ts
            and r.get("err") is None
        ]

        # Mark touched days as partial
        for r in in_window:
            if r.get("blockTime"):
                d = _date_of(r["blockTime"])
                if d in todo_days and not checkpoint.is_done(d):
                    checkpoint.mark_partial(d)

        if in_window:
            sigs = [r["signature"] for r in in_window]
            txs  = rpc.get_transactions(sigs)
            for sig_row, tx in zip(in_window, txs):
                sig  = sig_row["signature"]
                bt   = sig_row.get("blockTime") or 0
                date = _date_of(bt) if bt else "unknown"
                if date not in todo_days:
                    continue
                events = decoder(tx, sig)
                for ev in events:
                    ev["date"] = date
                    store.add(date, ev)
                total_events += len(events)

        # Advance cursor and flush completed days
        new_cursor     = page[-1]["signature"]
        new_cursor_ts  = page[-1].get("blockTime") or 0
        new_cursor_day = _date_of(new_cursor_ts) if new_cursor_ts else None

        if active_day and new_cursor_day and new_cursor_day < active_day:
            if active_day in todo_days and not checkpoint.is_done(active_day):
                rows = store.flush_day(active_day)
                checkpoint.mark_done(active_day)
                days_done += 1
                remaining = len(todo_days) - days_done
                elapsed   = time.monotonic() - t_start
                ev_s      = total_events / elapsed if elapsed > 0 else 0
                logger.info(
                    f"[{label}] DONE day={active_day} "
                    f"rows={rows:,} | "
                    f"total_events={total_events:,} | "
                    f"speed={ev_s:,.0f} ev/s | "
                    f"days_remaining={remaining}"
                )

        active_day = new_cursor_day
        cursor     = new_cursor
        set_cursor(cursor)

        # Progress line — rate-limited to once every 15 seconds
        now = time.monotonic()
        if now - t_last_log >= 15:
            elapsed = now - t_start
            ev_s    = total_events / elapsed if elapsed > 0 else 0
            logger.info(
                f"[{label}] "
                f"date={active_day} | "
                f"pages={total_pages:,} | "
                f"events={total_events:,} | "
                f"speed={ev_s:,.0f} ev/s | "
                f"days_done={days_done}/{len(todo_days)}"
            )
            t_last_log = now

        if new_cursor_ts and new_cursor_ts < start_ts:
            logger.info(f"[{label}] Passed start boundary. Done.")
            _flush_pending(label, store, checkpoint, todo_days)
            break

    return total_events


def _flush_pending(
    label: str,
    store: DayStore,
    checkpoint: Checkpoint,
    todo_days: set[str],
) -> None:
    for day in sorted(store.buffered_rows()):
        if day in todo_days:
            rows = store.flush_day(day)
            checkpoint.mark_done(day)
            logger.info(f"[{label}] Flushed {day} ({rows:,} rows).")


# ── Top-level collect ─────────────────────────────────────────────────────────

def collect(
    start: str,
    end: str,
    workers: int,
    single_day: str | None = None,
    program_filter: str | None = None,
) -> None:
    if single_day:
        start = end = single_day
        logger.info(f"Single-day mode: {single_day}")

    start_ts = int(datetime.datetime.fromisoformat(start + "T00:00:00").timestamp())
    end_ts   = int(datetime.datetime.fromisoformat(end   + "T23:59:59").timestamp())

    checkpoint = Checkpoint()
    store      = DayStore()
    rpc        = SolanaRPC()

    logger.info(f"Checkpoint: {checkpoint.summary()}")

    all_days  = _dates_in_range(start, end)
    todo_days = set(d for d in all_days if not checkpoint.is_done(d))

    if not todo_days:
        logger.info("All days already collected. Nothing to do.")
        return

    programs = []
    if program_filter in (None, "pumpfun"):
        programs.append((
            "pumpfun",
            PUMPFUN_PROGRAM,
            decode_transaction,
            checkpoint.cursor,
            checkpoint.set_cursor,
        ))
    if program_filter in (None, "pumpswap"):
        programs.append((
            "pumpswap",
            PUMPSWAP_PROGRAM,
            decode_pumpswap_transaction,
            checkpoint.pumpswap_cursor,
            checkpoint.set_pumpswap_cursor,
        ))

    try:
        for label, program, decoder, get_cur, set_cur in programs:
            logger.info(f"=== Starting {label} collection ===")
            n = _collect_program(
                label=label,
                program=program,
                decoder=decoder,
                get_cursor=lambda gc=get_cur: gc() if callable(gc) else gc,
                set_cursor=set_cur,
                rpc=rpc,
                checkpoint=checkpoint,
                store=store,
                start_ts=start_ts,
                end_ts=end_ts,
                todo_days=todo_days,
            )
            logger.info(f"=== {label} done — {n:,} events ===")

        store.save_mint_creators()
        store.save_pool_mints()

    except KeyboardInterrupt:
        logger.info("Interrupted — flushing buffered data …")
        store.flush_all()
        store.save_mint_creators()
        store.save_pool_mints()
        logger.info("Flushed. Restart to resume.")

    except Exception as exc:
        logger.error(f"Fatal error: {exc}", exc_info=True)
        store.flush_all()
        store.save_mint_creators()
        store.save_pool_mints()
        raise

    finally:
        rpc.close()


# ── Status ────────────────────────────────────────────────────────────────────

def status(start: str, end: str) -> None:
    checkpoint = Checkpoint()
    all_days   = _dates_in_range(start, end)
    done_days  = set(checkpoint.done_days)

    print(f"\nCollection window : {start} → {end}  ({len(all_days)} days)")
    print(f"Checkpoint        : {checkpoint.summary()}")

    done_in    = [d for d in all_days if d in done_days]
    pending_in = [d for d in all_days if d not in done_days]

    print(f"Done   : {len(done_in):>4} day(s)")
    print(f"Pending: {len(pending_in):>4} day(s)")
    if pending_in:
        sample = pending_in[:5]
        print(f"  First pending: {', '.join(sample)}{'…' if len(pending_in) > 5 else ''}")

    from config import PARQUET_DIR
    files      = sorted(PARQUET_DIR.glob("transactions_*.parquet"))
    total_size = sum(f.stat().st_size for f in files)
    print(f"\nParquet files : {len(files)} ({total_size / 1e9:.2f} GB)")

    side_tables = ["mint_creators.parquet", "pool_mints.parquet"]
    for name in side_tables:
        p = PARQUET_DIR / name
        if p.exists():
            print(f"  {name}: {p.stat().st_size / 1e6:.1f} MB")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    _setup_logging()

    parser = argparse.ArgumentParser(
        description="pump.fun + PumpSwap Solana RPC crawler."
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Print checkpoint summary and exit.",
    )
    parser.add_argument(
        "--day", metavar="YYYY-MM-DD",
        help="Collect a single day only (for testing).",
    )
    parser.add_argument(
        "--start", default=COLLECT_START,
        help=f"Start date (default: {COLLECT_START})",
    )
    parser.add_argument(
        "--end", default=COLLECT_END,
        help=f"End date (default: {COLLECT_END})",
    )
    parser.add_argument(
        "--program", choices=["pumpfun", "pumpswap"],
        help="Collect only one program (default: both).",
    )
    parser.add_argument(
        "--workers", type=int, default=WORKERS,
        help=f"Parallel decode workers (default: {WORKERS})",
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
        program_filter=args.program,
    )


if __name__ == "__main__":
    main()
