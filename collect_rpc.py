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
import sys
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("collect_rpc")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _date_of(unix_ts: int) -> str:
    return datetime.datetime.utcfromtimestamp(unix_ts).strftime("%Y-%m-%d")


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

    total_events = 0
    active_day: str | None = None

    while True:
        page = rpc.get_signatures_for_address(address=program, before=cursor)

        if not page:
            logger.info(f"[{label}] No more signatures — reached beginning of history.")
            _flush_pending(label, store, checkpoint, todo_days)
            break

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
                sig    = sig_row["signature"]
                bt     = sig_row.get("blockTime") or 0
                date   = _date_of(bt) if bt else "unknown"
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
                logger.info(
                    f"[{label}] Day {active_day} complete "
                    f"({rows:,} rows, total events so far: {total_events:,})."
                )

        active_day = new_cursor_day
        cursor     = new_cursor
        set_cursor(cursor)

        buffered = store.buffered_rows()
        buf_tail = ", ".join(f"{d}:{n}" for d, n in sorted(buffered.items())[-3:])
        logger.info(
            f"[{label}] cursor={cursor[:16]}… day={active_day} "
            f"buf=[{buf_tail}] events={total_events:,}"
        )

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
