"""
collect_dune_auto.py
---------------------
Automated, checkpointed, optionally parallel Dune Analytics collector for pump.fun.

Single key (free tier):
    python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13
    → ~2 months to finish (2,500 credits/month, ~250 queries/month)

Multiple keys (parallel workers, one per key):
    python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13 --keys-file keys.txt
    → Speed scales linearly: 3 keys → ~3 weeks; 6 keys → ~10 days

keys.txt format — one Dune API key per line, blank lines and # comments ignored:
    DUNE_KEY_aaaa...
    DUNE_KEY_bbbb...
    DUNE_KEY_cccc...

Other usage:
    python collect_dune_auto.py --status [--keys-file keys.txt]
    python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13 --types trades
    python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13 --budget 50000

Cron (every 4 h, resumes automatically):
    0 */4 * * * cd /path/to/project && python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13 --keys-file keys.txt
"""

import argparse
import logging
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from collectors.dune.checkpoint import DuneCheckpoint
from collectors.dune.dune_runner import DuneRunner
from collectors.dune.queries import (
    ALL_TRADES_SQL,
    MIGRATIONS_SQL,
    TOKEN_CREATIONS_RAW_SQL,
    TOKEN_CREATIONS_SQL,
)
from config import (
    DUNE_API_KEY,
    DUNE_QUERY_CREATIONS,
    DUNE_QUERY_MIGRATIONS,
    DUNE_QUERY_TRADES,
    PARQUET_DIR,
)
from schemas import MIGRATION_COLS, RAW_TRADE_COLS, TOKEN_CREATION_COLS, coerce_schema
from utils import save_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [%(threadName)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TRUNCATION_THRESHOLD = 990_000
CREDITS_PER_QUERY = 10
DEFAULT_MONTHLY_BUDGET = 2_000   # conservative; actual free tier = 2,500/month


# ── window generators ─────────────────────────────────────────────────────────

def _month_windows(start: datetime, end: datetime):
    cur = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cur < end:
        nxt = (cur.replace(year=cur.year + 1, month=1)
               if cur.month == 12
               else cur.replace(month=cur.month + 1))
        yield cur, min(nxt, end), cur.strftime("%Y-%m")
        cur = nxt


def _day_windows(start: datetime, end: datetime):
    cur = start.replace(hour=0, minute=0, second=0, microsecond=0)
    while cur < end:
        nxt = cur + timedelta(days=1)
        yield cur, min(nxt, end), cur.strftime("%Y-%m-%d")
        cur = nxt


def _halve(start: datetime, end: datetime):
    mid = start + (end - start) / 2
    return [(start, mid), (mid, end)]


# ── per-worker collector ──────────────────────────────────────────────────────

class Worker:
    """
    Executes collection queries for a single Dune API key.
    Multiple Workers share one DuneCheckpoint instance (thread-safe).
    """

    def __init__(
        self,
        api_key: str,
        checkpoint: DuneCheckpoint,
        monthly_budget: int,
        out_dir: Path,
        query_ids: dict[str, int],
    ):
        self.api_key = api_key
        self.cp = checkpoint
        self.budget = monthly_budget
        self.out = out_dir
        self.runner = DuneRunner(api_key=api_key)
        self.query_ids = query_ids   # {"creations": N, "trades": N, "migrations": N}

    # ── credit gate ───────────────────────────────────────────────────────────

    def _wait_if_over_budget(self):
        remaining = self.cp.remaining_budget(self.api_key, self.budget)
        if remaining < CREDITS_PER_QUERY:
            now = datetime.utcnow()
            nxt = (now.replace(year=now.year + 1, month=1, day=1)
                   if now.month == 12
                   else now.replace(month=now.month + 1, day=1))
            nxt = nxt.replace(hour=0, minute=5, second=0, microsecond=0)
            secs = (nxt - now).total_seconds()
            logger.warning(
                f"Key ...{self.api_key[-8:]} budget exhausted "
                f"({self.cp.credits_used(self.api_key)}/{self.budget}). "
                f"Sleeping {secs / 3600:.1f} h until {nxt.strftime('%Y-%m-%d')}."
            )
            time.sleep(secs)

    # ── execute with truncation guard ─────────────────────────────────────────

    def _execute(self, query_id: int, params: dict, label: str, depth: int = 0) -> pd.DataFrame:
        self._wait_if_over_budget()
        try:
            df = self.runner.run_saved_query(query_id, params, name=label)
            self.cp.add_credits(self.api_key, CREDITS_PER_QUERY)
        except Exception as exc:
            logger.error(f"[{label}] query failed: {exc}")
            raise

        if len(df) >= TRUNCATION_THRESHOLD:
            if depth >= 2:
                logger.warning(f"[{label}] still truncated at max depth; keeping {len(df):,} rows.")
                return df
            logger.warning(f"[{label}] truncated ({len(df):,} rows). Splitting window.")
            ws = datetime.fromisoformat(params["date_start"].replace(" ", "T"))
            we = datetime.fromisoformat(params["date_end"].replace(" ", "T"))
            parts = []
            for i, (s, e) in enumerate(_halve(ws, we)):
                sub = {**params,
                       "date_start": s.strftime("%Y-%m-%d %H:%M:%S"),
                       "date_end":   e.strftime("%Y-%m-%d %H:%M:%S")}
                parts.append(self._execute(query_id, sub, f"{label}_h{i}", depth + 1))
            return pd.concat(parts, ignore_index=True)

        return df

    # ── window processors (called by the queue dispatcher) ────────────────────

    def process(self, query_type: str, ws: datetime, we: datetime, key: str) -> int:
        """Execute one window. Returns number of rows collected (0 on empty/error)."""
        params = {
            "date_start": ws.strftime("%Y-%m-%d"),
            "date_end":   we.strftime("%Y-%m-%d"),
        }
        label = f"{query_type}_{key}"

        schema_map = {
            "creations":  TOKEN_CREATION_COLS,
            "trades":     RAW_TRADE_COLS,
            "migrations": MIGRATION_COLS,
        }

        try:
            df = self._execute(self.query_ids[query_type], params, label)
        except Exception:
            logger.error(f"Skipping {label} after error.")
            return 0

        if df.empty:
            self.cp.mark_done(query_type, key, 0)
            return 0

        df["source"] = "dune"
        df = coerce_schema(df, schema_map[query_type])
        path = self.out / f"{query_type}_{key}.parquet"
        save_parquet(df, path)
        self.cp.mark_done(query_type, key, len(df))
        logger.info(f"{label}: {len(df):,} rows → {path.name}")
        return len(df)


# ── orchestrator ──────────────────────────────────────────────────────────────

class AutoCollector:
    """
    Distributes pending windows across N parallel workers (one per API key).
    Work is shared via a thread-safe queue; the checkpoint prevents any window
    from being executed more than once even if multiple workers race.
    """

    def __init__(
        self,
        api_keys: list[str],
        query_ids: dict[str, int],
        monthly_budget: int = DEFAULT_MONTHLY_BUDGET,
        out_dir: Path | None = None,
        checkpoint_path: Path = Path("data/dune_checkpoint.json"),
    ):
        if not api_keys:
            raise ValueError("At least one API key is required.")
        self.api_keys = api_keys
        self.query_ids = query_ids
        self.cp = DuneCheckpoint(checkpoint_path)
        self.budget = monthly_budget
        self.out = out_dir or (PARQUET_DIR / "dune")
        self.out.mkdir(parents=True, exist_ok=True)
        self.workers = [
            Worker(key, self.cp, monthly_budget, self.out, query_ids)
            for key in api_keys
        ]

    def _build_queue(self, query_type: str, windows) -> queue.Queue:
        """Return a Queue of (query_type, ws, we, key) for windows not yet done."""
        q: queue.Queue = queue.Queue()
        for ws, we, key in windows:
            if not self.cp.is_done(query_type, key):
                q.put((query_type, ws, we, key))
        return q

    def _run_workers(self, work_queue: queue.Queue, desc: str) -> int:
        """
        Drain work_queue with parallel workers.
        Each worker picks the next item; if the queue is empty it exits.
        Returns total rows collected.
        """
        n_workers = len(self.workers)
        total_rows = 0
        lock = threading.Lock()

        # Progress bar counts completed windows (not rows).
        pbar = tqdm(total=work_queue.qsize(), desc=desc, unit="window")

        def worker_loop(worker: Worker):
            nonlocal total_rows
            while True:
                try:
                    query_type, ws, we, key = work_queue.get_nowait()
                except queue.Empty:
                    break
                rows = worker.process(query_type, ws, we, key)
                with lock:
                    total_rows += rows
                pbar.update(1)

        with ThreadPoolExecutor(max_workers=n_workers,
                                thread_name_prefix="dune-worker") as pool:
            futures = [pool.submit(worker_loop, w) for w in self.workers]
            for f in as_completed(futures):
                exc = f.exception()
                if exc:
                    logger.error(f"Worker raised: {exc}")

        pbar.close()
        return total_rows

    # ── public collection methods ─────────────────────────────────────────────

    def collect_creations(self, start: datetime, end: datetime) -> int:
        windows = list(_month_windows(start, end))
        pending = sum(1 for _, _, k in windows if not self.cp.is_done("creations", k))
        logger.info(f"Creations: {len(windows)} months, {pending} remaining.")
        if pending == 0:
            return 0
        q = self._build_queue("creations", windows)
        return self._run_workers(q, "Creations")

    def collect_migrations(self, start: datetime, end: datetime) -> int:
        windows = list(_month_windows(start, end))
        pending = sum(1 for _, _, k in windows if not self.cp.is_done("migrations", k))
        logger.info(f"Migrations: {len(windows)} months, {pending} remaining.")
        if pending == 0:
            return 0
        q = self._build_queue("migrations", windows)
        return self._run_workers(q, "Migrations")

    def collect_trades(self, start: datetime, end: datetime) -> int:
        windows = list(_day_windows(start, end))
        pending = sum(1 for _, _, k in windows if not self.cp.is_done("trades", k))
        logger.info(f"Trades: {len(windows)} days, {pending} remaining.")
        if pending == 0:
            return 0
        q = self._build_queue("trades", windows)
        return self._run_workers(q, "Trades")

    def status(self):
        print(self.cp.summary(self.api_keys, self.budget))


# ── setup instructions ────────────────────────────────────────────────────────

def _print_setup():
    """Print step-by-step instructions for creating the 3 required Dune queries."""
    from collectors.dune.queries import (
        ALL_TRADES_SQL, MIGRATIONS_SQL, TOKEN_CREATIONS_SQL,
    )
    sep = "─" * 72

    print(f"""
{sep}
  DUNE QUERY SETUP  (one-time, ~5 minutes)
{sep}

The Dune free-tier API cannot CREATE queries — it can only EXECUTE ones
you already saved in the Dune IDE.  You need to create 3 queries manually,
then add their numeric IDs to your .env file.

STEP 1 — Go to https://dune.com  and log in (free account is fine).

STEP 2 — Create the 3 queries below.  For each one:
  a) Click "New Query"
  b) Paste the SQL
  c) Click Run once to confirm it works
  d) Click the query title and give it a recognisable name
  e) Note the numeric ID in the URL:  dune.com/queries/XXXXXXX
  f) Make the query public (or at least saved)

─── Query 1: TOKEN CREATIONS ────────────────────────────────────────────────
Suggested name: pumpfun-creations

{TOKEN_CREATIONS_SQL.strip()}

─── Query 2: ALL TRADES ─────────────────────────────────────────────────────
Suggested name: pumpfun-trades

{ALL_TRADES_SQL.strip()}

─── Query 3: MIGRATIONS ─────────────────────────────────────────────────────
Suggested name: pumpfun-migrations

{MIGRATIONS_SQL.strip()}

{sep}
STEP 3 — Add the IDs to your .env file:

  DUNE_QUERY_CREATIONS=<ID from query 1>
  DUNE_QUERY_TRADES=<ID from query 2>
  DUNE_QUERY_MIGRATIONS=<ID from query 3>

STEP 4 — Run the collector:

  python collect_dune_auto.py --start 2025-01-01 --end 2026-05-13
{sep}
NOTE: The queries use {{{{date_start}}}} and {{{{date_end}}}} parameters.
Dune will auto-detect these as text parameters when you first run them.
Leave the default values empty or set them to any valid date — the script
will always override them at execution time.
{sep}
""")


# ── key / query-id loading ────────────────────────────────────────────────────

def _load_keys(keys_file: str | None, require: bool = True) -> list[str]:
    if keys_file:
        path = Path(keys_file)
        if not path.exists():
            sys.exit(f"Keys file not found: {path}")
        keys = [
            line.strip()
            for line in path.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]
        if not keys:
            sys.exit(f"No keys found in {path}")
        logger.info(f"Loaded {len(keys)} API key(s) from {path}")
        return keys
    if not DUNE_API_KEY or DUNE_API_KEY == "YOUR_DUNE_API_KEY_HERE":
        if require:
            sys.exit(
                "No Dune API key configured. Either set DUNE_API_KEY in .env "
                "or pass --keys-file with a list of keys."
            )
        return []
    return [DUNE_API_KEY]


def _load_query_ids(types: list[str], raw_creations: bool) -> dict[str, int]:
    """
    Load saved Dune query IDs from config. Exits with a helpful message if any
    required ID is missing (0 means not configured).
    """
    from collectors.dune.queries import TOKEN_CREATIONS_RAW_SQL  # noqa: F401
    id_map = {
        "creations":  DUNE_QUERY_CREATIONS,
        "trades":     DUNE_QUERY_TRADES,
        "migrations": DUNE_QUERY_MIGRATIONS,
    }
    missing = [t for t in types if id_map.get(t, 0) == 0]
    if missing:
        sys.exit(
            f"\nMissing Dune query IDs for: {', '.join(missing)}\n"
            f"Run  python collect_dune_auto.py --setup  for instructions.\n"
            f"Then add the IDs to your .env file and retry."
        )
    return {t: id_map[t] for t in types}


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser(
        description="Automated pump.fun data collection via Dune Analytics.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--start", default="2025-01-01",
                   help="Start date inclusive (YYYY-MM-DD). Default: 2025-01-01")
    p.add_argument("--end", default="2026-05-13",
                   help="End date exclusive (YYYY-MM-DD). Default: 2026-05-13")
    p.add_argument("--types", nargs="+",
                   choices=["creations", "trades", "migrations"],
                   default=["creations", "migrations", "trades"],
                   help="Data types to collect. Default: all three.")
    p.add_argument("--keys-file", metavar="FILE",
                   help="Path to a file with one Dune API key per line. "
                        "Each key becomes a parallel worker. "
                        "Falls back to DUNE_API_KEY in .env if omitted.")
    p.add_argument("--budget", type=int, default=DEFAULT_MONTHLY_BUDGET,
                   help=f"Monthly credit budget PER KEY. "
                        f"Default: {DEFAULT_MONTHLY_BUDGET} (free tier ≈ 2,500). "
                        f"Use 50000 for Dune Plus.")
    p.add_argument("--status", action="store_true",
                   help="Print collection progress and exit.")
    p.add_argument("--setup", action="store_true",
                   help="Print step-by-step instructions for creating the required "
                        "Dune queries in the web IDE, then exit.")
    p.add_argument("--raw-creations", action="store_true",
                   help="Use raw instruction_calls fallback for token creations.")
    p.add_argument("--checkpoint", default="data/dune_checkpoint.json",
                   help="Checkpoint file path.")
    return p.parse_args()


def main():
    args = _parse_args()

    if args.setup:
        _print_setup()
        return

    if args.status:
        keys = _load_keys(args.keys_file, require=False)
        cp = DuneCheckpoint(Path(args.checkpoint))
        print(cp.summary(keys, args.budget))
        return

    api_keys  = _load_keys(args.keys_file, require=True)
    query_ids = _load_query_ids(args.types, args.raw_creations)

    collector = AutoCollector(
        api_keys=api_keys,
        query_ids=query_ids,
        monthly_budget=args.budget,
        checkpoint_path=Path(args.checkpoint),
    )

    start = datetime.fromisoformat(args.start)
    end   = datetime.fromisoformat(args.end)

    logger.info(
        f"Starting collection | {start.date()} → {end.date()} | "
        f"types={args.types} | {len(api_keys)} worker(s) | "
        f"{args.budget} credits/key/month"
    )

    total = 0
    if "creations"  in args.types: total += collector.collect_creations(start, end)
    if "migrations" in args.types: total += collector.collect_migrations(start, end)
    if "trades"     in args.types: total += collector.collect_trades(start, end)

    logger.info(f"Session done. Rows collected this run: {total:,}")
    collector.status()


if __name__ == "__main__":
    main()
