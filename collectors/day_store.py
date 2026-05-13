"""
collectors/day_store.py
-----------------------
Thread-safe, day-partitioned Parquet writer.

One Parquet file per calendar day:
    data/parquet/transactions_2026-01-01.parquet

A day is written (or merged) to disk when flush_day() is called.
Records for a day accumulate in memory until that point.

Design notes
------------
- Deduplication on (signature, tx_type) when merging with an existing file.
  This makes re-runs safe without creating duplicates.
- zstd compression balances read speed and disk usage.
- The `_mint_creators` side table is updated on every CreateEvent so that
  a later enrichment pass can join coin_creator into trade records.
"""

import logging
import threading
from collections import defaultdict
from pathlib import Path

import pandas as pd

from config import PARQUET_DIR
from schemas import TX_DTYPES, apply_dtypes

logger = logging.getLogger(__name__)

_DEDUP_KEY = ["signature", "tx_type"]


class DayStore:
    def __init__(self, parquet_dir: Path = PARQUET_DIR):
        self._dir   = parquet_dir
        self._lock  = threading.Lock()
        # date (str) → list of record dicts
        self._buffers: dict[str, list[dict]] = defaultdict(list)
        # mint → creator wallet (built from CreateEvents for join enrichment)
        self._mint_creators: dict[str, str] = {}
        # pool address → base mint (built from PumpSwap CreatePoolEvents)
        self._pool_mints: dict[str, str] = {}

    # ── Adding records ────────────────────────────────────────────────────────

    def add(self, date: str, record: dict) -> None:
        """Buffer a decoded event record for the given date (YYYY-MM-DD)."""
        tx_type = record.get("tx_type")

        with self._lock:
            # Build mint→creator index from bonding curve CreateEvents
            if tx_type == "create":
                mint = record.get("mint", "")
                creator = record.get("coin_creator", "")
                if mint and creator:
                    self._mint_creators[mint] = creator

            # Build pool→mint index from PumpSwap CreatePoolEvents
            if tx_type == "pool_create":
                pool = record.get("pool", "")
                mint = record.get("mint", "")
                if pool and mint:
                    self._pool_mints[pool] = mint

            # Enrich PumpSwap buy/sell records with the token mint
            if record.get("source") == "pumpswap" and record.get("mint") is None:
                pool = record.get("pool", "")
                if pool and pool in self._pool_mints:
                    record = dict(record)
                    record["mint"] = self._pool_mints[pool]

            self._buffers[date].append(record)

    def add_many(self, date: str, records: list[dict]) -> None:
        for rec in records:
            self.add(date, rec)

    # ── Flushing ──────────────────────────────────────────────────────────────

    def flush_day(self, date: str) -> int:
        """
        Write buffered records for `date` to Parquet. Returns row count written.
        If the file already exists, merges and deduplicates.
        """
        with self._lock:
            buf = self._buffers.pop(date, [])

        if not buf:
            return 0

        path = self._dir / f"transactions_{date}.parquet"
        df_new = pd.DataFrame(buf)
        df_new = apply_dtypes(df_new, TX_DTYPES)

        if path.exists():
            try:
                df_old = pd.read_parquet(path)
                df = pd.concat([df_old, df_new], ignore_index=True)
                df = df.drop_duplicates(subset=_DEDUP_KEY, keep="last")
            except Exception as exc:
                logger.warning(f"Could not merge {path}: {exc}. Overwriting.")
                df = df_new.drop_duplicates(subset=_DEDUP_KEY, keep="last")
        else:
            df = df_new.drop_duplicates(subset=_DEDUP_KEY, keep="last")

        df.to_parquet(path, index=False, compression="zstd")
        logger.info(f"Wrote {len(df):,} rows → {path.name}")
        return len(df)

    def flush_all(self) -> None:
        """Flush every pending day buffer to disk."""
        with self._lock:
            pending = list(self._buffers.keys())
        for date in sorted(pending):
            self.flush_day(date)

    # ── Mint-creator enrichment ───────────────────────────────────────────────

    def save_mint_creators(self) -> None:
        """
        Persist the mint→creator map to data/parquet/mint_creators.parquet.

        This lets users join coin_creator into trade records that were saved
        before their token's CreateEvent was processed.
        """
        if not self._mint_creators:
            return
        path = self._dir / "mint_creators.parquet"
        df_new = pd.DataFrame(
            list(self._mint_creators.items()),
            columns=["mint", "coin_creator"],
        )
        if path.exists():
            try:
                df_old = pd.read_parquet(path)
                df = pd.concat([df_old, df_new]).drop_duplicates("mint", keep="last")
            except Exception:
                df = df_new
        else:
            df = df_new
        df.to_parquet(path, index=False, compression="zstd")
        logger.info(f"Saved {len(df):,} mint→creator mappings → {path.name}")

    def save_pool_mints(self) -> None:
        """
        Persist the pool→mint map to data/parquet/pool_mints.parquet.

        Needed to join the token mint into PumpSwap buy/sell records when the
        pool was created outside the active collection window.
        """
        if not self._pool_mints:
            return
        path = self._dir / "pool_mints.parquet"
        df_new = pd.DataFrame(
            list(self._pool_mints.items()),
            columns=["pool", "mint"],
        )
        if path.exists():
            try:
                df_old = pd.read_parquet(path)
                df = pd.concat([df_old, df_new]).drop_duplicates("pool", keep="last")
            except Exception:
                df = df_new
        else:
            df = df_new
        df.to_parquet(path, index=False, compression="zstd")
        logger.info(f"Saved {len(df):,} pool→mint mappings → {path.name}")

    # ── Stats ─────────────────────────────────────────────────────────────────

    def buffered_rows(self) -> dict[str, int]:
        with self._lock:
            return {d: len(rows) for d, rows in self._buffers.items()}
