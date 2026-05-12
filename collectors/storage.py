"""
collectors/storage.py
---------------------
Thread-safe incremental Parquet writer.

Accumulates rows in memory and flushes to partitioned Parquet files when
the buffer exceeds FLUSH_EVERY rows or flush() is called explicitly.

Output layout:
  data/parquet/tokens.parquet          – all tokens (one row per mint)
  data/parquet/trades_graduated.parquet
  data/parquet/trades_active.parquet
  data/parquet/trades_dead.parquet     – no trades, only token row saved here
"""

import logging
import threading
from pathlib import Path
from typing import Literal

import pandas as pd

from config import PARQUET_DIR
from schemas import TOKEN_DTYPES, TRADE_DTYPES, apply_dtypes

logger = logging.getLogger(__name__)

FLUSH_EVERY = 5_000   # rows before auto-flush to disk

Category = Literal["graduated", "active", "dead"]


class ParquetStore:
    """
    Collects rows into buffers; flushes by merging with existing Parquet files.

    Using read-merge-write keeps files small on disk even across restarts,
    at the cost of re-reading the file on each flush (acceptable for our sizes).
    """

    def __init__(self, parquet_dir: Path = PARQUET_DIR):
        self._dir = parquet_dir
        self._lock = threading.Lock()

        # Buffers: list of dicts
        self._token_buf:  list[dict] = []
        self._trade_bufs: dict[Category, list[dict]] = {
            "graduated": [],
            "active":    [],
            "dead":      [],
        }

    # ── Public API ────────────────────────────────────────────────────────────

    def add_token(self, token: dict) -> None:
        with self._lock:
            self._token_buf.append(token)
            if len(self._token_buf) >= FLUSH_EVERY:
                self._flush_tokens()

    def add_trades(self, category: Category, trades: list[dict]) -> None:
        if not trades:
            return
        with self._lock:
            buf = self._trade_bufs[category]
            buf.extend(trades)
            if len(buf) >= FLUSH_EVERY:
                self._flush_trades(category)

    def flush(self) -> None:
        """Force-flush all pending buffers to disk."""
        with self._lock:
            self._flush_tokens()
            for cat in self._trade_bufs:
                self._flush_trades(cat)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _flush_tokens(self) -> None:
        """Must be called with self._lock held."""
        if not self._token_buf:
            return
        path = self._dir / "tokens.parquet"
        df_new = pd.DataFrame(self._token_buf)
        df_new = apply_dtypes(df_new, TOKEN_DTYPES)
        df_merged = self._merge(path, df_new, key="mint")
        df_merged.to_parquet(path, index=False, compression="zstd")
        logger.info(f"Flushed {len(self._token_buf)} tokens → {path} ({len(df_merged)} total)")
        self._token_buf.clear()

    def _flush_trades(self, category: Category) -> None:
        """Must be called with self._lock held."""
        buf = self._trade_bufs[category]
        if not buf:
            return
        path = self._dir / f"trades_{category}.parquet"
        df_new = pd.DataFrame(buf)
        df_new = apply_dtypes(df_new, TRADE_DTYPES)
        df_merged = self._merge(path, df_new, key="signature")
        df_merged.to_parquet(path, index=False, compression="zstd")
        logger.info(f"Flushed {len(buf)} {category} trades → {path} ({len(df_merged)} total)")
        buf.clear()

    @staticmethod
    def _merge(path: Path, df_new: pd.DataFrame, key: str) -> pd.DataFrame:
        """Read existing Parquet (if any), append new rows, deduplicate on key."""
        if path.exists():
            try:
                df_old = pd.read_parquet(path)
                combined = pd.concat([df_old, df_new], ignore_index=True)
                combined = combined.drop_duplicates(subset=[key], keep="last")
                return combined
            except Exception as exc:
                logger.warning(f"Could not read {path} for merge: {exc}. Overwriting.")
        return df_new.drop_duplicates(subset=[key], keep="last")

    def stats(self) -> dict:
        with self._lock:
            return {
                "token_buffer":         len(self._token_buf),
                "trades_graduated_buf": len(self._trade_bufs["graduated"]),
                "trades_active_buf":    len(self._trade_bufs["active"]),
                "trades_dead_buf":      len(self._trade_bufs["dead"]),
            }
