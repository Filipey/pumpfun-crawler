"""
collectors/checkpoint.py
------------------------
Thread-safe, day-level checkpoint backed by a JSON file.

Tracks which calendar days have been fully collected so that the crawler
can resume without re-fetching already-processed data.

States per day:
  "done"     → all transactions for this day have been fetched, decoded,
                and written to Parquet
  "partial"  → collection started but not yet complete (e.g. process killed)
"""

import json
import logging
import threading
from pathlib import Path

from config import CHECKPOINT_PATH

logger = logging.getLogger(__name__)


class Checkpoint:
    def __init__(self, path: Path = CHECKPOINT_PATH):
        self.path = path
        self._lock = threading.Lock()
        self._data: dict = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                with open(self.path, encoding="utf-8") as fh:
                    data = json.load(fh)
                logger.info(
                    f"Checkpoint loaded: "
                    f"{len(data.get('done', []))} done days, "
                    f"cursor={data.get('cursor')}"
                )
                return data
            except Exception as exc:
                logger.warning(f"Could not read checkpoint: {exc}. Starting fresh.")
        return {"done": [], "partial": [], "cursor": None, "pumpswap_cursor": None}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2)
        tmp.replace(self.path)

    # ── Day status ────────────────────────────────────────────────────────────

    def is_done(self, date: str) -> bool:
        with self._lock:
            return date in self._data["done"]

    def mark_done(self, date: str) -> None:
        with self._lock:
            done = self._data.setdefault("done", [])
            partial = self._data.setdefault("partial", [])
            if date not in done:
                done.append(date)
            if date in partial:
                partial.remove(date)
            self._save()

    def mark_partial(self, date: str) -> None:
        with self._lock:
            partial = self._data.setdefault("partial", [])
            if date not in partial:
                partial.append(date)
            self._save()

    # ── Pagination cursor ─────────────────────────────────────────────────────

    @property
    def cursor(self) -> str | None:
        """Last pump.fun bonding curve signature processed."""
        with self._lock:
            return self._data.get("cursor")

    def set_cursor(self, signature: str | None) -> None:
        with self._lock:
            self._data["cursor"] = signature
            self._save()

    @property
    def pumpswap_cursor(self) -> str | None:
        """Last PumpSwap AMM signature processed."""
        with self._lock:
            return self._data.get("pumpswap_cursor")

    def set_pumpswap_cursor(self, signature: str | None) -> None:
        with self._lock:
            self._data["pumpswap_cursor"] = signature
            self._save()

    # ── Summary ───────────────────────────────────────────────────────────────

    @property
    def done_days(self) -> list[str]:
        with self._lock:
            return sorted(self._data.get("done", []))

    def summary(self) -> str:
        with self._lock:
            n_done      = len(self._data.get("done", []))
            n_partial   = len(self._data.get("partial", []))
            cursor      = self._data.get("cursor", "none")
            ps_cursor   = self._data.get("pumpswap_cursor", "none")
        return (
            f"done={n_done} days, partial={n_partial}, "
            f"pumpfun_cursor={cursor}, pumpswap_cursor={ps_cursor}"
        )
