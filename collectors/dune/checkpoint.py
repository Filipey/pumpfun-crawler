"""
collectors/dune/checkpoint.py
------------------------------
Thread-safe checkpoint and per-key credit tracking for automated Dune collection.

The checkpoint JSON lives at data/dune_checkpoint.json and records:
  - completed: which (query_type, window_key) pairs are done
  - credits:   estimated API credit usage per key per calendar month

Credit estimation: ~10 credits per query execution (Dune free = 2,500/month).
With N parallel keys the effective monthly budget is N × 2,500 credits.
"""

import json
import logging
import threading
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_CHECKPOINT_PATH = Path("data/dune_checkpoint.json")


class DuneCheckpoint:
    """
    Tracks completed collection windows and monthly Dune API credit usage.

    Thread-safe: a single instance can be shared across parallel worker threads.
    Credit usage is tracked per API key (identified by the last 8 chars of the key)
    so each key's 2,500/month budget is managed independently.
    """

    def __init__(self, path: Path = DEFAULT_CHECKPOINT_PATH):
        self.path = path
        self._lock = threading.Lock()
        self._data = self._load()

    # ── persistence ──────────────────────────────────────────────────────────

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except json.JSONDecodeError:
                logger.warning(f"Corrupt checkpoint at {self.path}, starting fresh.")
        return {"completed": {}, "credits": {}}

    def _save(self):
        # Called only while self._lock is held.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._data, indent=2))

    # ── window tracking (thread-safe) ────────────────────────────────────────

    def is_done(self, query_type: str, window_key: str) -> bool:
        with self._lock:
            return window_key in self._data["completed"].get(query_type, {})

    def mark_done(self, query_type: str, window_key: str, rows: int):
        with self._lock:
            self._data["completed"].setdefault(query_type, {})[window_key] = {
                "rows": rows,
                "ts": datetime.utcnow().isoformat(),
            }
            self._save()

    def done_count(self, query_type: str) -> int:
        with self._lock:
            return len(self._data["completed"].get(query_type, {}))

    def total_rows(self, query_type: str) -> int:
        with self._lock:
            return sum(
                v["rows"]
                for v in self._data["completed"].get(query_type, {}).values()
            )

    # ── per-key credit tracking (thread-safe) ────────────────────────────────

    @staticmethod
    def _key_id(api_key: str) -> str:
        """Short identifier for a key: last 8 characters."""
        return api_key[-8:] if len(api_key) >= 8 else api_key

    def _month(self) -> str:
        return datetime.utcnow().strftime("%Y-%m")

    def add_credits(self, api_key: str, n: int = 10):
        """Record that n credits were consumed by this key."""
        kid = self._key_id(api_key)
        month = self._month()
        with self._lock:
            bucket = self._data["credits"].setdefault(kid, {})
            bucket[month] = bucket.get(month, 0) + n
            self._save()

    def credits_used(self, api_key: str) -> int:
        """Credits consumed by this key in the current calendar month."""
        kid = self._key_id(api_key)
        with self._lock:
            return self._data["credits"].get(kid, {}).get(self._month(), 0)

    def remaining_budget(self, api_key: str, monthly_limit: int) -> int:
        return max(0, monthly_limit - self.credits_used(api_key))

    # ── reporting ─────────────────────────────────────────────────────────────

    def summary(self, api_keys: list[str], monthly_limit: int = 2000) -> str:
        lines = ["Dune collection progress:"]
        for qtype in ["creations", "migrations", "trades"]:
            n = self.done_count(qtype)
            rows = self.total_rows(qtype)
            lines.append(f"  {qtype:12s}: {n:4d} windows done  |  {rows:>12,} rows")

        if api_keys:
            lines.append("\n  Credit usage this month (per key):")
            for key in api_keys:
                used = self.credits_used(key)
                kid = self._key_id(key)
                lines.append(
                    f"    ...{kid}: {used}/{monthly_limit} "
                    f"({monthly_limit - used} remaining)"
                )

        return "\n".join(lines)
