"""
collectors/checkpoint.py
------------------------
Thread-safe checkpoint backed by a JSON file, generalized across ingestion
sources (solarchive, oldfaithful, uri_resolution, ...) and their natural work
unit ("chunk" — a day string, an epoch id, whatever the source uses).

Shape:
    {
      "sources": {
        "solarchive": {
          "2025-11-01": {"status": "done", "rows": 812345, "updated_at": "..."},
          "2025-11-02": {"status": "partial", ...}
        },
        "oldfaithful": { ... }
      },
      "spend": {}
    }

States per chunk: "pending" (absent) -> "in_progress" -> "done" | "partial" | "failed_permanent"
"""

import json
import logging
import threading
import time
from pathlib import Path

from config import CHECKPOINT_PATH

logger = logging.getLogger(__name__)

TERMINAL_STATES = {"done", "failed_permanent"}


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
                data.setdefault("sources", {})
                data.setdefault("spend", {})
                n_sources = len(data["sources"])
                logger.info(f"Checkpoint loaded: {n_sources} source(s) tracked from {self.path}")
                return data
            except Exception as exc:
                logger.warning(f"Could not read checkpoint: {exc}. Starting fresh.")
        return {"sources": {}, "spend": {}}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2)
        tmp.replace(self.path)

    # ── Chunk status ──────────────────────────────────────────────────────────

    def status(self, source: str, chunk: str) -> str:
        """Return the chunk's status, or "pending" if never touched."""
        with self._lock:
            return self._data["sources"].get(source, {}).get(chunk, {}).get("status", "pending")

    def is_done(self, source: str, chunk: str) -> bool:
        return self.status(source, chunk) == "done"

    def mark(self, source: str, chunk: str, status: str, **meta) -> None:
        with self._lock:
            bucket = self._data["sources"].setdefault(source, {})
            entry = bucket.setdefault(chunk, {})
            entry["status"] = status
            entry["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            entry.update(meta)
            self._save()

    def done_chunks(self, source: str) -> list[str]:
        with self._lock:
            bucket = self._data["sources"].get(source, {})
            return sorted(c for c, e in bucket.items() if e.get("status") == "done")

    def pending_chunks(self, source: str, all_chunks: list[str]) -> list[str]:
        """All_chunks not yet in a terminal state for this source."""
        with self._lock:
            bucket = self._data["sources"].get(source, {})
            return [
                c for c in all_chunks
                if bucket.get(c, {}).get("status") not in TERMINAL_STATES
            ]

    # ── Spend tracking (paid-API budget guard; unused by free sources) ───────

    def add_spend(self, source: str, amount: float) -> float:
        with self._lock:
            spend = self._data.setdefault("spend", {})
            spend[source] = spend.get(source, 0.0) + amount
            self._save()
            return spend[source]

    def spend_so_far(self, source: str) -> float:
        with self._lock:
            return self._data.get("spend", {}).get(source, 0.0)

    # ── Summary ───────────────────────────────────────────────────────────────

    def summary(self, source: str | None = None) -> str:
        with self._lock:
            sources = self._data["sources"]
            if source:
                sources = {source: sources.get(source, {})}
            parts = []
            for name, bucket in sources.items():
                counts: dict[str, int] = {}
                for entry in bucket.values():
                    st = entry.get("status", "pending")
                    counts[st] = counts.get(st, 0) + 1
                parts.append(f"{name}: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
            return " | ".join(parts) if parts else "no sources tracked yet"
