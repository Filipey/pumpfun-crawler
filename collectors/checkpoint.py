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
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path

try:
    import fcntl
except ImportError:                       # non-POSIX; degrades to thread-only
    fcntl = None

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

    @contextmanager
    def _file_lock(self):
        """
        Exclusive inter-process lock around a read-modify-write of the file.

        Stages are meant to be runnable concurrently — `--stage images` for the
        days metadata has already finished, while `--stage metadata` works
        forward — and they share one checkpoint file. Each process holds its
        own in-memory copy loaded at startup, so without this the second writer
        serialises its stale dict over the first writer's updates and days
        silently revert to pending. A thread lock does not help across
        processes.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if fcntl is None:
            yield
            return
        lock_path = self.path.with_suffix(".lock")
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def _merge_from_disk(self) -> None:
        """Fold whatever another process has written into our in-memory copy."""
        if not self.path.exists():
            return
        try:
            with open(self.path, encoding="utf-8") as fh:
                on_disk = json.load(fh)
        except Exception as exc:
            logger.warning(f"Could not re-read checkpoint before write: {exc}")
            return
        # The file is authoritative for every entry except the one edit this
        # call is making, which the caller re-applies afterwards. setdefault
        # here instead would keep whatever this process happened to read
        # earlier, so a long-running stage would serialise its stale snapshot
        # of another stage's days back over that stage's newer progress —
        # observed reverting a day from "done" to "in_progress".
        for source, bucket in on_disk.get("sources", {}).items():
            self._data["sources"].setdefault(source, {}).update(bucket)
        for k, v in on_disk.get("spend", {}).items():
            self._data.setdefault("spend", {})[k] = max(
                v, self._data.get("spend", {}).get(k, 0.0)
            )

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # PID in the temp name: two processes sharing one ".tmp" path raced on
        # the rename and died with FileNotFoundError, one clobbering the other's
        # half-written file. The file lock above already serialises writers,
        # but a unique scratch file costs nothing and removes the failure mode
        # entirely — including where fcntl is unavailable.
        tmp = self.path.with_suffix(f".tmp.{os.getpid()}")
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(self._data, fh, indent=2)
            tmp.replace(self.path)
        finally:
            tmp.unlink(missing_ok=True)

    # ── Chunk status ──────────────────────────────────────────────────────────

    def status(self, source: str, chunk: str) -> str:
        """Return the chunk's status, or "pending" if never touched."""
        with self._lock:
            return self._data["sources"].get(source, {}).get(chunk, {}).get("status", "pending")

    def is_done(self, source: str, chunk: str) -> bool:
        return self.status(source, chunk) == "done"

    def mark(self, source: str, chunk: str, status: str, **meta) -> None:
        with self._lock, self._file_lock():
            bucket = self._data["sources"].setdefault(source, {})
            entry = bucket.setdefault(chunk, {})
            entry["status"] = status
            entry["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            entry.update(meta)
            self._merge_from_disk()
            # _merge_from_disk uses setdefault, so re-apply our own edit in
            # case the on-disk copy also had this chunk.
            self._data["sources"][source][chunk] = entry
            self._save()

    def reset(self, source: str, chunk: str) -> None:
        """Remove a chunk's entry entirely, reverting it to 'pending' (never touched)."""
        with self._lock, self._file_lock():
            self._merge_from_disk()
            self._data["sources"].get(source, {}).pop(chunk, None)
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
        with self._lock, self._file_lock():
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
