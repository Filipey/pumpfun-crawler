"""
collectors/checkpoint.py
------------------------
Thread-safe checkpoint that persists progress to a JSONL file.

Each line is a JSON record:  {"mint": "...", "status": "done"|"error", "ts": unix}

The checkpoint keeps a fast in-memory set of completed mints so resume is O(1).
"""

import json
import logging
import threading
import time
from pathlib import Path

from config import CHECKPOINT_PATH

logger = logging.getLogger(__name__)


class Checkpoint:
    def __init__(self, path: Path = CHECKPOINT_PATH):
        self.path = path
        self._lock = threading.Lock()
        self._done: set[str] = set()
        self._errors: set[str] = set()
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        with open(self.path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    mint = rec.get("mint", "")
                    if rec.get("status") == "done":
                        self._done.add(mint)
                    elif rec.get("status") == "error":
                        self._errors.add(mint)
                except json.JSONDecodeError:
                    pass
        logger.info(f"Checkpoint loaded: {len(self._done)} done, {len(self._errors)} errors")

    def is_done(self, mint: str) -> bool:
        with self._lock:
            return mint in self._done

    def mark_done(self, mint: str) -> None:
        with self._lock:
            self._done.add(mint)
            self._errors.discard(mint)
            self._append({"mint": mint, "status": "done", "ts": int(time.time())})

    def mark_error(self, mint: str, reason: str = "") -> None:
        with self._lock:
            self._errors.add(mint)
            self._append({"mint": mint, "status": "error", "reason": reason, "ts": int(time.time())})

    def _append(self, record: dict) -> None:
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")

    @property
    def done_count(self) -> int:
        with self._lock:
            return len(self._done)

    @property
    def error_count(self) -> int:
        with self._lock:
            return len(self._errors)

    def summary(self) -> str:
        with self._lock:
            return f"done={len(self._done)}, errors={len(self._errors)}"
