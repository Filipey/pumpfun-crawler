"""
collectors/blob_store.py
------------------------
Content-addressed image blob storage as Parquet shards, in the layout
HuggingFace `datasets` expects for an image column:

    <IMAGES_DIR>/shard_000000.parquet
    <IMAGES_DIR>/shard_000001.parquet
    ...

Each shard holds `IMG_SHARD_ROWS` rows of
`(sha256, bytes, content_type, width, height)` — roughly 176 MB at the
observed ~17.6 KB mean stored size.

Why shards and not one file per image
-------------------------------------
At ~9.3M tokens, one file per image means 9.3M inodes. That makes rsync,
backup and even `du` painful, and can exhaust the inode table on a filesystem
sized for a normal workload. ~930 shard files do not.

Dedup
-----
Images are keyed by the SHA-256 of their *stored* (re-encoded) bytes, so
tokens that reuse the same artwork — ~15% of a sampled 100, and likely more at
scale, since recycled scam art and template launches are common — share one
blob. `token_images` is the ledger that maps mint -> sha256 -> shard; the
filesystem is never scanned to decide what still needs fetching.

The in-memory dedup index costs roughly 1 GB of RSS at 9.3M distinct images.
Pass `dedup=False` (CLI: `--no-dedup`) to trade that memory for re-storing
duplicate blobs.

Crash safety
------------
The currently-open shard is rewritten in full (atomic tmp + rename) on every
flush, so an interrupted run leaves a valid Parquet file. Once a shard reaches
the row cap it is sealed and never rewritten.
"""

import hashlib
import logging
import threading
from pathlib import Path

import pandas as pd

from config import IMAGES_DIR, IMG_SHARD_ROWS

logger = logging.getLogger(__name__)

SHARD_GLOB = "shard_*.parquet"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ImageBlobStore:
    def __init__(
        self,
        images_dir: Path = IMAGES_DIR,
        shard_rows: int = IMG_SHARD_ROWS,
        dedup: bool = True,
    ):
        self._dir = Path(images_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._shard_rows = shard_rows
        self._dedup = dedup
        self._lock = threading.Lock()
        self._buf: list[dict] = []
        self._index: dict[str, str] = {}      # sha256 -> shard filename
        self._shard_no = 0
        self._scan_existing()

    # ── Startup ───────────────────────────────────────────────────────────────

    def _scan_existing(self) -> None:
        """
        Resume into the existing shard set: find the highest shard number, load
        its rows back into the buffer if it is still under the cap (so it keeps
        filling rather than leaving a stunted shard behind), and build the
        dedup index from every shard's sha256 column. Reading one column out of
        Parquet is cheap — the blob column is never touched here.
        """
        shards = sorted(self._dir.glob(SHARD_GLOB))
        if not shards:
            return

        if self._dedup:
            n = 0
            for path in shards:
                try:
                    df = pd.read_parquet(path, columns=["sha256"])
                except Exception as exc:
                    logger.warning(f"Could not read {path.name} for dedup index: {exc}")
                    continue
                name = path.name
                for h in df["sha256"]:
                    self._index[h] = name
                    n += 1
            if n:
                logger.info(
                    f"Loaded {n:,} image hashes from {len(shards)} shard(s) "
                    f"({len(self._index):,} distinct) — ~{len(self._index) * 110 / 1e9:.2f} GB RSS"
                )

        last = shards[-1]
        try:
            self._shard_no = int(last.stem.rsplit("_", 1)[-1])
        except ValueError:
            self._shard_no = len(shards) - 1

        # Reopen the last shard for appending if it never reached the cap.
        try:
            df = pd.read_parquet(last)
        except Exception as exc:
            logger.warning(f"Could not reopen {last.name} for appending: {exc}")
            self._shard_no += 1
            return
        if len(df) < self._shard_rows:
            self._buf = df.to_dict("records")
            logger.info(f"Reopened {last.name} for appending ({len(df):,}/{self._shard_rows:,} rows)")
        else:
            self._shard_no += 1

    # ── Writing ───────────────────────────────────────────────────────────────

    @property
    def _current_shard_name(self) -> str:
        return f"shard_{self._shard_no:06d}.parquet"

    def put(self, data: bytes, content_type: str, width: int, height: int) -> tuple[str, str, bool]:
        """
        Store `data`, returning (sha256, shard_name, was_duplicate).
        A duplicate is not re-stored; the caller still records the mint->sha256
        edge in the ledger, so N mints can point at one blob.
        """
        h = sha256_hex(data)
        with self._lock:
            if self._dedup and h in self._index:
                return h, self._index[h], True

            self._buf.append({
                "sha256": h,
                "bytes": data,
                "content_type": content_type,
                "width": int(width),
                "height": int(height),
            })
            shard_name = self._current_shard_name
            if self._dedup:
                self._index[h] = shard_name

            if len(self._buf) >= self._shard_rows:
                self._write_locked(seal=True)
            return h, shard_name, False

    def _write_locked(self, seal: bool) -> int:
        """Write the buffer to the current shard. Caller must hold the lock."""
        if not self._buf:
            return 0
        path = self._dir / self._current_shard_name
        tmp = path.with_suffix(".parquet.tmp")
        df = pd.DataFrame(self._buf)
        # zstd on top of already-compressed WebP buys little; keep it cheap.
        df.to_parquet(tmp, index=False, compression="snappy")
        tmp.replace(path)
        n = len(df)
        if seal:
            logger.info(f"Sealed {path.name} ({n:,} images, {path.stat().st_size / 1e6:.0f} MB)")
            self._buf = []
            self._shard_no += 1
        return n

    def flush(self) -> int:
        """Persist the open shard without sealing it. Safe to call often."""
        with self._lock:
            return self._write_locked(seal=False)

    # ── Stats ─────────────────────────────────────────────────────────────────

    def stats(self) -> dict:
        with self._lock:
            return {
                "distinct_blobs": len(self._index),
                "buffered": len(self._buf),
                "open_shard": self._current_shard_name,
                "shards_on_disk": len(list(self._dir.glob(SHARD_GLOB))),
            }
