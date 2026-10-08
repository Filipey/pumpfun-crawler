"""
collectors/day_store.py
-----------------------
Thread-safe, day-partitioned Parquet writer for the normalized per-entity
tables (tokens, pools, graduations, bonding_curve_trades, pumpswap_trades).

Replaces the old single-flat-table DayStore. One Parquet file per
(table, day):
    data/parquet/tokens/tokens_2025-11-01.parquet
    data/parquet/bonding_curve_trades/bonding_curve_trades_2025-11-01.parquet
    ...

Design notes
------------
- `add()` takes a raw decoder.py output record (plus injected "date" and
  "source_phase") and routes it to the right entity table, reshaping it to
  that table's column set.
- Memory-bounded, append-only flushing: `flush_batch()` writes whatever is
  currently buffered for a date straight to a new numbered part file
  (`table_date.partNNNNNN.parquet`) with NO read-merge-rewrite of existing
  data. Callers are expected to call this periodically (e.g. every N shards)
  during a long ingestion run so memory usage stays bounded by "how much is
  buffered since the last flush", not "the whole day". A prior version
  buffered an entire day (hundreds of thousands of rows across ~300 shards)
  before writing anything, which pushed the host into a memory-pressure crash.
- `compact_day()` does the actual dedup pass: merges all part files (+ any
  previously compacted file) for a date, drops duplicates by the table's
  dedup key from schemas.ENTITY_TABLES, and atomically replaces the canonical
  `table_date.parquet`. Call this once after a day's ingestion finishes (or
  again later — it's idempotent and safe to rerun on leftover part files from
  an interrupted run).
- PumpSwap trades often carry mint=None (the on-chain event only has `pool`);
  a pool->mint index is built from `pool_create` events as they're seen and
  also loaded from any already-written `pools` Parquet files (compacted or
  part files) on startup, so a trade against a pool created on an earlier day
  still gets its mint filled in.
"""

import logging
import threading
from collections import defaultdict
from pathlib import Path

import pandas as pd

from config import PARQUET_DIR
from schemas import ENTITY_TABLES, apply_dtypes, sanitize_text

logger = logging.getLogger(__name__)


class EntityStore:
    def __init__(self, parquet_dir: Path = PARQUET_DIR):
        self._dir = parquet_dir
        self._lock = threading.Lock()
        # table -> date (str) -> list of row dicts
        self._buffers: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
        # pool address -> mint, for enriching pumpswap_trades rows missing `mint`
        self._pool_mint: dict[str, str] = {}
        self._load_pool_mint_index()

    # ── Startup ───────────────────────────────────────────────────────────────

    def _load_pool_mint_index(self) -> None:
        table_dir = self._dir / "pools"
        if not table_dir.exists():
            return
        n = 0
        for f in sorted(table_dir.glob("pools_*.parquet")):
            try:
                df = pd.read_parquet(f, columns=["pool", "mint"])
            except Exception as exc:
                logger.warning(f"Could not read {f} for pool->mint index: {exc}")
                continue
            for pool, mint in zip(df["pool"], df["mint"]):
                if pool and mint:
                    self._pool_mint[pool] = mint
                    n += 1
        if n:
            logger.info(f"Loaded {n:,} pool->mint mappings from existing pools/ files")

    def _next_part_number(self, table_dir: Path, table: str, date: str) -> int:
        existing = table_dir.glob(f"{table}_{date}.part*.parquet")
        nums = []
        for p in existing:
            try:
                nums.append(int(p.stem.rsplit(".part", 1)[-1]))
            except ValueError:
                continue
        return (max(nums) + 1) if nums else 0

    # ── Routing ───────────────────────────────────────────────────────────────

    @staticmethod
    def _route(record: dict) -> tuple[str | None, dict]:
        """Map a decoder.py output record to (table_name, row_dict)."""
        tx_type = record.get("tx_type")
        source = record.get("source")

        if tx_type == "create":
            return "tokens", {
                "mint":              record.get("mint"),
                "coin_creator":      record.get("coin_creator"),
                "bonding_curve":     record.get("bonding_curve"),
                "name":              record.get("_name"),
                "symbol":            record.get("_symbol"),
                "uri":               record.get("_uri"),
                "created_slot":      record.get("slot"),
                "created_at":        record.get("timestamp"),
                "created_signature": record.get("signature"),
            }

        if tx_type == "complete":
            return "graduations", {
                "mint":       record.get("mint"),
                "signature":  record.get("signature"),
                "slot":       record.get("slot"),
                "block_time": record.get("timestamp"),
            }

        if tx_type == "pool_create":
            return "pools", {
                "pool":              record.get("pool"),
                "mint":              record.get("mint"),
                "creator":           record.get("coin_creator"),
                "created_slot":      record.get("slot"),
                "created_at":        record.get("timestamp"),
                "created_signature": record.get("signature"),
            }

        if tx_type in ("buy", "sell") and source == "pumpfun":
            return "bonding_curve_trades", {
                "signature":              record.get("signature"),
                "slot":                   record.get("slot"),
                "block_time":             record.get("timestamp"),
                "mint":                   record.get("mint"),
                "trader":                 record.get("trader"),
                "tx_type":                tx_type,
                "is_buy":                 record.get("is_buy"),
                "v_sol_in_bonding_curve": record.get("v_sol_in_bonding_curve"),
                "v_tok_in_bonding_curve": record.get("v_tok_in_bonding_curve"),
                "sol_amount":             record.get("sol_amount"),
                "token_amount":           record.get("token_amount"),
                "is_bot":                 record.get("is_bot"),
                "coin_creator":           record.get("coin_creator"),
                "coin_creator_fee":       record.get("coin_creator_fee"),
                "source_phase":           record.get("source_phase"),
            }

        if tx_type in ("buy", "sell") and source == "pumpswap":
            return "pumpswap_trades", {
                "signature":              record.get("signature"),
                "slot":                   record.get("slot"),
                "block_time":             record.get("timestamp"),
                "mint":                   record.get("mint"),
                "pool":                   record.get("pool"),
                "trader":                 record.get("trader"),
                "tx_type":                tx_type,
                "is_buy":                 record.get("is_buy"),
                "v_sol_in_bonding_curve": record.get("v_sol_in_bonding_curve"),
                "v_tok_in_bonding_curve": record.get("v_tok_in_bonding_curve"),
                "sol_amount":             record.get("sol_amount"),
                "token_amount":           record.get("token_amount"),
                "lp_fee":                 record.get("lp_fee"),
                "protocol_fee":           record.get("protocol_fee"),
                "coin_creator":           record.get("coin_creator"),
                "coin_creator_fee":       record.get("coin_creator_fee"),
                "is_bot":                 record.get("is_bot"),
                "source_phase":           record.get("source_phase"),
            }

        return None, {}

    # ── Adding records ────────────────────────────────────────────────────────

    def add(self, date: str, record: dict) -> None:
        table, row = self._route(record)
        if table is None:
            return

        with self._lock:
            if table == "pools" and row.get("pool") and row.get("mint"):
                self._pool_mint[row["pool"]] = row["mint"]

            if table == "pumpswap_trades" and not row.get("mint"):
                row["mint"] = self._pool_mint.get(row.get("pool"))

            self._buffers[table][date].append(row)

    def add_many(self, date: str, records: list[dict]) -> None:
        for rec in records:
            self.add(date, rec)

    def add_row(self, table: str, date: str, row: dict) -> None:
        """
        Buffer a pre-shaped row directly into `table`, bypassing _route().

        Phase C rows (token_metadata, token_images, token_pumpfun_api) are
        produced by collect_metadata.py from HTTP responses, not by decoder.py
        from chain events, so they have no `tx_type` for _route() to dispatch
        on. Routing them would mean inventing fake tx_type values purely to
        satisfy a dispatcher that exists for on-chain records.
        """
        if table not in ENTITY_TABLES:
            raise KeyError(f"Unknown entity table: {table}")
        with self._lock:
            self._buffers[table][date].append(row)

    def add_rows(self, table: str, date: str, rows: list[dict]) -> None:
        if table not in ENTITY_TABLES:
            raise KeyError(f"Unknown entity table: {table}")
        if not rows:
            return
        with self._lock:
            self._buffers[table][date].extend(rows)

    # ── Flushing ──────────────────────────────────────────────────────────────

    def flush_batch(self, date: str) -> dict[str, int]:
        """
        Write whatever is currently buffered for `date` to new part files, one
        per table, and clear the buffer. No read-merge-rewrite — O(batch size),
        not O(day size). Call this periodically during ingestion (e.g. every
        N shards/chunks) to keep memory bounded; call compact_day() once the
        date is fully ingested to dedup part files into the canonical file.
        """
        written: dict[str, int] = {}
        with self._lock:
            tables_touched = [t for t, by_date in self._buffers.items() if by_date.get(date)]
            pending = {t: self._buffers[t].pop(date, []) for t in tables_touched}

        for table, buf in pending.items():
            if not buf:
                continue
            dtypes, _dedup_key = ENTITY_TABLES[table]
            table_dir = self._dir / table
            table_dir.mkdir(parents=True, exist_ok=True)

            part_no = self._next_part_number(table_dir, table, date)
            path = table_dir / f"{table}_{date}.part{part_no:06d}.parquet"

            try:
                try:
                    df = apply_dtypes(pd.DataFrame(buf), dtypes)
                    df.to_parquet(path, index=False, compression="zstd")
                except UnicodeEncodeError:
                    # A lone UTF-16 surrogate somewhere in the batch. pandas
                    # converts object columns to Arrow strings while building
                    # the DataFrame, so this fires before apply_dtypes gets a
                    # chance to coerce anything — which is why it is handled
                    # here rather than only in the coercers. Retrying after a
                    # sanitising pass costs nothing on clean batches, which
                    # is nearly all of them.
                    n_fixed = sum(
                        1 for row in buf for v in row.values()
                        if isinstance(v, str) and v != sanitize_text(v)
                    )
                    cleaned = [
                        {k: (sanitize_text(v) if isinstance(v, str) else v)
                         for k, v in row.items()}
                        for row in buf
                    ]
                    df = apply_dtypes(pd.DataFrame(cleaned), dtypes)
                    df.to_parquet(path, index=False, compression="zstd")
                    logger.warning(
                        f"{table}/{path.name}: {n_fixed} string value(s) held unpaired "
                        f"UTF-16 surrogates (truncated emoji in off-chain text); "
                        f"replaced with U+FFFD and written"
                    )
            except Exception:
                # The buffer was popped before the write, so a failure here
                # used to destroy the batch: rows gone from memory, nothing on
                # disk, and the traceback naming only the column. Put them
                # back so a later flush (or the caller's error handling) still
                # has them, then let the caller see the failure.
                with self._lock:
                    self._buffers[table][date] = buf + self._buffers[table].get(date, [])
                path.unlink(missing_ok=True)
                logger.error(
                    f"Failed to write {table}/{path.name} ({len(buf):,} rows) — "
                    f"rows restored to the buffer, not lost",
                    exc_info=True,
                )
                raise

            written[table] = len(df)
            logger.debug(f"Flushed batch -> {table}/{path.name} ({len(df):,} rows)")

        return written

    def flush_all(self) -> None:
        with self._lock:
            pending_dates = sorted({d for by_date in self._buffers.values() if any(by_date.values()) for d in by_date})
        for date in pending_dates:
            self.flush_batch(date)

    def compact_day(self, date: str, tables: list[str] | None = None) -> dict[str, int]:
        """
        Merge every part file (+ any previously compacted file) for `date`
        into a single deduplicated canonical Parquet file per table, then
        remove the part files. Idempotent — safe to call again on leftovers
        from an interrupted run.

        `tables` restricts the pass to specific tables; the default is every
        table. Restricting it is what makes concurrent stages safe. Compaction
        reads the part files it can see, writes the canonical file, then
        deletes everything matching the part glob — so a process compacting a
        table another process is still writing will delete part files that
        appeared after the read, losing those rows. Each Phase C stage
        therefore compacts only the table it owns.
        """
        compacted: dict[str, int] = {}
        selected = ENTITY_TABLES if tables is None else {
            t: ENTITY_TABLES[t] for t in tables if t in ENTITY_TABLES
        }
        for table, (dtypes, dedup_key) in selected.items():
            table_dir = self._dir / table
            if not table_dir.exists():
                continue

            final_path = table_dir / f"{table}_{date}.parquet"
            part_paths = sorted(table_dir.glob(f"{table}_{date}.part*.parquet"))
            if not part_paths and not final_path.exists():
                continue

            frames = []
            if final_path.exists():
                try:
                    frames.append(pd.read_parquet(final_path))
                except Exception as exc:
                    logger.warning(f"Could not read existing {final_path}: {exc}")
            for p in part_paths:
                try:
                    frames.append(pd.read_parquet(p))
                except Exception as exc:
                    logger.warning(f"Could not read part file {p}: {exc}")

            if not frames:
                continue

            df = pd.concat(frames, ignore_index=True)
            del frames
            df = apply_dtypes(df, dtypes)
            df = df.drop_duplicates(subset=dedup_key, keep="last")

            tmp_path = table_dir / f"{table}_{date}.parquet.tmp"
            df.to_parquet(tmp_path, index=False, compression="zstd")
            tmp_path.replace(final_path)
            compacted[table] = len(df)
            logger.info(
                f"Compacted {table}/{final_path.name}: {len(df):,} rows "
                f"({len(part_paths)} part file(s) merged)"
            )

            for p in part_paths:
                p.unlink(missing_ok=True)

        return compacted

    # ── Stats ─────────────────────────────────────────────────────────────────

    def buffered_rows(self) -> dict[str, dict[str, int]]:
        with self._lock:
            return {
                table: {d: len(rows) for d, rows in by_date.items()}
                for table, by_date in self._buffers.items()
            }

    def buffered_dates(self) -> set[str]:
        with self._lock:
            return {d for by_date in self._buffers.values() for d in by_date}
