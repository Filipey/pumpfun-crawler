"""
collect_metadata.py
-------------------
Phase C CLI: resolve every token's off-chain metadata JSON, download its
image, and fetch pump.fun's moderation flags — the inputs the multimodal
taxonomy classifier needs.

Three independent stages, each checkpointed per day against the same
`tokens_YYYY-MM-DD.parquet` partitions Phase A/B produced:

    metadata   tokens.uri -> metadata JSON  -> token_metadata
    images     token_metadata.image_url     -> token_images + image blob shards
    api        pump.fun frontend API        -> token_pumpfun_api

Usage
-----
    python collect_metadata.py --status
    python collect_metadata.py --stage metadata --limit 10000     # pilot first
    python collect_metadata.py --stage metadata                   # full range
    python collect_metadata.py --stage images --limit 10000
    python collect_metadata.py --stage images
    python collect_metadata.py --stage api
    python collect_metadata.py --stage all

    python collect_metadata.py --stage images --image-strategy transform
    python collect_metadata.py --stage metadata --retry-failed
    python collect_metadata.py --stage images --reset 2025-11-01

Run `--stage metadata` to completion before `--stage images`: it is the cheap
half (~45 req/s vs ~27 tok/s), it produces the description/twitter text
features days earlier, and it tells you exactly how many images actually exist
before you commit disk to them.

Safe to interrupt (Ctrl-C) and rerun — checkpointed per day, flushed in
bounded batches, and mints already carrying a terminal status are skipped.
"""

import argparse
import datetime
import logging
import logging.handlers
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from collectors.blob_store import ImageBlobStore
from collectors.checkpoint import Checkpoint
from collectors.day_store import EntityStore
from collectors.metadata_source import (
    fetch_image,
    fetch_metadata,
    fetch_pump_api,
    make_client,
)
from schemas import ENTITY_TABLES
from config import (
    API_WORKERS,
    DATA_DIR,
    FLUSH_EVERY_MINTS,
    IMAGE_STRATEGY,
    IMAGE_WORKERS,
    IMAGES_DIR,
    METADATA_WORKERS,
    PARQUET_DIR,
    SOLARCHIVE_START,
    OLDFAITHFUL_END,
)

STAGES = ("metadata", "images", "api")

# Statuses that mean "we got a real answer, don't ask again". Everything else
# is transient and gets picked back up by --retry-failed.
TERMINAL_STATUS = {
    "metadata": {"ok", "no_uri", "invalid_json", "unsupported_uri"},
    "images":   {"ok", "no_url", "decode_error", "unsupported", "too_large"},
    "api":      {"ok", "not_found"},
}

STAGE_TABLE = {
    "metadata": "token_metadata",
    "images":   "token_images",
    "api":      "token_pumpfun_api",
}

# A day whose success ratio collapses is a gateway outage, not a day of bad
# tokens. Phase A learned this the hard way (see MIN_PLAUSIBLE_DAY_EVENTS in
# collect_solarchive.py): a silently-failing source that still marks days
# "done" permanently hides the gap. Refuse to finalize such a day.
MIN_SUCCESS_RATIO = 0.50

_LOG_FMT = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def _setup_logging() -> None:
    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "metadata.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(_LOG_FMT, _DATE_FMT))

    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=10 * 1024 * 1024, backupCount=10, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(_LOG_FMT, _DATE_FMT))

    root.addHandler(console)
    root.addHandler(file_handler)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("collect_metadata").info(f"Log file: {log_path}")


logger = logging.getLogger("collect_metadata")


def _dates_in_range(start: str, end: str) -> list[str]:
    d = datetime.date.fromisoformat(start)
    e = datetime.date.fromisoformat(end)
    days = []
    while d <= e:
        days.append(d.isoformat())
        d += datetime.timedelta(days=1)
    return days


def _available_days() -> list[str]:
    """Days that actually have a compacted tokens file to work from."""
    table_dir = PARQUET_DIR / "tokens"
    if not table_dir.exists():
        return []
    days = []
    for p in sorted(table_dir.glob("tokens_????-??-??.parquet")):
        days.append(p.stem.rsplit("_", 1)[-1])
    return days


# ── Reading prior state ───────────────────────────────────────────────────────

def _error_row(table: str, mint: str, **extra) -> dict:
    """
    An all-NULL row for `table` carrying a non-terminal status.

    Any unexpected exception in a worker becomes one of these instead of
    propagating out of ThreadPoolExecutor.map and aborting the day. At ~9M
    rows something will always throw — a malformed URI, a decoder edge case,
    a truncated response — and losing a whole day's progress to one bad row
    is far worse than recording it as failed and moving on. "crashed" is
    deliberately not in TERMINAL_STATUS, so --retry-failed picks these up.
    """
    dtypes, _ = ENTITY_TABLES[table]
    row = {col: None for col in dtypes}
    row["mint"] = mint
    row["status"] = "crashed"
    row.update(extra)
    return row


def _s(v) -> str | None:
    """Parquet/pandas NaN -> None, so string columns stay strings or nothing."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return v if isinstance(v, str) else None


def _read_table_day(table: str, date: str, columns: list[str] | None) -> pd.DataFrame | None:
    """Read a day's canonical file for `table`, plus any uncompacted part files."""
    table_dir = PARQUET_DIR / table
    if not table_dir.exists():
        return None
    paths = list(table_dir.glob(f"{table}_{date}.parquet"))
    paths += sorted(table_dir.glob(f"{table}_{date}.part*.parquet"))
    frames = []
    for p in paths:
        try:
            frames.append(pd.read_parquet(p, columns=columns))
        except Exception as exc:
            logger.warning(f"Could not read {p.name}: {exc}")
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def _done_mints(stage: str, date: str, retry_failed: bool) -> set[str]:
    """Mints to skip for this (stage, date)."""
    df = _read_table_day(STAGE_TABLE[stage], date, ["mint", "status"])
    if df is None or df.empty:
        return set()
    if retry_failed:
        df = df[df["status"].isin(TERMINAL_STATUS[stage])]
    return set(df["mint"].dropna())


# ── Stage: metadata ───────────────────────────────────────────────────────────

def _stage_metadata(date: str, store: EntityStore, workers: int,
                    limit: int | None, retry_failed: bool, dry_run: bool) -> tuple[int, int]:
    df = _read_table_day("tokens", date, ["mint", "uri"])
    if df is None or df.empty:
        logger.warning(f"[{date}] no tokens file — nothing to resolve")
        return 0, 0

    skip = _done_mints("metadata", date, retry_failed)
    work = [(m, _s(u)) for m, u in zip(df["mint"], df["uri"]) if m and m not in skip]
    if limit:
        work = work[:limit]
    if not work:
        logger.info(f"[{date}] metadata: all {len(df):,} token(s) already resolved")
        return 0, 0

    logger.info(f"[{date}] metadata: {len(work):,} to resolve ({len(skip):,} already done)")
    client = make_client(workers)
    done = ok = 0
    lock = threading.Lock()
    t0 = time.time()

    def one(item):
        nonlocal done, ok
        mint, uri = item
        try:
            row = fetch_metadata(client, mint, uri)
        except Exception:
            logger.exception(f"[{date}] metadata worker crashed on {mint} (uri={uri!r})")
            row = _error_row("token_metadata", mint, uri=uri, resolved_at=int(time.time()))
        with lock:
            if not dry_run:
                store.add_row("token_metadata", date, row)
            done += 1
            if row["status"] == "ok":
                ok += 1
            if done % FLUSH_EVERY_MINTS == 0:
                if not dry_run:
                    store.flush_batch(date)
                rate = done / max(time.time() - t0, 1e-6)
                logger.info(f"[{date}] metadata {done:,}/{len(work):,} "
                            f"({ok / done:.1%} ok, {rate:.0f}/s)")

    try:
        with ThreadPoolExecutor(workers) as ex:
            list(ex.map(one, work))
    finally:
        client.close()
        if not dry_run:
            store.flush_batch(date)

    el = time.time() - t0
    logger.info(f"[{date}] metadata done: {ok:,}/{done:,} ok ({ok / max(done,1):.1%}) "
                f"in {el:.0f}s ({done / max(el,1e-6):.0f}/s)")
    return done, ok


# ── Stage: images ─────────────────────────────────────────────────────────────

# Carried from the first mint that fetched a CID to every later mint that
# reuses it. Everything here is a property of the *image*, not of the token,
# so a deduped row is complete rather than a row of NULLs — at ~17% reuse
# that would otherwise punch a sizeable hole in the original-dimension data.
_CID_CARRY = ("sha256", "shard", "orig_bytes", "orig_width", "orig_height",
              "orig_format", "is_animated", "n_frames", "stored_bytes",
              "stored_width", "stored_height")


def _load_cid_index(dedup: bool) -> dict[str, dict]:
    """
    image_cid -> the image-level facts of an already-stored blob.

    This is the bandwidth-saving half of dedup: a CID we've already stored
    never needs downloading again, no matter which mint references it. ~17% of
    a sampled 100 reused artwork, and reuse should be higher at scale.
    Costs roughly 1-2 GB of RSS at ~9.3M rows — `--no-dedup` skips it.
    """
    if not dedup:
        return {}
    table_dir = PARQUET_DIR / "token_images"
    if not table_dir.exists():
        return {}
    idx: dict[str, dict] = {}
    for p in sorted(table_dir.glob("token_images_*.parquet")):
        try:
            df = pd.read_parquet(p, columns=["image_cid", "status", *_CID_CARRY])
        except Exception as exc:
            logger.warning(f"Could not read {p.name} for CID index: {exc}")
            continue
        df = df[(df["status"] == "ok") & df["image_cid"].notna() & df["sha256"].notna()]
        for rec in df.to_dict("records"):
            cid = rec.pop("image_cid")
            rec.pop("status", None)
            idx[cid] = rec
    if idx:
        logger.info(f"Loaded {len(idx):,} CID->blob mappings (pre-download dedup)")
    return idx


def _stage_images(date: str, store: EntityStore, blobs: ImageBlobStore, workers: int,
                  limit: int | None, retry_failed: bool, dry_run: bool,
                  strategy: str, cid_index: dict) -> tuple[int, int]:
    meta = _read_table_day("token_metadata", date, ["mint", "image_url", "image_cid", "status"])
    if meta is None or meta.empty:
        logger.warning(f"[{date}] no token_metadata — run `--stage metadata` for this day first")
        return 0, 0
    meta = meta[meta["status"] == "ok"]

    skip = _done_mints("images", date, retry_failed)
    # Parquet round-trips a None in an object column back as NaN (a float), so
    # normalize before anything tries to concatenate a CID onto a gateway URL.
    work = [(m, _s(u), _s(c)) for m, u, c in
            zip(meta["mint"], meta["image_url"], meta["image_cid"])
            if m and m not in skip]
    if limit:
        work = work[:limit]
    if not work:
        logger.info(f"[{date}] images: nothing to fetch ({len(skip):,} already done)")
        return 0, 0

    logger.info(f"[{date}] images: {len(work):,} to fetch ({len(skip):,} already done, "
                f"strategy={strategy})")
    client = make_client(workers)
    done = ok = reused = 0
    lock = threading.Lock()
    t0 = time.time()

    def one(item):
        nonlocal done, ok, reused
        mint, image_url, cid = item

        # Already have this exact artwork: inherit the image-level facts and
        # skip the download entirely.
        if cid and cid in cid_index:
            row = {
                "mint": mint, "image_url": image_url, "image_cid": cid,
                "status": "ok", "http_status": None, "strategy": "cid_dedup",
                "fetched_at": int(time.time()),
                **cid_index[cid],
            }
            with lock:
                if not dry_run:
                    store.add_row("token_images", date, row)
                done += 1; ok += 1; reused += 1
            return

        try:
            row, data, ctype = fetch_image(client, mint, image_url, cid, strategy=strategy)
            if data is not None and not dry_run:
                sha, shard, was_dupe = blobs.put(
                    data, ctype or "image/webp",
                    row.get("stored_width") or 0, row.get("stored_height") or 0,
                )
                row["sha256"] = sha
                row["shard"] = shard
                if cid:
                    cid_index[cid] = {k: row.get(k) for k in _CID_CARRY}
                if was_dupe:
                    with lock:
                        reused += 1
        except Exception:
            logger.exception(f"[{date}] image worker crashed on {mint} (url={image_url!r})")
            row = _error_row("token_images", mint, image_url=image_url,
                             image_cid=cid, fetched_at=int(time.time()))
        with lock:
            if not dry_run:
                store.add_row("token_images", date, row)
            done += 1
            if row["status"] == "ok":
                ok += 1
            if done % FLUSH_EVERY_MINTS == 0:
                if not dry_run:
                    store.flush_batch(date)
                    blobs.flush()
                rate = done / max(time.time() - t0, 1e-6)
                logger.info(f"[{date}] images {done:,}/{len(work):,} "
                            f"({ok / done:.1%} ok, {reused:,} dedup, {rate:.1f}/s)")

    try:
        with ThreadPoolExecutor(workers) as ex:
            list(ex.map(one, work))
    finally:
        client.close()
        if not dry_run:
            store.flush_batch(date)
            blobs.flush()

    el = time.time() - t0
    logger.info(f"[{date}] images done: {ok:,}/{done:,} ok ({ok / max(done,1):.1%}), "
                f"{reused:,} deduped, in {el:.0f}s ({done / max(el,1e-6):.1f}/s)")
    return done, ok


# ── Stage: api ────────────────────────────────────────────────────────────────

def _stage_api(date: str, store: EntityStore, workers: int,
               limit: int | None, retry_failed: bool, dry_run: bool) -> tuple[int, int]:
    df = _read_table_day("tokens", date, ["mint"])
    if df is None or df.empty:
        logger.warning(f"[{date}] no tokens file — nothing to enrich")
        return 0, 0

    skip = _done_mints("api", date, retry_failed)
    work = [m for m in df["mint"] if m and m not in skip]
    if limit:
        work = work[:limit]
    if not work:
        logger.info(f"[{date}] api: all {len(df):,} token(s) already enriched")
        return 0, 0

    logger.info(f"[{date}] api: {len(work):,} to enrich ({len(skip):,} already done)")
    client = make_client(workers)
    done = ok = nsfw = banned = 0
    lock = threading.Lock()
    t0 = time.time()

    def one(mint):
        nonlocal done, ok, nsfw, banned
        try:
            row = fetch_pump_api(client, mint)
        except Exception:
            logger.exception(f"[{date}] api worker crashed on {mint}")
            row = _error_row("token_pumpfun_api", mint, fetched_at=int(time.time()))
        with lock:
            if not dry_run:
                store.add_row("token_pumpfun_api", date, row)
            done += 1
            if row["status"] == "ok":
                ok += 1
                nsfw += bool(row.get("nsfw"))
                banned += bool(row.get("is_banned"))
            if done % FLUSH_EVERY_MINTS == 0:
                if not dry_run:
                    store.flush_batch(date)
                rate = done / max(time.time() - t0, 1e-6)
                logger.info(f"[{date}] api {done:,}/{len(work):,} ({rate:.0f}/s)")

    try:
        with ThreadPoolExecutor(workers) as ex:
            list(ex.map(one, work))
    finally:
        client.close()
        if not dry_run:
            store.flush_batch(date)

    el = time.time() - t0
    # nsfw looked near-zero in sampling; surface the rate so a useless label
    # is noticed at pilot time rather than after the full run.
    logger.info(f"[{date}] api done: {ok:,}/{done:,} ok in {el:.0f}s "
                f"({done / max(el,1e-6):.0f}/s) — nsfw={nsfw:,} ({nsfw / max(ok,1):.3%}), "
                f"is_banned={banned:,} ({banned / max(ok,1):.3%})")
    return done, ok


# ── Driver ────────────────────────────────────────────────────────────────────

def collect(stage: str, start: str, end: str, workers: int | None,
            single_day: str | None, limit: int | None, retry_failed: bool,
            dry_run: bool, strategy: str, dedup: bool, force_accept: bool) -> None:
    if single_day:
        start = end = single_day

    checkpoint = Checkpoint()
    store = EntityStore()

    have = set(_available_days())
    requested = _dates_in_range(start, end)
    all_days = [d for d in requested if d in have]
    missing = len(requested) - len(all_days)
    if missing:
        logger.info(f"{missing} requested day(s) have no tokens_*.parquet yet — skipped")
    if not all_days:
        logger.error(f"No tokens_*.parquet found in {PARQUET_DIR / 'tokens'} for {start}..{end}. "
                     f"Run Phase A/B first.")
        return

    todo = all_days if (limit or retry_failed) else checkpoint.pending_chunks(stage, all_days)
    logger.info(f"Checkpoint: {checkpoint.summary(stage)}")
    if not todo:
        logger.info(f"Stage '{stage}': all requested days already done. Nothing to do.")
        return
    logger.info(f"Stage '{stage}': {len(todo)}/{len(all_days)} day(s) pending: {todo[0]}..{todo[-1]}")

    blobs = None
    cid_index: dict = {}
    if stage == "images":
        blobs = ImageBlobStore(dedup=dedup)
        cid_index = _load_cid_index(dedup)
        logger.info(f"Image blobs -> {IMAGES_DIR} ({blobs.stats()})")

    if workers is None:
        workers = {"metadata": METADATA_WORKERS, "images": IMAGE_WORKERS, "api": API_WORKERS}[stage]

    try:
        for date in todo:
            checkpoint.mark(stage, date, "in_progress")
            logger.info(f"=== [{date}] {stage} starting ===")

            if stage == "metadata":
                done, ok = _stage_metadata(date, store, workers, limit, retry_failed, dry_run)
            elif stage == "images":
                done, ok = _stage_images(date, store, blobs, workers, limit,
                                         retry_failed, dry_run, strategy, cid_index)
            else:
                done, ok = _stage_api(date, store, workers, limit, retry_failed, dry_run)

            if dry_run:
                logger.info(f"=== [{date}] dry run — nothing written, checkpoint untouched ===")
                checkpoint.reset(stage, date)
                continue

            compacted = store.compact_day(date)

            if limit:
                # A capped run only touched the first N mints of the day, so
                # the day is not complete. Leaving it 'in_progress' keeps it
                # pending for a real run — marking it done would silently
                # freeze the day at one pilot's worth of rows.
                logger.info(f"=== [{date}] pilot (--limit {limit:,}) — NOT marked done, "
                            f"still pending for a full run ===")
                continue

            ratio = ok / done if done else 1.0
            if done and ratio < MIN_SUCCESS_RATIO and not force_accept:
                checkpoint.mark(stage, date, "partial", attempted=done, ok=ok, ratio=round(ratio, 4))
                logger.error(
                    f"=== [{date}] SUSPICIOUSLY LOW — only {ok:,}/{done:,} ({ratio:.1%}) "
                    f"succeeded (expected >={MIN_SUCCESS_RATIO:.0%}). NOT marking done. This "
                    f"is almost always a gateway outage or a rate limit mid-run, not a day of "
                    f"bad tokens — check the status histogram in the log above, then rerun the "
                    f"day with --retry-failed. Pass --force-accept if it's genuinely correct. ==="
                )
            else:
                checkpoint.mark(stage, date, "done", attempted=done, ok=ok, rows=compacted)
                logger.info(f"=== [{date}] {stage} DONE — {ok:,}/{done:,} ok, rows={compacted} ===")

    except KeyboardInterrupt:
        logger.info("Interrupted — flushing buffered data …")
        store.flush_all()
        if blobs:
            blobs.flush()
        logger.info("Flushed. Interrupted days stay 'in_progress' — rerun to resume.")
    except Exception:
        logger.error("Fatal error", exc_info=True)
        store.flush_all()
        if blobs:
            blobs.flush()
        raise


def status(start: str, end: str) -> None:
    checkpoint = Checkpoint()
    have = _available_days()
    window = [d for d in _dates_in_range(start, end) if d in have]
    print(f"\ntokens_*.parquet days available: {len(have)}"
          + (f"  ({have[0]} .. {have[-1]})" if have else ""))
    print(f"Window {start} .. {end}: {len(window)} day(s) with data")
    print(f"Image blobs dir: {IMAGES_DIR}")
    for stage in STAGES:
        pending = checkpoint.pending_chunks(stage, window)
        print(f"\n  [{stage}] {checkpoint.summary(stage)}")
        print(f"      pending: {len(pending)}/{len(window)}"
              + (f"   first: {pending[0]}" if pending else ""))
    print()


def reset_days(stage: str, days: list[str]) -> None:
    checkpoint = Checkpoint()
    for date in days:
        before = checkpoint.status(stage, date)
        checkpoint.reset(stage, date)
        logger.info(f"Reset {stage}/{date}: {before} -> pending")


def main() -> None:
    _setup_logging()

    p = argparse.ArgumentParser(
        description="Phase C: off-chain metadata, images and pump.fun moderation flags.")
    p.add_argument("--stage", choices=(*STAGES, "all"), default="metadata",
                   help="Which stage to run (default: metadata). 'all' runs metadata, images, api in order.")
    p.add_argument("--status", action="store_true", help="Print checkpoint summary and exit.")
    p.add_argument("--reset", metavar="YYYY-MM-DD", nargs="+", default=None,
                   help="Revert the given day(s) of --stage to pending, then exit.")
    # Default the window to whatever tokens_*.parquet actually exists rather
    # than to the Phase A/B constants: if collection ran past OLDFAITHFUL_END,
    # a constant default would silently skip those days instead of failing.
    have = _available_days()
    default_start = have[0] if have else SOLARCHIVE_START
    default_end = have[-1] if have else OLDFAITHFUL_END

    p.add_argument("--day", metavar="YYYY-MM-DD", help="Process a single day only.")
    p.add_argument("--start", default=default_start,
                   help=f"Start date (default: earliest day with a tokens file — currently {default_start})")
    p.add_argument("--end", default=default_end,
                   help=f"End date (default: latest day with a tokens file — currently {default_end})")
    p.add_argument("--workers", type=int, default=None,
                   help="Concurrent fetches (default: per-stage value from config/env).")
    p.add_argument("--limit", type=int, default=None,
                   help="Pilot mode: process at most N mints per day. Days are NOT marked done.")
    p.add_argument("--dry-run", action="store_true",
                   help="Fetch but write nothing (measure throughput/success without touching disk).")
    p.add_argument("--retry-failed", action="store_true",
                   help="Re-drive mints whose status was transient (http_error/timeout), skipping terminal ones.")
    p.add_argument("--image-strategy", choices=("original", "transform"), default=IMAGE_STRATEGY,
                   help=f"Image fetch path (default: {IMAGE_STRATEGY}). 'transform' is ~10x less "
                        f"download for ~1.5x the wall clock — use on a metered connection.")
    p.add_argument("--no-dedup", action="store_true",
                   help="Disable content/CID dedup indexes (saves ~1-2GB RSS at full scale, re-stores duplicate blobs).")
    p.add_argument("--force-accept", action="store_true",
                   # argparse runs help through %-formatting, so a literal % must be doubled.
                   help=f"Mark a day done even if under {MIN_SUCCESS_RATIO:.0%} success.".replace("%", "%%"))

    args = p.parse_args()

    if args.reset:
        if args.stage == "all":
            p.error("--reset needs a concrete --stage, not 'all'")
        reset_days(args.stage, args.reset)
        sys.exit(0)

    if args.status:
        status(args.start, args.end)
        sys.exit(0)

    stages = STAGES if args.stage == "all" else (args.stage,)
    for stage in stages:
        if len(stages) > 1:
            logger.info(f"########## stage: {stage} ##########")
        collect(
            stage=stage, start=args.start, end=args.end, workers=args.workers,
            single_day=args.day, limit=args.limit, retry_failed=args.retry_failed,
            dry_run=args.dry_run, strategy=args.image_strategy,
            dedup=not args.no_dedup, force_accept=args.force_accept,
        )


if __name__ == "__main__":
    main()
