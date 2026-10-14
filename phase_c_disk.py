"""
phase_c_disk.py
---------------
Capacity report for Phase C: how much disk the collected images actually take,
how much the rest of the window will need, and whether it fits where
IMAGES_DIR currently points.

The projection is measured, not assumed: bytes-per-blob and the dedup rate
come from what is already on disk, so it self-corrects as collection
progresses rather than relying on the ~17.6 KB mean from the original
sampling run.

Usage
-----
    python phase_c_disk.py

Reads DATA_DIR / IMAGES_DIR from config (so .env applies). Read-only.
"""

import glob
import os

import duckdb

from config import DATA_DIR, IMAGES_DIR, PARQUET_DIR


def _scalar(con, query: str, default=0):
    """Run a query, tolerating a table that has no files on disk yet."""
    try:
        row = con.sql(query).fetchone()
        return (row[0] if row else default) or default
    except Exception:
        return default


def main() -> None:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")

    def g(table: str) -> str:
        # union_by_name tolerates schema drift across days, e.g. a column added
        # partway through collection.
        return f"read_parquet('{PARQUET_DIR / table / (table + '_*.parquet')}', union_by_name=true)"

    tokens    = _scalar(con, f"SELECT count(*) FROM {g('tokens')}")
    meta_rows = _scalar(con, f"SELECT count(*) FROM {g('token_metadata')}")
    meta_work = _scalar(con, f"SELECT count(*) FROM {g('token_metadata')} "
                             f"WHERE status='ok' AND image_url IS NOT NULL")
    img_ok    = _scalar(con, f"SELECT count(*) FROM {g('token_images')} WHERE status='ok'")
    img_blobs = _scalar(con, f"SELECT count(DISTINCT sha256) FROM {g('token_images')} "
                             f"WHERE status='ok'")

    shards = sorted(glob.glob(os.path.join(str(IMAGES_DIR), "shard_*.parquet")))
    shard_bytes = sum(os.path.getsize(p) for p in shards)

    meta_dir_bytes = sum(
        os.path.getsize(p)
        for p in glob.glob(str(PARQUET_DIR / "token_metadata" / "*.parquet"))
    )

    print(f"\nDATA_DIR    {DATA_DIR}")
    print(f"IMAGES_DIR  {IMAGES_DIR}\n")

    print(f"  tokens total              {tokens:>14,}")
    print(f"  token_metadata rows       {meta_rows:>14,}  "
          f"({meta_rows / max(tokens, 1):>6.1%} of tokens, {meta_dir_bytes / 1e9:.2f} GB)")
    print(f"    with an image_url       {meta_work:>14,}  <- the images stage's workload")
    print(f"  token_images ok           {img_ok:>14,}  "
          f"({img_ok / max(meta_work, 1):>6.1%} of that workload)")
    print(f"    distinct blobs          {img_blobs:>14,}  "
          f"dedup {1 - img_blobs / max(img_ok, 1):.1%}")
    print(f"  shards on disk            {len(shards):>14,}  {shard_bytes / 1e9:.1f} GB")

    if not (img_ok and img_blobs and shard_bytes):
        print("\n  Not enough collected yet to project. Run some of --stage images first.")
        return

    per_blob = shard_bytes / img_blobs
    stored_per_image = img_blobs / img_ok          # dedup already applied
    cost = lambda n: n * stored_per_image * per_blob

    todo_known = max(meta_work - img_ok, 0)
    # Tokens with no metadata yet produce images at the rate metadata observes.
    no_meta = max(tokens - meta_rows, 0)
    todo_later = no_meta * (meta_work / max(meta_rows, 1))

    print(f"\n  measured bytes per blob   {per_blob:>14,.0f}")
    print(f"  images awaiting fetch     {todo_known:>14,}  -> {cost(todo_known) / 1e9:>7.1f} GB")
    if no_meta:
        print(f"  tokens without metadata   {no_meta:>14,}  -> {cost(todo_later) / 1e9:>7.1f} GB "
              f"(their metadata is still to collect too)")
    total = cost(todo_known + todo_later)
    print(f"  {'TOTAL STILL NEEDED':<25} {'':>14}     {total / 1e9:>7.1f} GB")

    st = os.statvfs(str(IMAGES_DIR))
    free = st.f_bavail * st.f_frsize
    print(f"\n  free on the IMAGES_DIR filesystem    {free / 1e9:>7.1f} GB")
    if total > free:
        print(f"  ** SHORT BY {(total - free) / 1e9:.1f} GB — point IMAGES_DIR at a bigger "
              f"filesystem before continuing. Shard references in token_images are\n"
              f"     bare filenames, so moving the directory and setting IMAGES_DIR is "
              f"enough; nothing needs recollecting.")
    else:
        print(f"  fits, with {(free - total) / 1e9:.1f} GB to spare")
    print()


if __name__ == "__main__":
    main()
