"""
backup_manifest.py
------------------
Write (or verify) a checksum manifest for a set of collected files, so a
backup can be proven intact after a restore.

Why this exists
---------------
The image blob shards are keyed by SHA-256 of their contents and mean nothing
on their own: the mint -> sha256 -> shard mapping lives only in the
`token_images` Parquet ledger. A backup that loses the ledger leaves ~85 GB of
anonymous blobs. A manifest makes both the presence and the integrity of every
piece checkable years later, including after a cloud round-trip that silently
truncated or re-encoded something.

Parquet row counts come from the file footer, so they are read without
touching the data pages — a cheap second signal that catches a file which
hashes differently because it was rewritten rather than corrupted.

Usage
-----
    python backup_manifest.py                      # Phase C set -> manifest file
    python backup_manifest.py --all                # every table plus images
    python backup_manifest.py --paths a/ b.parquet  # an explicit set
    python backup_manifest.py --verify data/backup_manifest.tsv
    python backup_manifest.py --verify m.tsv --quick # sizes/rows only, no hashing

Output is TSV: relative_path, bytes, rows ("-" if not Parquet), sha256.
"""

import argparse
import hashlib
import os
import sys
from pathlib import Path

from config import DATA_DIR, IMAGES_DIR, PARQUET_DIR

PHASE_C_TABLES = ("token_metadata", "token_images", "token_pumpfun_api")
_CHUNK = 8 * 1024 * 1024


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(_CHUNK):
            h.update(chunk)
    return h.hexdigest()


def _rows(path: Path) -> str:
    """Parquet row count from the footer; '-' for anything else."""
    if path.suffix != ".parquet":
        return "-"
    try:
        import pyarrow.parquet as pq
        return str(pq.ParquetFile(path).metadata.num_rows)
    except Exception:
        return "?"


def _collect(roots: list[Path]) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        if root.is_file():
            files.append(root)
        elif root.is_dir():
            files += [p for p in sorted(root.rglob("*")) if p.is_file()]
    return sorted(set(files))


def _default_roots(all_tables: bool) -> list[Path]:
    tables = sorted(p.name for p in PARQUET_DIR.iterdir() if p.is_dir()) \
        if all_tables and PARQUET_DIR.exists() else list(PHASE_C_TABLES)
    roots = [PARQUET_DIR / t for t in tables]
    roots.append(Path(IMAGES_DIR))
    roots.append(DATA_DIR / "checkpoint.json")
    return [r for r in roots if r.exists()]


def _rel(path: Path) -> str:
    """A path that survives the move to a backup and back."""
    for base, label in ((Path(IMAGES_DIR), "images"), (PARQUET_DIR, "parquet"), (DATA_DIR, "data")):
        try:
            return f"{label}/{path.relative_to(base)}"
        except ValueError:
            continue
    return str(path)


def write(roots: list[Path], out: Path, quick: bool) -> None:
    files = _collect(roots)
    if not files:
        print("Nothing to manifest — check the paths.", file=sys.stderr)
        sys.exit(1)
    total = sum(f.stat().st_size for f in files)
    print(f"Hashing {len(files):,} file(s), {total / 1e9:.1f} GB ...", file=sys.stderr)

    out.parent.mkdir(parents=True, exist_ok=True)
    done_bytes = 0
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("# relative_path\tbytes\trows\tsha256\n")
        for i, path in enumerate(files, 1):
            size = path.stat().st_size
            digest = "-" if quick else _sha256(path)
            fh.write(f"{_rel(path)}\t{size}\t{_rows(path)}\t{digest}\n")
            done_bytes += size
            if i % 25 == 0 or i == len(files):
                print(f"  {i:,}/{len(files):,}  {done_bytes / 1e9:.1f}/{total / 1e9:.1f} GB",
                      file=sys.stderr)
    print(f"\nManifest: {out}  ({len(files):,} files, {total / 1e9:.1f} GB)", file=sys.stderr)


def verify(manifest: Path, quick: bool) -> int:
    rows = []
    with open(manifest, encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            rel, size, nrows, digest = line.rstrip("\n").split("\t")
            rows.append((rel, int(size), nrows, digest))

    bases = {"images": Path(IMAGES_DIR), "parquet": PARQUET_DIR, "data": DATA_DIR}
    missing = bad_size = bad_rows = bad_hash = ok = 0
    for rel, size, nrows, digest in rows:
        label, _, rest = rel.partition("/")
        path = bases.get(label, DATA_DIR) / rest
        if not path.exists():
            print(f"MISSING    {rel}"); missing += 1; continue
        if path.stat().st_size != size:
            print(f"SIZE       {rel}  manifest={size:,} local={path.stat().st_size:,}")
            bad_size += 1; continue
        if nrows not in ("-", "?") and _rows(path) != nrows:
            print(f"ROWS       {rel}  manifest={nrows} local={_rows(path)}")
            bad_rows += 1; continue
        if not quick and digest != "-" and _sha256(path) != digest:
            print(f"CHECKSUM   {rel}"); bad_hash += 1; continue
        ok += 1

    print(f"\n{ok:,}/{len(rows):,} verified"
          + (f" · {missing} missing" if missing else "")
          + (f" · {bad_size} wrong size" if bad_size else "")
          + (f" · {bad_rows} wrong row count" if bad_rows else "")
          + (f" · {bad_hash} checksum mismatch" if bad_hash else ""))
    return 0 if ok == len(rows) else 1


def main() -> None:
    p = argparse.ArgumentParser(description="Checksum manifest for backup verification.")
    p.add_argument("--verify", metavar="MANIFEST", help="Verify local files against a manifest.")
    p.add_argument("--all", action="store_true",
                   help="Every table under parquet/, not just the Phase C ones.")
    p.add_argument("--paths", nargs="+", default=None, help="Explicit files/directories.")
    p.add_argument("--out", default=None,
                   help="Manifest path (default: $DATA_DIR/backup_manifest.tsv)")
    p.add_argument("--quick", action="store_true",
                   help="Skip SHA-256; compare sizes and Parquet row counts only.")
    args = p.parse_args()

    if args.verify:
        sys.exit(verify(Path(args.verify), args.quick))

    roots = [Path(x) for x in args.paths] if args.paths else _default_roots(args.all)
    out = Path(args.out) if args.out else DATA_DIR / "backup_manifest.tsv"
    write(roots, out, args.quick)


if __name__ == "__main__":
    main()
