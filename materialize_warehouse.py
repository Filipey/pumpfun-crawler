"""
materialize_warehouse.py
-------------------------
Materializes the DuckDB views over data/parquet/ (defined in
collectors/warehouse.py) into real on-disk tables in data/warehouse.duckdb,
so repeated queries don't re-scan the full Parquet glob every time. Run this
once a backfill phase is done; safe to rerun any time more data is added
(drops and rebuilds each table from whatever's on disk).

Usage:
    python materialize_warehouse.py
"""

import logging
import time

from collectors.warehouse import connect, materialize, refresh_views

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
logger = logging.getLogger("materialize_warehouse")


def main() -> None:
    con = connect()
    views = refresh_views(con)
    if not views:
        logger.warning("No Parquet data found under data/parquet/ — nothing to materialize.")
        return

    print("\n=== Materializing tables (this scans all Parquet files on disk) ===")
    counts = {}
    for table in views:
        t0 = time.monotonic()
        counts.update(materialize(con, [table]))
        logger.info(f"{table}: {counts[table]:,} rows ({time.monotonic() - t0:.1f}s)")

    print("\n=== Final row counts (data/warehouse.duckdb) ===")
    for table, n in counts.items():
        print(f"  {table:24s} {n:>14,}")
    con.close()


if __name__ == "__main__":
    main()
