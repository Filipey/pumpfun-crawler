"""
collectors/warehouse.py
------------------------
DuckDB analysis layer on top of the day-partitioned Parquet files written by
EntityStore. Parquet remains the source of truth (crash-safe, one file per
table per day); DuckDB just gives a single-file, SQL-queryable view over it.

Two modes per table, picked by refresh_views():
  - VIEW over a glob of *_YYYY-MM-DD.parquet files (cheap, always reflects
    whatever is on disk right now, no duplication) — used while a backfill is
    still in progress.
  - TABLE materialized from that view (fast to query repeatedly, indexed) —
    built once a phase is finished via materialize().

Usage
-----
    from collectors.warehouse import connect, refresh_views, materialize

    con = connect()
    refresh_views(con)                 # cheap, safe to call anytime
    con.sql("SELECT count(*) FROM bonding_curve_trades").show()

    materialize(con, ["bonding_curve_trades", "pumpswap_trades"])  # slower, once backfill is done
"""

import logging
from pathlib import Path

import duckdb

from config import PARQUET_DIR, WAREHOUSE_DB_PATH
from schemas import ENTITY_TABLES

logger = logging.getLogger(__name__)


def connect(db_path: Path = WAREHOUSE_DB_PATH) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    # Every timestamp column (block_time, created_at, ...) is a UTC unix
    # epoch. DuckDB's to_timestamp() otherwise interprets/casts through the
    # session's local timezone, silently shifting the calendar day for any
    # non-UTC host — e.g. 2025-11-10T00:00:00Z reads back as 2025-11-09.
    con.execute("SET TimeZone='UTC'")
    return con


def _glob_for(table: str) -> str:
    # Deliberately `????-??-??` rather than `*` — a broader glob would also
    # match leftover `{table}_{date}.partNNNNNN.parquet` files from a run
    # that crashed before EntityStore.compact_day() merged/deduped them,
    # silently double-counting or including undeduplicated rows. Only the
    # canonical compacted file is a trustworthy source for analysis; a
    # dangling part file means that date needs compact_day() rerun, not to
    # be queried as-is.
    return str(PARQUET_DIR / table / f"{table}_????-??-??.parquet")


def _catalog_type(con: duckdb.DuckDBPyConnection, table: str) -> str | None:
    """'BASE TABLE', 'VIEW', or None if no catalog entry exists with this name.

    CREATE OR REPLACE (and DROP ... IF EXISTS) in DuckDB are type-specific —
    they error out ("Existing object x is of type Table, trying to replace/
    drop with type View") rather than silently handling a name that already
    exists as the *other* kind of relation. Both functions below need to know
    which kind is currently there before touching it.
    """
    row = con.execute(
        "SELECT table_type FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()
    return row[0] if row else None


def refresh_views(con: duckdb.DuckDBPyConnection) -> list[str]:
    """
    (Re)create a VIEW per entity table over its Parquet glob. Skips tables
    with no files on disk yet, and leaves any already-materialized table
    (from a prior materialize() call) alone rather than erroring. Safe to
    call repeatedly (e.g. after every day's ingestion) — cheap, no data copy.
    """
    created = []
    for table in ENTITY_TABLES:
        table_dir = PARQUET_DIR / table
        if not table_dir.exists() or not any(table_dir.glob(f"{table}_*.parquet")):
            continue
        if _catalog_type(con, table) == "BASE TABLE":
            created.append(table)  # already materialized — queryable as-is
            continue
        glob = _glob_for(table)
        con.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{glob}')")
        created.append(table)
    if created:
        logger.info(f"Refreshed views: {', '.join(created)}")
    return created


def materialize(con: duckdb.DuckDBPyConnection, tables: list[str] | None = None) -> dict[str, int]:
    """
    Replace the given tables' views with materialized DuckDB tables (or all
    tables with data on disk, if `tables` is omitted). Much faster for
    repeated querying once a table's ingestion is done; re-run refresh_views()
    + materialize() again later if more data is added.
    """
    targets = tables or list(ENTITY_TABLES)
    counts: dict[str, int] = {}
    for table in targets:
        glob = _glob_for(table)
        table_dir = PARQUET_DIR / table
        if not table_dir.exists() or not any(table_dir.glob(f"{table}_*.parquet")):
            continue
        if _catalog_type(con, table) == "VIEW":
            con.execute(f"DROP VIEW {table}")
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_parquet('{glob}')")
        n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        counts[table] = n
        logger.info(f"Materialized {table}: {n:,} rows")
    return counts


def table_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    counts = {}
    for table in ENTITY_TABLES:
        try:
            counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        except duckdb.CatalogException:
            continue
    return counts
