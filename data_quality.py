"""
data_quality.py
----------------
Post-backfill data-quality pass over the materialized warehouse tables:
duplicate checks, null/completeness checks on required fields, referential
integrity between tables, value-sanity checks on trade amounts, and a
day-partition sanity check (does each row's block_time actually fall on the
day implied by the Parquet file it lives in).

Every check is read-only and independent — nothing here mutates data. Run
materialize_warehouse.py first for speed (this will still work against the
raw Parquet views if you skip that, just slower).

Usage:
    python data_quality.py
"""

import logging
import time

from collectors.warehouse import ENTITY_TABLES, connect, refresh_views
from config import PARQUET_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
logger = logging.getLogger("data_quality")

TRADE_TABLES = ["bonding_curve_trades", "pumpswap_trades"]


def check_leftover_part_files() -> bool:
    """
    A dangling `{table}_{date}.partNNNNNN.parquet` means some date's ingestion
    crashed before EntityStore.compact_day() merged/deduped it — that date's
    canonical file (if any) is stale/incomplete, and running compact_day()
    for it again is needed before its data can be trusted. Returns True if
    any were found.
    """
    print("\n=== 0. Leftover uncompacted part files (should be none) ===")
    found = False
    for table in ENTITY_TABLES:
        table_dir = PARQUET_DIR / table
        if not table_dir.exists():
            continue
        parts = sorted(table_dir.glob(f"{table}_*.part*.parquet"))
        if parts:
            found = True
            dates = sorted({p.name[len(table) + 1:].split(".part")[0] for p in parts})
            print(f"  {table}: {len(parts)} leftover part file(s) across {len(dates)} date(s): {dates}")
    if not found:
        print("  none found")
    return found


def _run(con, label: str, sql: str) -> None:
    t0 = time.monotonic()
    row = con.execute(sql).fetchone()
    dt = time.monotonic() - t0
    print(f"  [{dt:6.1f}s] {label}: {row}")


def check_duplicates(con) -> None:
    print("\n=== 1. Duplicate keys (should all be 0) ===")
    _run(con, "tokens dup mint", "SELECT count(*) FROM (SELECT mint FROM tokens GROUP BY mint HAVING count(*) > 1)")
    _run(con, "pools dup pool", "SELECT count(*) FROM (SELECT pool FROM pools GROUP BY pool HAVING count(*) > 1)")
    _run(con, "graduations dup signature", "SELECT count(*) FROM (SELECT signature FROM graduations GROUP BY signature HAVING count(*) > 1)")
    for table in TRADE_TABLES:
        _run(
            con, f"{table} dup (signature, tx_type)",
            f"SELECT count(*) FROM (SELECT signature, tx_type FROM {table} GROUP BY signature, tx_type HAVING count(*) > 1)",
        )


def check_nulls(con) -> None:
    print("\n=== 2. Required-field nulls (should all be 0) ===")
    _run(con, "tokens null mint/created_signature", "SELECT count(*) FROM tokens WHERE mint IS NULL OR created_signature IS NULL")
    _run(con, "pools null pool", "SELECT count(*) FROM pools WHERE pool IS NULL")
    _run(con, "graduations null signature", "SELECT count(*) FROM graduations WHERE signature IS NULL")
    for table in TRADE_TABLES:
        _run(con, f"{table} null signature/slot/block_time", f"SELECT count(*) FROM {table} WHERE signature IS NULL OR slot IS NULL OR block_time IS NULL")
    _run(con, "bonding_curve_trades null mint (unexpected — always known on-chain)", "SELECT count(*) FROM bonding_curve_trades WHERE mint IS NULL")
    _run(con, "pumpswap_trades null mint (expected if pool created before our window)", "SELECT count(*) FROM pumpswap_trades WHERE mint IS NULL")


def check_referential_integrity(con) -> None:
    print("\n=== 3. Referential integrity (some >0 is expected near the window's start edge) ===")
    _run(
        con, "bonding_curve_trades.mint not in tokens",
        "SELECT count(*) FROM bonding_curve_trades t WHERE NOT EXISTS (SELECT 1 FROM tokens k WHERE k.mint = t.mint)",
    )
    _run(
        con, "pumpswap_trades.pool not in pools",
        "SELECT count(*) FROM pumpswap_trades t WHERE t.pool IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pools p WHERE p.pool = t.pool)",
    )
    _run(
        con, "graduations.mint not in tokens",
        "SELECT count(*) FROM graduations g WHERE NOT EXISTS (SELECT 1 FROM tokens k WHERE k.mint = g.mint)",
    )


def check_value_sanity(con) -> None:
    print("\n=== 4. Trade value sanity (should all be 0 or near-0) ===")
    for table in TRADE_TABLES:
        _run(con, f"{table} sol_amount <= 0", f"SELECT count(*) FROM {table} WHERE sol_amount <= 0")
        _run(con, f"{table} token_amount <= 0", f"SELECT count(*) FROM {table} WHERE token_amount <= 0")
        _run(
            con, f"{table} is_buy mismatched with tx_type",
            f"SELECT count(*) FROM {table} WHERE (tx_type = 'buy' AND is_buy IS NOT TRUE) OR (tx_type = 'sell' AND is_buy IS NOT FALSE)",
        )
        _run(con, f"{table} negative coin_creator_fee", f"SELECT count(*) FROM {table} WHERE coin_creator_fee < 0")
    _run(con, "pumpswap_trades negative lp_fee/protocol_fee", "SELECT count(*) FROM pumpswap_trades WHERE lp_fee < 0 OR protocol_fee < 0")


def check_partition_consistency(con) -> None:
    """Does each row's block_time actually fall on the calendar day implied by
    the Parquet filename it was written into? A mismatch would mean a date
    was miscomputed at ingest time (e.g. a timezone bug) and the row landed
    in the wrong day's file."""
    print("\n=== 5. Day-partition consistency (block_time vs filename date; should be 0 mismatches) ===")
    for table in TRADE_TABLES + ["graduations"]:
        glob = str(PARQUET_DIR / table / f"{table}_????-??-??.parquet")  # canonical files only, not leftover parts
        sql = f"""
            SELECT count(*) FILTER (
                WHERE CAST(to_timestamp(block_time) AS DATE)
                      != CAST(regexp_extract(filename, '(\\d{{4}}-\\d{{2}}-\\d{{2}})\\.parquet$', 1) AS DATE)
            ) AS mismatches,
            count(*) AS total
            FROM read_parquet('{glob}', filename=true)
        """
        _run(con, f"{table} block_time/filename mismatches", sql)
    for table in ["tokens", "pools"]:
        glob = str(PARQUET_DIR / table / f"{table}_????-??-??.parquet")  # canonical files only, not leftover parts
        sql = f"""
            SELECT count(*) FILTER (
                WHERE CAST(to_timestamp(created_at) AS DATE)
                      != CAST(regexp_extract(filename, '(\\d{{4}}-\\d{{2}}-\\d{{2}})\\.parquet$', 1) AS DATE)
            ) AS mismatches,
            count(*) AS total
            FROM read_parquet('{glob}', filename=true)
        """
        _run(con, f"{table} created_at/filename mismatches", sql)


def check_cross_table_patterns(con) -> None:
    print("\n=== 6. Cross-table patterns (informational — not necessarily failures) ===")
    _run(con, "tokens total", "SELECT count(*) FROM tokens")
    _run(con, "pools total", "SELECT count(*) FROM pools")
    _run(con, "graduations total", "SELECT count(*) FROM graduations")
    _run(
        con, "tokens with >=1 bonding-curve trade",
        "SELECT count(DISTINCT mint) FROM bonding_curve_trades",
    )
    _run(
        con, "tokens with >=1 pumpswap trade",
        "SELECT count(DISTINCT mint) FROM pumpswap_trades WHERE mint IS NOT NULL",
    )


def main() -> None:
    has_leftover_parts = check_leftover_part_files()

    con = connect()
    tables = refresh_views(con)
    logger.info(f"Checking tables: {', '.join(tables)}")

    check_duplicates(con)
    check_nulls(con)
    check_referential_integrity(con)
    check_value_sanity(con)
    check_partition_consistency(con)
    check_cross_table_patterns(con)

    con.close()
    print("\nDone. Any nonzero count in sections 1, 2, or 4 needs a look; section 3 and the "
          "tokens/pools edge in section 2 are expected to be nonzero near the window's start "
          "(Oct 26, 2025) since trades can reference tokens/pools created before our coverage begins.")
    if has_leftover_parts:
        print("Section 0 found leftover part files — those dates were excluded from every check "
              "above (canonical-file-only glob). Run compact_day() for them, then rerun this script.")


if __name__ == "__main__":
    main()
