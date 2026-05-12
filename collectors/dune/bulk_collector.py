"""
collectors/dune/bulk_collector.py
----------------------------------
Retroactive bulk collection of pump.fun data via Dune Analytics.

This is the FREE, large-scale collector for historical data (jan/2024 → now).
It paginates queries by day to stay within Dune's row limits.

Two workflows:
  A) API mode (--dune-mode api): fully automatic, costs ~10 credits/query
  B) Manual mode (--dune-mode manual): generates SQL files, user runs in IDE,
     imports CSVs back — completely free, unlimited

Output: same Parquet schema as Bitquery collectors (see schemas.py).
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from collectors.dune.dune_runner import DuneRunner
from collectors.dune.queries import (ALL_TRADES_SQL, FIRST_BUYERS_SQL,
                                     MIGRATIONS_SQL, OHLCV_SQL,
                                     TOKEN_CREATIONS_RAW_SQL,
                                     TOKEN_CREATIONS_SQL, TOKEN_STATS_SQL,
                                     TOP_CREATORS_SQL, TRADES_SQL,
                                     WALLET_TRADES_SQL)
from config import PARQUET_DIR
from schemas import (MIGRATION_COLS, OHLCV_COLS, TOKEN_CREATION_COLS,
                     TRADE_COLS, coerce_schema)
from utils import date_range_days, save_parquet

logger = logging.getLogger(__name__)

SOL_MINT = "So11111111111111111111111111111111111111112"


class DuneBulkCollector:
    """
    Retroactive pump.fun data collector using Dune Analytics.
    Covers all data needs for the three research axes.
    """

    def __init__(self, runner: DuneRunner | None = None):
        self.runner = runner or DuneRunner()
        self.out = PARQUET_DIR / "dune"
        self.out.mkdir(parents=True, exist_ok=True)

    def _day_str(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d")

    # ══════════════════════════════════════════════════════════════════════
    #  AXIS 1: Token creations (taxonomy input)
    # ══════════════════════════════════════════════════════════════════════

    def collect_token_creations(
        self, start: datetime, end: datetime, use_raw_fallback: bool = False
    ) -> Path:
        """
        Collect all token creation events in [start, end), one query per day.
        Output: Parquet with columns from TOKEN_CREATION_COLS.
        """
        sql = TOKEN_CREATIONS_RAW_SQL if use_raw_fallback else TOKEN_CREATIONS_SQL
        all_dfs = []

        for day_start, day_end in tqdm(
            list(date_range_days(start, end)), desc="Token creations"
        ):
            ds, de = self._day_str(day_start), self._day_str(day_end)
            try:
                df = self.runner.execute_sql(
                    sql, {"date_start": ds, "date_end": de},
                    name=f"creations_{ds}",
                )
                if not df.empty:
                    df["source"] = "dune"
                    all_dfs.append(df)
            except Exception as e:
                logger.error(f"Failed creations for {ds}: {e}")

        if not all_dfs:
            return Path()

        result = pd.concat(all_dfs, ignore_index=True)
        result = coerce_schema(result, TOKEN_CREATION_COLS)
        out_path = self.out / f"creations_{self._day_str(start)}_{self._day_str(end)}.parquet"
        save_parquet(result, out_path)
        return out_path

    # ══════════════════════════════════════════════════════════════════════
    #  AXIS 3: Trades (IRL input)
    # ══════════════════════════════════════════════════════════════════════

    def collect_all_trades(self, start: datetime, end: datetime) -> Path:
        """
        Collect ALL pump.fun trades in [start, end), day by day.
        This is the bulk version — no specific mint filter.
        Warning: can be very large (millions of rows/day in peak periods).
        """
        all_dfs = []

        for day_start, day_end in tqdm(
            list(date_range_days(start, end)), desc="All trades"
        ):
            ds, de = self._day_str(day_start), self._day_str(day_end)
            try:
                df = self.runner.execute_sql(
                    ALL_TRADES_SQL, {"date_start": ds, "date_end": de},
                    name=f"trades_all_{ds}",
                )
                if not df.empty:
                    df["source"] = "dune"
                    all_dfs.append(df)
                    # Save per-day to avoid memory issues
                    day_path = self.out / f"trades_all_{ds}.parquet"
                    df.to_parquet(day_path, index=False)
                    logger.info(f"Saved {len(df)} trades for {ds}")
            except Exception as e:
                logger.error(f"Failed trades for {ds}: {e}")

        if not all_dfs:
            return Path()

        return self.out  # return directory, not single file

    def collect_token_trades(
        self, mint: str, start: datetime, end: datetime
    ) -> Path:
        """Collect trades for a specific token in [start, end)."""
        all_dfs = []
        for day_start, day_end in date_range_days(start, end):
            ds, de = self._day_str(day_start), self._day_str(day_end)
            try:
                df = self.runner.execute_sql(
                    TRADES_SQL, {"mint": mint, "date_start": ds, "date_end": de},
                    name=f"trades_{mint[:8]}_{ds}",
                )
                if not df.empty:
                    df["source"] = "dune"
                    all_dfs.append(df)
            except Exception as e:
                logger.error(f"Failed trades for {mint[:12]} on {ds}: {e}")

        if not all_dfs:
            return Path()

        result = pd.concat(all_dfs, ignore_index=True)
        result = coerce_schema(result, TRADE_COLS)
        out_path = self.out / f"trades_{mint[:16]}_{self._day_str(start)}.parquet"
        save_parquet(result, out_path)
        return out_path

    def collect_first_buyers(self, mint: str, limit: int = 100) -> Path:
        """Collect the first N buyers of a specific token."""
        try:
            df = self.runner.execute_sql(
                FIRST_BUYERS_SQL, {"mint": mint, "limit": str(limit)},
                name=f"first_buyers_{mint[:8]}",
            )
        except Exception as e:
            logger.error(f"Failed first buyers for {mint[:12]}: {e}")
            return Path()

        if df.empty:
            return Path()

        df["source"] = "dune"
        df = coerce_schema(df, TRADE_COLS)
        out_path = self.out / f"first_buyers_{mint[:16]}.parquet"
        save_parquet(df, out_path)
        return out_path

    def collect_wallet_trades(
        self, wallet: str, start: datetime, end: datetime
    ) -> Path:
        """Collect all pump.fun trades by a specific wallet (cross-token)."""
        all_dfs = []
        for day_start, day_end in date_range_days(start, end):
            ds, de = self._day_str(day_start), self._day_str(day_end)
            try:
                df = self.runner.execute_sql(
                    WALLET_TRADES_SQL,
                    {"wallet": wallet, "date_start": ds, "date_end": de},
                    name=f"wallet_{wallet[:8]}_{ds}",
                )
                if not df.empty:
                    df["source"] = "dune"
                    all_dfs.append(df)
            except Exception as e:
                logger.error(f"Failed wallet trades {wallet[:12]} on {ds}: {e}")

        if not all_dfs:
            return Path()

        result = pd.concat(all_dfs, ignore_index=True)
        out_path = self.out / f"wallet_{wallet[:16]}.parquet"
        save_parquet(result, out_path)
        return out_path

    # ══════════════════════════════════════════════════════════════════════
    #  Migrations (graduation events)
    # ══════════════════════════════════════════════════════════════════════

    def collect_migrations(self, start: datetime, end: datetime) -> Path:
        """Collect all graduation events in [start, end)."""
        all_dfs = []
        for day_start, day_end in tqdm(
            list(date_range_days(start, end)), desc="Migrations"
        ):
            ds, de = self._day_str(day_start), self._day_str(day_end)
            try:
                df = self.runner.execute_sql(
                    MIGRATIONS_SQL, {"date_start": ds, "date_end": de},
                    name=f"migrations_{ds}",
                )
                if not df.empty:
                    df["source"] = "dune"
                    all_dfs.append(df)
            except Exception as e:
                logger.error(f"Failed migrations for {ds}: {e}")

        if not all_dfs:
            return Path()

        result = pd.concat(all_dfs, ignore_index=True)
        out_path = self.out / f"migrations_{self._day_str(start)}_{self._day_str(end)}.parquet"
        save_parquet(result, out_path)
        return out_path

    # ══════════════════════════════════════════════════════════════════════
    #  Aggregates & discovery
    # ══════════════════════════════════════════════════════════════════════

    def collect_token_stats(self, start: datetime, end: datetime) -> Path:
        """
        Get summary stats per token in [start, end):
        total trades, unique traders, total volume, max price.
        Useful for filtering which tokens to deep-dive.
        """
        ds, de = self._day_str(start), self._day_str(end)
        try:
            df = self.runner.execute_sql(
                TOKEN_STATS_SQL, {"date_start": ds, "date_end": de},
                name=f"token_stats_{ds}_{de}",
            )
        except Exception as e:
            logger.error(f"Failed token stats: {e}")
            return Path()

        if df.empty:
            return Path()

        df["source"] = "dune"
        out_path = self.out / f"token_stats_{ds}_{de}.parquet"
        save_parquet(df, out_path)
        return out_path

    def collect_top_creators(self, start: datetime, end: datetime) -> Path:
        """Get top 500 creators by number of tokens launched."""
        ds, de = self._day_str(start), self._day_str(end)
        try:
            df = self.runner.execute_sql(
                TOP_CREATORS_SQL, {"date_start": ds, "date_end": de},
                name=f"top_creators_{ds}_{de}",
            )
        except Exception as e:
            logger.error(f"Failed top creators: {e}")
            return Path()

        if df.empty:
            return Path()

        df["source"] = "dune"
        out_path = self.out / f"top_creators_{ds}_{de}.parquet"
        save_parquet(df, out_path)
        return out_path

    # ══════════════════════════════════════════════════════════════════════
    #  Manual mode: generate SQL files for free IDE execution
    # ══════════════════════════════════════════════════════════════════════

    def generate_sql_files(
        self, start: datetime, end: datetime, query_type: str = "all"
    ) -> list[Path]:
        """
        Generate SQL files for manual execution in the Dune IDE.
        One file per day per query type. Completely free, no API credits.

        After running in the IDE:
          1. Download CSV from each query result
          2. Place CSVs in data/raw/dune_csv/
          3. Run: python pipeline.py --dune-import-csv
        """
        date_ranges = [
            (self._day_str(ds), self._day_str(de))
            for ds, de in date_range_days(start, end)
        ]

        queries = {
            "creations": TOKEN_CREATIONS_RAW_SQL,
            "trades": ALL_TRADES_SQL,
            "migrations": MIGRATIONS_SQL,
        }

        if query_type != "all":
            queries = {query_type: queries[query_type]}

        all_paths = []
        for name, sql in queries.items():
            paths = self.runner.export_sql_files(sql, date_ranges, name_prefix=name)
            all_paths.extend(paths)

        logger.info(
            f"Generated {len(all_paths)} SQL files in {self.runner.sql_export_dir}\n"
            f"  → Run each in Dune IDE (free), download CSV\n"
            f"  → Place CSVs in {self.runner.csv_import_dir}\n"
            f"  → Then run: python pipeline.py --dune-import-csv"
        )
        return all_paths

    def import_manual_csvs(self) -> dict[str, pd.DataFrame]:
        """
        Import CSVs from manual Dune IDE execution.
        Tries to detect the type (creations, trades, migrations) from filename.
        """
        combined = self.runner.import_csvs()
        if combined.empty:
            return {}

        results = {}
        for source_file, group in combined.groupby("_source_file"):
            fname = str(source_file).lower()
            if "creation" in fname:
                dtype = "creations"
            elif "trade" in fname:
                dtype = "trades"
            elif "migration" in fname:
                dtype = "migrations"
            else:
                dtype = "unknown"

            if dtype not in results:
                results[dtype] = []
            results[dtype].append(group)

        output = {}
        for dtype, dfs in results.items():
            df = pd.concat(dfs, ignore_index=True)
            out_path = self.out / f"{dtype}_imported.parquet"
            save_parquet(df, out_path)
            output[dtype] = df
            logger.info(f"Imported {len(df)} rows of type '{dtype}'")

        return output
