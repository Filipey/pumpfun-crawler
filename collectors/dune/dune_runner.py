"""
collectors/dune/dune_runner.py
-------------------------------
Wrapper around dune-client SDK for executing parameterized SQL queries.

Two modes of operation:
  1. API mode: create + execute queries programmatically (costs credits)
  2. Manual mode: generate SQL files for the user to run in the Dune IDE
     (free, unlimited) and then import the CSVs

The free Dune plan gives ~2500 API credits/month. Each medium execution
costs ~10 credits, so ~250 queries/month. For bulk retroactive collection,
manual mode may be necessary.
"""

import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import DUNE_API_KEY, RAW_DIR

logger = logging.getLogger(__name__)

# Try to import dune_client; gracefully handle if not installed
try:
    from dune_client.client import DuneClient
    from dune_client.query import QueryBase
    from dune_client.types import QueryParameter
    HAS_DUNE_CLIENT = True
except ImportError:
    HAS_DUNE_CLIENT = False
    logger.warning("dune-client not installed. Install with: pip install dune-client")


class DuneRunner:
    """
    Executes SQL queries against Dune Analytics.

    Supports:
      - API execution (programmatic, costs credits)
      - SQL file generation (for manual IDE execution, free)
      - CSV import (load results exported manually from Dune IDE)
    """

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or DUNE_API_KEY
        self._client = None
        self.sql_export_dir = RAW_DIR / "dune_sql"
        self.csv_import_dir = RAW_DIR / "dune_csv"
        self.sql_export_dir.mkdir(parents=True, exist_ok=True)
        self.csv_import_dir.mkdir(parents=True, exist_ok=True)

    @property
    def client(self) -> "DuneClient":
        if self._client is None:
            if not HAS_DUNE_CLIENT:
                raise ImportError("dune-client is required. pip install dune-client")
            self._client = DuneClient(api_key=self.api_key)
        return self._client

    def _render_sql(self, sql_template: str, params: dict) -> str:
        """Replace {{param}} placeholders in SQL with actual values."""
        rendered = sql_template
        for key, value in params.items():
            rendered = rendered.replace(f"{{{{{key}}}}}", str(value))
        return rendered

    # ── API mode ──────────────────────────────────────────────────────────

    def execute_sql(
        self,
        sql_template: str,
        params: dict,
        name: str = "pumpfun_query",
    ) -> pd.DataFrame:
        """
        Create a query on Dune, execute it, and return results as DataFrame.
        Costs API credits (~10 per medium execution).
        """
        rendered = self._render_sql(sql_template, params)
        logger.info(f"Executing Dune query: {name} ({len(rendered)} chars)")

        try:
            query = self.client.create_query(
                name=f"[pumpfun-research] {name}",
                query_sql=rendered,
                is_private=True,
            )
            query_id = query.base.query_id
            logger.info(f"Created query ID: {query_id}")

            result = self.client.run_query_dataframe(
                QueryBase(query_id=query_id)
            )

            # Clean up the query to not clutter the account
            try:
                self.client.archive_query(query_id)
            except Exception:
                pass

            logger.info(f"Got {len(result)} rows from Dune")
            return result

        except Exception as e:
            logger.error(f"Dune API execution failed: {e}")
            raise

    # ── Manual / hybrid mode ──────────────────────────────────────────────

    def export_sql_files(
        self,
        sql_template: str,
        date_ranges: list[tuple[str, str]],
        name_prefix: str = "query",
    ) -> list[Path]:
        """
        Generate .sql files (one per date window) for manual execution
        in the Dune IDE. Returns list of file paths created.

        User workflow:
          1. Run this to generate SQL files
          2. Copy each SQL into Dune IDE, run it, download CSV
          3. Place CSVs in data/raw/dune_csv/
          4. Run import_csvs() to consolidate into Parquet
        """
        paths = []
        for i, (start, end) in enumerate(date_ranges):
            rendered = self._render_sql(sql_template, {
                "date_start": start,
                "date_end": end,
            })
            fname = f"{name_prefix}_{start}_{end}.sql".replace(" ", "_")
            path = self.sql_export_dir / fname
            path.write_text(rendered, encoding="utf-8")
            paths.append(path)

        logger.info(f"Exported {len(paths)} SQL files to {self.sql_export_dir}")
        return paths

    def import_csvs(
        self,
        glob_pattern: str = "*.csv",
        source_label: str = "dune",
    ) -> pd.DataFrame:
        """
        Import all CSV files from the dune_csv directory (manually exported
        from the Dune IDE) and concatenate into a single DataFrame.
        """
        csv_files = sorted(self.csv_import_dir.glob(glob_pattern))
        if not csv_files:
            logger.warning(f"No CSV files found in {self.csv_import_dir}")
            return pd.DataFrame()

        dfs = []
        for f in csv_files:
            try:
                df = pd.read_csv(f)
                df["source"] = source_label
                df["_source_file"] = f.name
                dfs.append(df)
                logger.info(f"Imported {len(df)} rows from {f.name}")
            except Exception as e:
                logger.error(f"Error reading {f}: {e}")

        if not dfs:
            return pd.DataFrame()

        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Total imported: {len(combined)} rows from {len(dfs)} files")
        return combined
