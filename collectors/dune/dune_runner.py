"""
collectors/dune/dune_runner.py
-------------------------------
Wrapper around dune-client SDK for executing Dune Analytics queries.

Free-tier usage (REQUIRED approach):
  The Dune free plan CANNOT create queries via API (POST /api/v1/query → 402).
  Queries must be created once in the Dune web IDE, then executed by ID:

    runner.run_saved_query(query_id=1234567, params={"date_start": "2025-01-01", ...})

  This uses POST /api/v1/query/{id}/execute which IS available on the free tier.
  Each execution costs ~10 credits from the 2,500/month free budget.

One-time setup:
  Run: python collect_dune_auto.py --setup
  It prints the SQL and guides you through creating the 3 queries in the IDE.
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

    # ── API mode (free-tier compatible) ──────────────────────────────────────

    def run_saved_query(
        self,
        query_id: int,
        params: dict,
        name: str = "query",
    ) -> pd.DataFrame:
        """
        Execute a pre-saved Dune query with text parameters.

        Free-tier compatible: uses POST /api/v1/query/{id}/execute, which does
        NOT require a paid plan. Creating queries via API (POST /api/v1/query)
        does require a paid plan and will return 402.

        Args:
            query_id: numeric ID from the Dune query URL (dune.com/queries/ID)
            params:   dict of parameter name → value (all passed as text type)
            name:     label used in log messages only
        """
        if not HAS_DUNE_CLIENT:
            raise ImportError("dune-client is required. pip install dune-client")

        from dune_client.query import QueryBase
        from dune_client.types import QueryParameter

        logger.info(f"Executing Dune query {query_id}: {name}")

        dune_params = [
            QueryParameter.text_type(k, str(v))
            for k, v in params.items()
        ]

        try:
            result = self.client.run_query_dataframe(
                QueryBase(query_id=query_id, params=dune_params)
            )
            logger.info(f"Got {len(result)} rows from query {query_id}")
            return result
        except Exception as e:
            logger.error(f"Dune API execution failed: {e}")
            raise

    def execute_sql(
        self,
        sql_template: str,
        params: dict,
        name: str = "pumpfun_query",
    ) -> pd.DataFrame:
        """
        DEPRECATED on free tier — POST /api/v1/query returns 402.
        Kept for paid-plan users or backward compatibility.
        Use run_saved_query() instead.
        """
        rendered = self._render_sql(sql_template, params)
        logger.info(f"Executing Dune query (via create): {name} ({len(rendered)} chars)")
        logger.warning(
            "execute_sql() creates a new query via API, which requires a paid Dune plan. "
            "If you see 402 errors, use run_saved_query() instead."
        )

        try:
            from dune_client.query import QueryBase
            query = self.client.create_query(
                name=f"[pumpfun-research] {name}",
                query_sql=rendered,
                is_private=True,
            )
            query_id = query.base.query_id
            result = self.client.run_query_dataframe(QueryBase(query_id=query_id))
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
