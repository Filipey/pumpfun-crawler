"""
collectors/migration_collector.py
----------------------------------
Collects graduation/migration events (pump.fun bonding curve → PumpSwap AMM).
"""

import logging
from datetime import datetime
from pathlib import Path

from client import BitqueryClient
from config import PARQUET_DIR
from queries.migrations import MIGRATIONS_BATCH, TOKEN_MIGRATION_STATUS
from utils import records_to_df, save_parquet, time_windows

logger = logging.getLogger(__name__)


class MigrationCollector:
    """Collects token graduation events."""

    def __init__(self, client: BitqueryClient):
        self.client = client
        self.output_dir = PARQUET_DIR / "migrations"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def collect_migrations(
        self, start: datetime, end: datetime, window_minutes: int = 60
    ) -> Path:
        """Batch-collect all graduation events in [start, end)."""
        all_records = []
        for after, before in time_windows(start, end, window_minutes):
            logger.info(f"Fetching migrations: {after} → {before}")
            data = self.client.query(MIGRATIONS_BATCH, {
                "after": after, "before": before
            })
            rows = data.get("Solana", {}).get("Instructions", [])
            all_records.extend(rows)
            if rows:
                logger.info(f"  → {len(rows)} migrations")

        df = records_to_df(all_records)
        if df.empty:
            logger.warning("No migrations found")
            return Path()

        out_path = self.output_dir / f"migrations_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.parquet"
        save_parquet(df, out_path)
        return out_path

    def check_migration(self, mint: str) -> dict | None:
        """Check if a specific token has graduated to PumpSwap."""
        data = self.client.query(TOKEN_MIGRATION_STATUS, {"mint": mint})
        rows = data.get("Solana", {}).get("Instructions", [])
        if not rows:
            return None
        r = rows[0]
        return {
            "mint": mint,
            "migrated": True,
            "migration_time": r.get("Block", {}).get("Time"),
            "migration_slot": r.get("Block", {}).get("Slot"),
            "tx_signature": r.get("Transaction", {}).get("Signature"),
        }
