"""
collectors/holder_collector.py
-------------------------------
Collects holder snapshots and balance history.
Used for: concentration metrics (top-N share), bundle detection, dev tracking.
"""

import logging
from datetime import datetime
from pathlib import Path

from client import BitqueryClient
from config import PARQUET_DIR, TOP_HOLDERS_LIMIT
from queries.holders import BALANCE_HISTORY, TOP_HOLDERS, WALLET_BALANCE
from utils import records_to_df, save_parquet, time_windows

logger = logging.getLogger(__name__)


class HolderCollector:
    """Collects holder data for pump.fun tokens."""

    def __init__(self, client: BitqueryClient):
        self.client = client
        self.output_dir = PARQUET_DIR / "holders"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def collect_top_holders(
        self, mint: str, limit: int = TOP_HOLDERS_LIMIT
    ) -> Path:
        """Snapshot of top N holders by balance for a token."""
        logger.info(f"Fetching top {limit} holders for {mint[:12]}...")
        data = self.client.query(TOP_HOLDERS, {"mint": mint, "limit": limit})
        rows = data.get("Solana", {}).get("BalanceUpdates", [])
        df = records_to_df(rows)

        if df.empty:
            return Path()

        col_map = {
            "BalanceUpdate.Account.Address": "holder_address",
            "BalanceUpdate.PostBalance": "balance",
            "BalanceUpdate.PreBalance": "prev_balance",
            "BalanceUpdate.Currency.MintAddress": "token_mint",
            "BalanceUpdate.Currency.Symbol": "token_symbol",
            "Block.Time": "snapshot_time",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        df["token_mint"] = mint

        out_path = self.output_dir / f"top_holders_{mint[:16]}.parquet"
        save_parquet(df, out_path)
        return out_path

    def collect_dev_balance(self, mint: str, dev_address: str) -> dict:
        """Check how much the dev still holds of their token."""
        data = self.client.query(WALLET_BALANCE, {
            "mint": mint, "wallet": dev_address
        })
        rows = data.get("Solana", {}).get("BalanceUpdates", [])
        if not rows:
            return {"mint": mint, "dev": dev_address, "balance": 0}
        row = rows[0]
        return {
            "mint": mint,
            "dev": dev_address,
            "balance": row.get("BalanceUpdate", {}).get("PostBalance", 0),
            "time": row.get("Block", {}).get("Time"),
        }

    def collect_balance_history(
        self, mint: str, start: datetime, end: datetime
    ) -> Path:
        """
        Full temporal balance update history for a token.
        Gives: who gained/lost tokens at every block.
        """
        all_records = []
        for after, before in time_windows(start, end, window_minutes=60):
            data = self.client.query(BALANCE_HISTORY, {
                "mint": mint, "after": after, "before": before
            })
            rows = data.get("Solana", {}).get("BalanceUpdates", [])
            all_records.extend(rows)

        df = records_to_df(all_records)
        if df.empty:
            return Path()

        out_path = self.output_dir / f"balance_history_{mint[:16]}_{start.strftime('%Y%m%d')}.parquet"
        save_parquet(df, out_path)
        logger.info(f"Collected {len(df)} balance updates for {mint[:12]}...")
        return out_path
