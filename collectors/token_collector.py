"""
collectors/token_collector.py
------------------------------
Collects token creation events with full metadata.
Output: one Parquet per day with columns:
  block_time, block_height, creator, tx_signature,
  mint_address, name, symbol, uri, decimals, metadata_address, ...

Covers Research Axis 1 (taxonomy) and Axis 2 (creator network).
"""

import logging
from datetime import datetime
from pathlib import Path

from client import BitqueryClient
from config import PARQUET_DIR
from queries.tokens import TOKEN_CREATIONS, TOKENS_BY_CREATOR
from utils import records_to_df, save_parquet, time_windows

logger = logging.getLogger(__name__)


class TokenCollector:
    """Collects pump.fun token creation events from Bitquery."""

    def __init__(self, client: BitqueryClient):
        self.client = client
        self.output_dir = PARQUET_DIR / "tokens"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def collect_creations(
        self, start: datetime, end: datetime, window_minutes: int = 60
    ) -> Path:
        """
        Collect all token creations in [start, end).
        Paginates in hourly windows to stay within the 1000-row API limit.
        Returns path to the output Parquet file.
        """
        all_records = []
        for after, before in time_windows(start, end, window_minutes):
            logger.info(f"Fetching token creations: {after} → {before}")
            data = self.client.query(TOKEN_CREATIONS, {"after": after, "before": before})
            rows = data.get("Solana", {}).get("TokenSupplyUpdates", [])
            all_records.extend(rows)
            logger.info(f"  → {len(rows)} creations in window")

        df = records_to_df(all_records)
        if df.empty:
            logger.warning("No token creations found in range")
            return Path()

        # Rename columns to friendlier names
        col_map = {
            "Block.Time": "block_time",
            "Block.Height": "block_height",
            "Transaction.Signer": "creator",
            "Transaction.Signature": "tx_signature",
            "TokenSupplyUpdate.Currency.Symbol": "symbol",
            "TokenSupplyUpdate.Currency.Name": "name",
            "TokenSupplyUpdate.Currency.MintAddress": "mint_address",
            "TokenSupplyUpdate.Currency.MetadataAddress": "metadata_address",
            "TokenSupplyUpdate.Currency.Uri": "uri",
            "TokenSupplyUpdate.Currency.Decimals": "decimals",
            "TokenSupplyUpdate.Currency.Fungible": "fungible",
            "TokenSupplyUpdate.Currency.UpdateAuthority": "update_authority",
            "TokenSupplyUpdate.Amount": "initial_supply",
            "TokenSupplyUpdate.PostBalance": "post_balance",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        out_path = self.output_dir / f"creations_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.parquet"
        save_parquet(df, out_path)
        return out_path

    def collect_tokens_by_creator(self, creator_address: str) -> Path:
        """
        Collect all tokens created by a specific dev address.
        Used for dev clustering and prolificacy analysis.
        """
        logger.info(f"Fetching all tokens by creator: {creator_address[:12]}...")
        data = self.client.query(TOKENS_BY_CREATOR, {"creator": creator_address})
        rows = data.get("Solana", {}).get("TokenSupplyUpdates", [])
        df = records_to_df(rows)

        if df.empty:
            return Path()

        out_path = self.output_dir / f"by_creator_{creator_address[:16]}.parquet"
        save_parquet(df, out_path)
        return out_path
