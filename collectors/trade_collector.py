"""
collectors/trade_collector.py
------------------------------
Collects individual DEX trades on the pump.fun bonding curve.
This is the core data for Axis 3 (IRL trader profiling) and P&D detection.

Output columns per trade:
  block_time, block_slot, block_height, tx_signature, tx_signer, tx_fee,
  trader_address, trade_type (buy/sell), token_mint, token_symbol,
  price, price_usd, amount, amount_usd, dex_protocol
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from client import BitqueryClient
from config import FIRST_BUYERS_LIMIT, PARQUET_DIR
from queries.trades import (FIRST_BUYERS, OHLCV_1MIN, TOKEN_TRADES,
                            WALLET_TRADES)
from utils import records_to_df, save_parquet, time_windows

logger = logging.getLogger(__name__)


class TradeCollector:
    """Collects pump.fun DEX trades from Bitquery."""

    def __init__(self, client: BitqueryClient):
        self.client = client
        self.output_dir = PARQUET_DIR / "trades"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def collect_token_trades(
        self,
        mint: str,
        start: datetime,
        end: datetime,
        window_minutes: int = 30,
    ) -> Path:
        """
        Collect ALL trades for a specific token in [start, end).
        Uses tighter windows (30 min) because popular tokens can have
        thousands of trades per hour.
        """
        all_records = []
        for after, before in time_windows(start, end, window_minutes):
            data = self.client.query(TOKEN_TRADES, {
                "mint": mint, "after": after, "before": before
            })
            rows = data.get("Solana", {}).get("DEXTrades", [])
            all_records.extend(rows)
            if rows:
                logger.debug(f"  {mint[:12]}: {len(rows)} trades in {after}")

        df = records_to_df(all_records)
        if df.empty:
            return Path()

        # Flatten and rename key columns
        col_map = {
            "Block.Time": "block_time",
            "Block.Slot": "block_slot",
            "Block.Height": "block_height",
            "Transaction.Signature": "tx_signature",
            "Transaction.Signer": "tx_signer",
            "Transaction.Fee": "tx_fee",
            "Trade.Side.Account.Address": "trader_address",
            "Trade.Side.Type": "trade_type",
            "Trade.Side.Amount": "side_amount",
            "Trade.Side.AmountInUSD": "side_amount_usd",
            "Trade.Currency.MintAddress": "token_mint",
            "Trade.Currency.Symbol": "token_symbol",
            "Trade.Currency.Name": "token_name",
            "Trade.Price": "price",
            "Trade.PriceInUSD": "price_usd",
            "Trade.Amount": "amount",
            "Trade.AmountInUSD": "amount_usd",
            "Trade.Dex.ProtocolName": "dex_protocol",
            "Trade.Dex.ProgramAddress": "dex_program",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        out_path = self.output_dir / f"trades_{mint[:16]}_{start.strftime('%Y%m%d')}.parquet"
        save_parquet(df, out_path)
        logger.info(f"Collected {len(df)} trades for {mint[:12]}...")
        return out_path

    def collect_first_buyers(self, mint: str, limit: int = FIRST_BUYERS_LIMIT) -> Path:
        """
        Collect the first N buyers of a token (ordered by slot).
        Critical for sniper/bundle detection.
        """
        logger.info(f"Fetching first {limit} buyers for {mint[:12]}...")
        data = self.client.query(FIRST_BUYERS, {"mint": mint, "limit": limit})
        rows = data.get("Solana", {}).get("DEXTrades", [])
        df = records_to_df(rows)

        if df.empty:
            return Path()

        out_path = self.output_dir / f"first_buyers_{mint[:16]}.parquet"
        save_parquet(df, out_path)
        return out_path

    def collect_wallet_trades(
        self,
        wallet: str,
        start: datetime,
        end: datetime,
        window_minutes: int = 60,
    ) -> Path:
        """
        Collect all pump.fun trades by a specific wallet across ALL tokens.
        Used for: building per-wallet trajectory for IRL, cross-token profiling.
        """
        all_records = []
        for after, before in time_windows(start, end, window_minutes):
            data = self.client.query(WALLET_TRADES, {
                "wallet": wallet, "after": after, "before": before
            })
            rows = data.get("Solana", {}).get("DEXTrades", [])
            all_records.extend(rows)

        df = records_to_df(all_records)
        if df.empty:
            return Path()

        out_path = self.output_dir / f"wallet_{wallet[:16]}_{start.strftime('%Y%m%d')}.parquet"
        save_parquet(df, out_path)
        logger.info(f"Collected {len(df)} trades for wallet {wallet[:12]}...")
        return out_path

    def collect_ohlcv(
        self, mint: str, start: datetime, end: datetime
    ) -> Path:
        """
        Collect 1-minute OHLCV candles for a token.
        Used for: feature engineering, price trajectory, P&D signal detection.
        """
        data = self.client.query(OHLCV_1MIN, {
            "mint": mint,
            "after": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "before": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        rows = data.get("Solana", {}).get("DEXTradeByTokens", [])
        df = records_to_df(rows)

        if df.empty:
            return Path()

        out_path = self.output_dir / f"ohlcv_{mint[:16]}_{start.strftime('%Y%m%d')}.parquet"
        save_parquet(df, out_path)
        return out_path
