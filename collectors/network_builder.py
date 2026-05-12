"""
collectors/network_builder.py
------------------------------
Builds the creator ↔ trader influence network from collected Parquet data.
Used for Research Axis 2: understanding user influence in pump.fun.

Produces:
  - Creator → Token bipartite edges (who created what)
  - Trader → Token bipartite edges (who traded what, with volume)
  - Creator ↔ Trader projection (shared token co-occurrence)
  - Wallet co-occurrence matrix (wallets that trade the same tokens)
"""

import logging
from collections import defaultdict
from pathlib import Path

import networkx as nx
import pandas as pd

from config import PARQUET_DIR
from utils import load_parquet

logger = logging.getLogger(__name__)


class NetworkBuilder:
    """
    Builds network graphs from already-collected token and trade Parquets.
    Call after TokenCollector and TradeCollector have finished.
    """

    def __init__(self):
        self.token_dir = PARQUET_DIR / "tokens"
        self.trade_dir = PARQUET_DIR / "trades"
        self.output_dir = PARQUET_DIR / "network"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _load_all_parquets(self, directory: Path, prefix: str = "") -> pd.DataFrame:
        """Load and concatenate all Parquet files in a directory."""
        files = sorted(directory.glob(f"{prefix}*.parquet"))
        if not files:
            return pd.DataFrame()
        dfs = [pd.read_parquet(f) for f in files]
        return pd.concat(dfs, ignore_index=True)

    def build_creator_token_edges(self) -> pd.DataFrame:
        """
        Extract creator → token edges from token creation data.
        Returns DataFrame with columns: [creator, mint_address, name, symbol, block_time]
        """
        df = self._load_all_parquets(self.token_dir, prefix="creations_")
        if df.empty:
            logger.warning("No token creation data found")
            return df

        cols = ["creator", "mint_address", "name", "symbol", "block_time"]
        available = [c for c in cols if c in df.columns]
        edges = df[available].drop_duplicates(subset=["creator", "mint_address"])

        out = self.output_dir / "creator_token_edges.parquet"
        edges.to_parquet(out, index=False)
        logger.info(f"Built {len(edges)} creator→token edges")
        return edges

    def build_trader_token_edges(self) -> pd.DataFrame:
        """
        Extract trader → token edges from trade data.
        Aggregates: total_buys, total_sells, total_volume_usd, first_trade_time,
                    last_trade_time, trade_count per (trader, token) pair.
        """
        df = self._load_all_parquets(self.trade_dir, prefix="trades_")
        if df.empty:
            logger.warning("No trade data found")
            return df

        # Determine which column names are available
        trader_col = "trader_address" if "trader_address" in df.columns else "Trade.Side.Account.Address"
        mint_col = "token_mint" if "token_mint" in df.columns else "Trade.Currency.MintAddress"
        type_col = "trade_type" if "trade_type" in df.columns else "Trade.Side.Type"
        amount_col = "amount_usd" if "amount_usd" in df.columns else "Trade.AmountInUSD"
        time_col = "block_time" if "block_time" in df.columns else "Block.Time"

        # Ensure numeric
        if amount_col in df.columns:
            df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)

        edges = (
            df.groupby([trader_col, mint_col])
            .agg(
                trade_count=(time_col, "count"),
                first_trade=(time_col, "min"),
                last_trade=(time_col, "max"),
                total_volume_usd=(amount_col, "sum") if amount_col in df.columns else (time_col, "count"),
            )
            .reset_index()
            .rename(columns={trader_col: "trader", mint_col: "token_mint"})
        )

        out = self.output_dir / "trader_token_edges.parquet"
        edges.to_parquet(out, index=False)
        logger.info(f"Built {len(edges)} trader→token edges")
        return edges

    def build_wallet_cooccurrence(self, min_shared_tokens: int = 2) -> pd.DataFrame:
        """
        Build wallet co-occurrence: pairs of wallets that traded the same tokens.
        Filtered by minimum number of shared tokens (to reduce noise).
        Used for: detecting coordinated wallets (bundles, insider rings).
        """
        trader_edges = load_parquet(self.output_dir / "trader_token_edges.parquet")
        if trader_edges.empty:
            trader_edges = self.build_trader_token_edges()
        if trader_edges.empty:
            return pd.DataFrame()

        # Build token → set of traders mapping
        token_traders = (
            trader_edges.groupby("token_mint")["trader"]
            .apply(set)
            .to_dict()
        )

        # Count co-occurrences
        cooccurrence = defaultdict(int)
        for token, traders in token_traders.items():
            traders = sorted(traders)
            for i, w1 in enumerate(traders):
                for w2 in traders[i+1:]:
                    cooccurrence[(w1, w2)] += 1

        # Filter
        pairs = [
            {"wallet_a": k[0], "wallet_b": k[1], "shared_tokens": v}
            for k, v in cooccurrence.items()
            if v >= min_shared_tokens
        ]
        df = pd.DataFrame(pairs)

        if not df.empty:
            out = self.output_dir / "wallet_cooccurrence.parquet"
            df.to_parquet(out, index=False)
            logger.info(f"Built {len(df)} co-occurrence pairs (min shared={min_shared_tokens})")

        return df

    def build_networkx_graph(self) -> nx.Graph:
        """
        Build a full NetworkX bipartite graph:
          - Node types: 'creator', 'trader', 'token'
          - Edge types: 'created', 'traded'
        Returns the graph for further analysis (centrality, community detection, etc.)
        """
        G = nx.Graph()

        # Creator → Token
        creator_edges = load_parquet(self.output_dir / "creator_token_edges.parquet")
        if not creator_edges.empty:
            for _, row in creator_edges.iterrows():
                creator = row.get("creator", "")
                mint = row.get("mint_address", "")
                if creator and mint:
                    G.add_node(creator, bipartite="wallet", role="creator")
                    G.add_node(mint, bipartite="token", name=row.get("name", ""))
                    G.add_edge(creator, mint, relation="created")

        # Trader → Token
        trader_edges = load_parquet(self.output_dir / "trader_token_edges.parquet")
        if not trader_edges.empty:
            for _, row in trader_edges.iterrows():
                trader = row.get("trader", "")
                mint = row.get("token_mint", "")
                if trader and mint:
                    if trader not in G:
                        G.add_node(trader, bipartite="wallet", role="trader")
                    if mint not in G:
                        G.add_node(mint, bipartite="token")
                    G.add_edge(
                        trader, mint,
                        relation="traded",
                        trade_count=row.get("trade_count", 0),
                        volume_usd=row.get("total_volume_usd", 0),
                    )

        logger.info(
            f"NetworkX graph: {G.number_of_nodes()} nodes, "
            f"{G.number_of_edges()} edges"
        )
        return G
