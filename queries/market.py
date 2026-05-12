"""
queries/market.py
------------------
GraphQL queries for market-level data: market cap, bonding curve, pairs info.
"""

# ── 1. Latest market cap for a specific token ─────────────────────────────
TOKEN_MARKETCAP = """
query TokenMarketCap($mint_id: String!) {
  Trading {
    Pairs(
      limit: { count: 1 }
      orderBy: { descending: Block_Time }
      where: {
        Token: { Id: { includesCaseInsensitive: $mint_id } }
        Interval: { Time: { Duration: { gt: 1 } } }
        Market: { ProtocolFamily: { is: "Pumpfun" } }
      }
    ) {
      Token { Name  Id  Address  Symbol }
      Block { Time }
      Supply {
        TotalSupply
        FullyDilutedValuationUsd
        MarketCap
      }
      Price {
        Average { Mean }
        Ohlc { Open  High  Low  Close }
      }
      Volume { Base  Usd }
    }
  }
}
"""

# ── 2. Stream tokens above a market cap threshold ─────────────────────────
TOKENS_ABOVE_MCAP = """
query TokensAboveMCap($min_mcap: Float!, $limit: Int!) {
  Trading {
    Pairs(
      limit: { count: $limit }
      orderBy: { descending: Supply_MarketCap }
      where: {
        Token: { Id: { includesCaseInsensitive: "solana" } }
        Interval: { Time: { Duration: { gt: 1 } } }
        Supply: { MarketCap: { gt: $min_mcap } }
        Market: { ProtocolFamily: { is: "Pumpfun" } }
      }
    ) {
      Token { Name  Symbol  Id  Address }
      Supply { MarketCap  TotalSupply  FullyDilutedValuationUsd }
      Price { Ohlc { Open  Close } }
      Volume { Usd }
    }
  }
}
"""

# ── 3. Top tokens by market cap change in last hour ───────────────────────
TOP_BY_MCAP_CHANGE = """
query TopByMcapChange($limit: Int!) {
  Trading {
    Pairs(
      limit: { count: $limit }
      orderBy: { descendingByField: "change_mcap" }
      where: {
        Market: { ProtocolFamily: { is: "Pumpfun" } }
        Interval: { Time: { Duration: { eq: 3600 } } }
        Token: { Network: { is: "Solana" } }
      }
    ) {
      Token { Id  Name  Symbol  Address }
      Supply { MarketCap  TotalSupply }
      change_mcap: calculate(
        expression: "($Price_Ohlc_Close-$Price_Ohlc_Open) * Supply_TotalSupply"
      )
      Price { Ohlc { Open  Close } }
    }
  }
}
"""
