"""
queries/trades.py
------------------
GraphQL queries for individual DEX trades on pump.fun bonding curve.
Used for: Axis 3 (IRL framework — every buy/sell with wallet, amount, price, slot).
"""

from config import PUMPFUN_PROGRAM

# ── 1. All trades for a specific token (paginated by time) ────────────────
# Core query for IRL: gives (wallet, action, amount, price, timestamp, slot)
TOKEN_TRADES = f"""
query TokenTrades($mint: String!, $after: DateTime!, $before: DateTime!) {{
  Solana {{
    DEXTrades(
      where: {{
        Trade: {{
          Dex: {{ ProgramAddress: {{ is: "{PUMPFUN_PROGRAM}" }} }}
          Currency: {{ MintAddress: {{ is: $mint }} }}
        }}
        Block: {{ Time: {{ after: $after, before: $before }} }}
        Transaction: {{ Result: {{ Success: true }} }}
      }}
      orderBy: {{ ascending: Block_Slot }}
      limit: {{ count: 1000 }}
    ) {{
      Block {{
        Time
        Slot
        Height
      }}
      Transaction {{
        Signature
        Signer
        Fee
      }}
      Trade {{
        Side {{
          Account {{ Address }}
          Type
          AmountInUSD
          Amount
        }}
        Currency {{
          MintAddress
          Symbol
          Name
        }}
        Price
        PriceInUSD
        Amount
        AmountInUSD
        Dex {{
          ProtocolName
          ProgramAddress
        }}
      }}
    }}
  }}
}}
"""

# ── 2. First N buyers of a token (sniper/bundle detection) ────────────────
FIRST_BUYERS = f"""
query FirstBuyers($mint: String!, $limit: Int!) {{
  Solana {{
    DEXTrades(
      where: {{
        Trade: {{
          Dex: {{ ProgramAddress: {{ is: "{PUMPFUN_PROGRAM}" }} }}
          Currency: {{ MintAddress: {{ is: $mint }} }}
          Side: {{ Type: {{ is: buy }} }}
        }}
        Transaction: {{ Result: {{ Success: true }} }}
      }}
      orderBy: {{ ascending: Block_Slot }}
      limit: {{ count: $limit }}
    ) {{
      Block {{
        Time
        Slot
        Height
      }}
      Transaction {{
        Signer
        Signature
      }}
      Trade {{
        Side {{
          Account {{ Address }}
          Amount
          AmountInUSD
        }}
        Price
        PriceInUSD
        Amount
        AmountInUSD
      }}
    }}
  }}
}}
"""

# ── 3. All trades by a specific wallet across all pump.fun tokens ─────────
# Used for: building wallet trajectory for IRL, profiling trader across tokens
WALLET_TRADES = f"""
query WalletTrades($wallet: String!, $after: DateTime!, $before: DateTime!) {{
  Solana {{
    DEXTrades(
      where: {{
        Trade: {{
          Dex: {{ ProgramAddress: {{ is: "{PUMPFUN_PROGRAM}" }} }}
          Side: {{ Account: {{ Address: {{ is: $wallet }} }} }}
        }}
        Block: {{ Time: {{ after: $after, before: $before }} }}
        Transaction: {{ Result: {{ Success: true }} }}
      }}
      orderBy: {{ ascending: Block_Slot }}
      limit: {{ count: 1000 }}
    ) {{
      Block {{
        Time
        Slot
      }}
      Transaction {{ Signer  Signature }}
      Trade {{
        Side {{
          Account {{ Address }}
          Type
          Amount
          AmountInUSD
        }}
        Currency {{
          MintAddress
          Symbol
        }}
        Price
        PriceInUSD
        Amount
      }}
    }}
  }}
}}
"""

# ── 4. OHLCV candles per minute for a token ───────────────────────────────
# Used for: feature engineering (price trajectory, velocity, drawdown)
OHLCV_1MIN = f"""
query OHLCV($mint: String!, $after: DateTime!, $before: DateTime!) {{
  Solana {{
    DEXTradeByTokens(
      where: {{
        Trade: {{
          Currency: {{ MintAddress: {{ is: $mint }} }}
          Dex: {{ ProgramAddress: {{ is: "{PUMPFUN_PROGRAM}" }} }}
        }}
        Block: {{ Time: {{ after: $after, before: $before }} }}
      }}
      orderBy: {{ ascending: Block_Time }}
    ) {{
      Block {{
        Time(interval: {{ in: minutes, count: 1 }})
      }}
      Trade {{
        open: Price(minimum: Block_Slot)
        high: Price(maximum: Trade_Price)
        low: Price(minimum: Trade_Price)
        close: Price(maximum: Block_Slot)
      }}
      volume: sum(of: Trade_Amount)
      volume_usd: sum(of: Trade_AmountInUSD)
      trade_count: count
      unique_traders: count(distinct: Trade_Side_Account_Address)
    }}
  }}
}}
"""

# ── 5. Trade stats: volume, buys, sells, makers ──────────────────────────
TOKEN_TRADE_STATS = f"""
query TradeStats($mint: String!, $after: DateTime!) {{
  Solana {{
    DEXTradeByTokens(
      where: {{
        Trade: {{
          Currency: {{ MintAddress: {{ is: $mint }} }}
          Dex: {{ ProgramAddress: {{ is: "{PUMPFUN_PROGRAM}" }} }}
        }}
        Block: {{ Time: {{ after: $after }} }}
      }}
    ) {{
      Trade {{
        Currency {{ MintAddress  Symbol }}
      }}
      buy_volume: sum(of: Trade_Side_AmountInUSD, selectWhere: {{ Trade: {{ Side: {{ Type: {{ is: buy }} }} }} }})
      sell_volume: sum(of: Trade_Side_AmountInUSD, selectWhere: {{ Trade: {{ Side: {{ Type: {{ is: sell }} }} }} }})
      total_trades: count
      buyers: count(distinct: Trade_Side_Account_Address, selectWhere: {{ Trade: {{ Side: {{ Type: {{ is: buy }} }} }} }})
      sellers: count(distinct: Trade_Side_Account_Address, selectWhere: {{ Trade: {{ Side: {{ Type: {{ is: sell }} }} }} }})
    }}
  }}
}}
"""
