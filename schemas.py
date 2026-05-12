"""
pumpfun_collector/schemas.py
------------------------------
Unified output column schemas for all data types.
Both Dune and Bitquery collectors MUST output DataFrames conforming
to these schemas, so downstream analysis is source-agnostic.

Each schema is a dict mapping column_name → dtype (for Pandas).
"""

# ── Token creations ───────────────────────────────────────────────────────
TOKEN_CREATION_COLS = {
    "block_time":        "str",     # ISO datetime
    "block_slot":        "Int64",
    "creator":           "str",     # wallet that signed the create tx
    "tx_signature":      "str",
    "mint_address":      "str",     # unique token identifier
    "name":              "str",
    "symbol":            "str",
    "uri":               "str",     # metadata URI (IPFS/Arweave)
    "decimals":          "Int64",
    "source":            "str",     # 'dune' or 'bitquery'
}

# ── Individual trades ─────────────────────────────────────────────────────
TRADE_COLS = {
    "block_time":        "str",
    "block_slot":        "Int64",
    "tx_signature":      "str",
    "trader_address":    "str",     # wallet executing the trade
    "trade_type":        "str",     # 'buy' or 'sell'
    "token_mint":        "str",
    "token_symbol":      "str",
    "amount_token":      "float64", # amount of memecoin traded
    "amount_sol":        "float64", # SOL side of the trade
    "amount_usd":        "float64",
    "price_usd":         "float64", # per-token price at time of trade
    "source":            "str",
}

# ── First buyers (subset of trades, ordered by slot) ──────────────────────
FIRST_BUYERS_COLS = TRADE_COLS  # same schema, just fewer rows

# ── Migrations (graduation events) ───────────────────────────────────────
MIGRATION_COLS = {
    "block_time":        "str",
    "block_slot":        "Int64",
    "tx_signature":      "str",
    "token_mint":        "str",     # the token that graduated
    "creator":           "str",     # tx signer (usually the dev or protocol)
    "source":            "str",
}

# ── OHLCV (1-minute candles) ──────────────────────────────────────────────
OHLCV_COLS = {
    "block_time":        "str",     # start of candle window
    "token_mint":        "str",
    "open":              "float64",
    "high":              "float64",
    "low":               "float64",
    "close":             "float64",
    "volume_token":      "float64",
    "volume_usd":        "float64",
    "trade_count":       "Int64",
    "unique_traders":    "Int64",
    "source":            "str",
}

# ── Holder snapshots ──────────────────────────────────────────────────────
HOLDER_COLS = {
    "snapshot_time":     "str",
    "token_mint":        "str",
    "holder_address":    "str",
    "balance":           "float64",
    "source":            "str",
}

# ── Wallet trade history (cross-token) ────────────────────────────────────
WALLET_TRADE_COLS = TRADE_COLS  # same as individual trades

# ── Bulk raw trades (ALL_TRADES_SQL output — direction not decoded) ───────────
# Used by collect_dune_auto.py for day-at-a-time bulk collection.
# Each row is a dex_solana.trades row before buy/sell normalization.
# To convert to TRADE_COLS format downstream, filter by SOL_MINT on each side.
RAW_TRADE_COLS = {
    "block_time":                "str",
    "block_slot":                "Int64",
    "tx_signature":              "str",
    "trader_address":            "str",
    "token_bought_mint_address": "str",
    "token_sold_mint_address":   "str",
    "token_bought_symbol":       "str",
    "token_sold_symbol":         "str",
    "token_bought_amount":       "float64",
    "token_sold_amount":         "float64",
    "amount_usd":                "float64",
    "source":                    "str",
}


def coerce_schema(df, schema: dict):
    """
    Ensure DataFrame has exactly the columns in schema, in order.
    Adds missing columns as None, drops extra columns, casts types.
    """
    import pandas as pd
    for col, dtype in schema.items():
        if col not in df.columns:
            df[col] = None
        try:
            df[col] = df[col].astype(dtype)
        except (ValueError, TypeError):
            pass  # keep as-is if cast fails
    return df[list(schema.keys())]
