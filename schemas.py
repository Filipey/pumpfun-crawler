"""
schemas.py
----------
Column definitions matching the paper's dataset schema (Section IV).

Reference:
  Marino et al. (2026), "Predicting the success of new crypto-tokens: the Pump.fun case"
  arXiv:2602.14860 — Table on page 7-8.
"""

import pandas as pd

# One row per on-chain event (create / buy / sell / complete).
# Matches the paper's per-transaction record structure.
TX_DTYPES: dict = {
    # ── Identity ──────────────────────────────────────────────────────────────
    "signature":    "str",    # unique transaction hash
    "slot":         "Int64",  # Solana slot number
    "timestamp":    "Int64",  # unix seconds (blockchain time)
    "date":         "str",    # YYYY-MM-DD (derived, for partitioning)

    # ── Token ─────────────────────────────────────────────────────────────────
    "mint":         "str",    # token mint address
    "coin_creator": "str",    # creator wallet (from CreateEvent; NULL in trades)
    "bonding_curve":"str",    # bonding curve PDA address

    # ── Participants ──────────────────────────────────────────────────────────
    "trader":       "str",    # wallet executing the trade (user field in events)

    # ── Transaction type ──────────────────────────────────────────────────────
    "tx_type":      "str",    # "create" | "buy" | "sell" | "complete"
    "is_buy":       "bool",   # True for buy, False for sell, None for create/complete

    # ── Bonding curve state AFTER the transaction ─────────────────────────────
    "in_bonding_curve":          "bool",   # always True for pump.fun launchpad events
    "v_sol_in_bonding_curve":    "Int64",  # virtual SOL reserves (lamports)
    "v_tok_in_bonding_curve":    "Int64",  # virtual token reserves (raw units)

    # ── Trade amounts ─────────────────────────────────────────────────────────
    "sol_amount":   "Int64",  # SOL exchanged (lamports)
    "token_amount": "Int64",  # tokens exchanged (raw units)

    # ── Bot detection (paper Section IV) ──────────────────────────────────────
    # True  = direct contract invocation (bypasses frontend) → likely bot
    # False = invoked through pump.fun dApp interface → likely human
    # None  = could not determine
    "is_bot":       "bool",

    # ── Data source ───────────────────────────────────────────────────────────
    # "pumpfun"   = bonding curve (launchpad) event
    # "pumpswap"  = PumpSwap AMM event (post-graduation)
    "source":       "str",

    # ── PumpSwap-specific ─────────────────────────────────────────────────────
    "pool":             "str",    # PumpSwap pool address (NULL for bonding curve)
    "lp_fee":           "Int64",  # LP fee paid (lamports; NULL for bonding curve)
    "protocol_fee":     "Int64",  # protocol fee paid (lamports)
    "coin_creator_fee": "Int64",  # creator revenue share (lamports)
}


# ── Per-entity tables (bulk-source redesign) ─────────────────────────────────
#
# Replaces the single flat TX_DTYPES table above. Normalized so bonding-curve
# and PumpSwap-only columns don't sit NULL on every row of the other kind.
# TX_DTYPES/apply_dtypes are kept as-is for reading any already-collected
# legacy files from the RPC-based crawler.

TOKEN_DTYPES: dict = {
    "mint":             "str",    # PK
    "coin_creator":     "str",    # wallet entitled to creator fee revenue
    "bonding_curve":    "str",
    "name":             "str",
    "symbol":           "str",
    "uri":              "str",
    "created_slot":     "Int64",
    "created_at":       "Int64",  # unix seconds
    "created_signature":"str",
}

TOKEN_METADATA_DTYPES: dict = {
    "mint":             "str",    # FK -> tokens.mint
    "image_url":        "str",
    "description":      "str",
    "twitter":          "str",
    "telegram":         "str",
    "website":          "str",
    "resolved_status":  "str",    # ok | not_found | timeout | invalid_json | pending
    "resolved_at":      "Int64",
}

POOL_DTYPES: dict = {
    "pool":             "str",    # PK
    "mint":             "str",    # FK -> tokens.mint
    "creator":          "str",    # coin_creator at time of pool creation
    "created_slot":     "Int64",
    "created_at":       "Int64",
    "created_signature":"str",
}

GRADUATION_DTYPES: dict = {
    "mint":             "str",
    "signature":        "str",
    "slot":             "Int64",
    "block_time":       "Int64",
}

_TRADE_COMMON_DTYPES: dict = {
    "signature":                "str",
    "slot":                     "Int64",
    "block_time":               "Int64",
    "mint":                     "str",
    "trader":                   "str",
    "tx_type":                  "str",    # "buy" | "sell"
    "is_buy":                   "bool",
    "v_sol_in_bonding_curve":   "Int64",
    "v_tok_in_bonding_curve":   "Int64",
    "sol_amount":               "Int64",
    "token_amount":             "Int64",
    "is_bot":                   "bool",
    "coin_creator":             "str",
    "coin_creator_fee":         "Int64",
    "source_phase":             "str",    # which ingestion phase produced this row (solarchive|oldfaithful)
}

BONDING_CURVE_TRADE_DTYPES: dict = dict(_TRADE_COMMON_DTYPES)

PUMPSWAP_TRADE_DTYPES: dict = {
    **_TRADE_COMMON_DTYPES,
    "pool":         "str",
    "lp_fee":       "Int64",
    "protocol_fee": "Int64",
}

# table name -> (dtype dict, dedup key columns)
ENTITY_TABLES: dict = {
    "tokens":               (TOKEN_DTYPES, ["mint"]),
    "pools":                (POOL_DTYPES, ["pool"]),
    "graduations":          (GRADUATION_DTYPES, ["signature"]),
    "bonding_curve_trades": (BONDING_CURVE_TRADE_DTYPES, ["signature", "tx_type"]),
    "pumpswap_trades":      (PUMPSWAP_TRADE_DTYPES, ["signature", "tx_type"]),
}


def apply_dtypes(df: pd.DataFrame, dtype_map: dict) -> pd.DataFrame:
    """Cast DataFrame columns to the specified dtypes; silently skip missing cols."""
    for col, dtype in dtype_map.items():
        if col not in df.columns:
            continue
        try:
            if dtype == "bool":
                df[col] = df[col].where(df[col].notna(), None)
                non_null = df[col].notna()
                df.loc[non_null, col] = df.loc[non_null, col].astype(bool)
            elif dtype == "str":
                df[col] = df[col].where(df[col].notna(), None).astype("object")
            else:
                df[col] = df[col].astype(dtype)
        except (ValueError, TypeError):
            pass
    return df
