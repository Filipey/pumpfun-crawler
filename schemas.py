"""
schemas.py
----------
Column definitions and dtype maps for all Parquet outputs.
"""

import pandas as pd

# ── Token record (one row per token) ─────────────────────────────────────────
TOKEN_DTYPES: dict = {
    "mint":                    "str",
    "name":                    "str",
    "symbol":                  "str",
    "description":             "str",
    "image_uri":               "str",
    "metadata_uri":            "str",
    "creator":                 "str",
    "created_timestamp":       "Int64",   # unix ms
    "complete":                "bool",    # graduated to Raydium?
    "raydium_pool":            "str",     # pool address if graduated
    "bonding_curve":           "str",
    "associated_bonding_curve": "str",
    "usd_market_cap":          "float64",
    "virtual_sol_reserves":    "Int64",
    "virtual_token_reserves":  "Int64",
    "real_sol_reserves":       "Int64",
    "real_token_reserves":     "Int64",
    "total_supply":            "Int64",
    "reply_count":             "Int64",
    "last_reply":              "Int64",   # unix ms
    "nsfw":                    "bool",
    "market_id":               "str",
    "inverted":                "bool",
    "is_currently_live":       "bool",
    "king_of_the_hill_timestamp": "Int64",
    "trade_count":             "Int64",   # derived during collection
    "category":                "str",     # "graduated" | "active" | "dead"
    "collected_at":            "Int64",   # unix ms when we fetched it
}

# ── Trade record (one row per trade) ─────────────────────────────────────────
TRADE_DTYPES: dict = {
    "signature":     "str",
    "mint":          "str",
    "sol_amount":    "Int64",   # lamports
    "token_amount":  "Int64",   # raw token units
    "is_buy":        "bool",
    "user":          "str",     # wallet address
    "timestamp":     "Int64",   # unix seconds
    "tx_index":      "Int64",
    "username":      "str",     # optional social name
    "profile_image": "str",
}


def apply_dtypes(df: pd.DataFrame, dtype_map: dict) -> pd.DataFrame:
    """Cast DataFrame columns to the specified dtypes, ignoring missing cols."""
    for col, dtype in dtype_map.items():
        if col not in df.columns:
            continue
        try:
            if dtype == "bool":
                df[col] = df[col].fillna(False).astype(bool)
            elif dtype == "str":
                df[col] = df[col].where(df[col].notna(), None).astype("object")
            else:
                df[col] = df[col].astype(dtype)
        except (ValueError, TypeError):
            pass
    return df
