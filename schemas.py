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

# ── Phase C: off-chain metadata + images ─────────────────────────────────────
# Populated by collect_metadata.py. See docs/metadata_image_collection_plan.md.

TOKEN_METADATA_DTYPES: dict = {
    "mint":             "str",    # PK, FK -> tokens.mint
    "uri":              "str",    # raw tokens.uri as collected on-chain
    "fetched_url":      "str",    # what we actually hit after CID re-routing
    "gateway":          "str",    # which gateway served it
    # Off-chain name/symbol can disagree with the on-chain CreateEvent values;
    # that disagreement is itself a taxonomy signal, so both are kept.
    "meta_name":        "str",
    "meta_symbol":      "str",
    "description":      "str",
    "image_url":        "str",
    "image_cid":        "str",    # extracted CID, NULL for non-IPFS hosts
    "twitter":          "str",
    "telegram":         "str",
    "website":          "str",
    "coin_community":   "str",
    "video_url":        "str",
    "banner_url":       "str",
    "show_name":        "bool",
    "created_on":       "str",
    "extra_json":       "str",    # unmodelled keys, verbatim JSON
    "status":           "str",    # ok|no_uri|unsupported_uri|http_error|timeout|invalid_json|all_gateways_failed
    "http_status":      "Int64",
    "attempts":         "Int64",
    "resolved_at":      "Int64",
}

TOKEN_IMAGE_DTYPES: dict = {
    "mint":             "str",    # PK, FK -> tokens.mint
    "image_url":        "str",
    "image_cid":        "str",
    "sha256":           "str",    # of STORED bytes — dedup key, joins to blob shards
    "shard":            "str",    # e.g. "shard_000123.parquet"
    "status":           "str",    # ok|no_url|http_error|timeout|decode_error|unsupported|too_large
    "http_status":      "Int64",
    "strategy":         "str",    # original_local | gateway_transform
    "orig_bytes":       "Int64",  # exact, from content-range/content-length
    # Original dimensions are parsed from the first few KB of the source bytes.
    # They are unrecoverable once the gateway transform path is used (it
    # resizes server-side and leaks no source size), which is why the default
    # strategy downloads originals — see the plan doc, section 2.
    "orig_width":       "Int64",
    "orig_height":      "Int64",
    "orig_format":      "str",
    # Animation is flattened to frame 0 when stored; the fact is retained here.
    "is_animated":      "bool",
    "n_frames":         "Int64",
    "stored_bytes":     "Int64",
    "stored_width":     "Int64",
    "stored_height":    "Int64",
    "fetched_at":       "Int64",
}

TOKEN_PUMPFUN_API_DTYPES: dict = {
    "mint":                     "str",    # PK, FK -> tokens.mint
    # nsfw/is_banned are MUTABLE moderation labels reflecting pump.fun's state
    # at `fetched_at`, not at token creation. Treat them as "label as of
    # fetched_at" in any downstream analysis.
    "nsfw":                     "bool",
    "is_banned":                "bool",
    "reply_count":              "Int64",
    "market_cap":               "float64",
    "ath_market_cap":           "float64",
    "ath_market_cap_timestamp": "Int64",
    "total_supply":             "Int64",
    "is_currently_live":        "bool",
    "hide_banner":              "bool",
    "api_image_uri":            "str",    # pump.fun's own resolved image (CDN mirror)
    "api_metadata_uri":         "str",
    "status":                   "str",    # ok|not_found|http_error|timeout
    "http_status":              "Int64",
    "fetched_at":               "Int64",
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
    # Phase C — written by collect_metadata.py, not by the decoder.
    "token_metadata":       (TOKEN_METADATA_DTYPES, ["mint"]),
    "token_images":         (TOKEN_IMAGE_DTYPES, ["mint"]),
    "token_pumpfun_api":    (TOKEN_PUMPFUN_API_DTYPES, ["mint"]),
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
