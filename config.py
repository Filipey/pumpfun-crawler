"""
pumpfun_collector/config.py
---------------------------
Central configuration: API keys, endpoints, program addresses, collection params.
Copy `.env.example` to `.env` and fill in your Bitquery API token.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Dune Analytics (retroactive bulk collection — FREE) ───────────────────
DUNE_API_KEY: str = os.getenv("DUNE_API_KEY", "YOUR_DUNE_API_KEY_HERE")

# Pre-saved query IDs (create once in Dune IDE, then paste the IDs here).
# Free-tier API can only execute existing queries; it cannot create new ones.
# Run: python collect_dune_auto.py --setup  for step-by-step instructions.
DUNE_QUERY_CREATIONS:  int = int(os.getenv("DUNE_QUERY_CREATIONS",  "0"))
DUNE_QUERY_TRADES:     int = int(os.getenv("DUNE_QUERY_TRADES",     "0"))
DUNE_QUERY_MIGRATIONS: int = int(os.getenv("DUNE_QUERY_MIGRATIONS", "0"))

# ── Bitquery API (continuous daily collection — FREE) ─────────────────────
BITQUERY_TOKEN: str = os.getenv("BITQUERY_TOKEN", "YOUR_BITQUERY_TOKEN_HERE")
BITQUERY_ENDPOINT: str = "https://streaming.bitquery.io/graphql"
BITQUERY_WS: str = "wss://streaming.bitquery.io/graphql"

# ── Solana / pump.fun constants ───────────────────────────────────────────
PUMPFUN_PROGRAM: str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMPSWAP_PROGRAM: str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
SOL_MINT: str = "So11111111111111111111111111111111111111112"
TOKEN_TOTAL_SUPPLY: int = 1_000_000_000
TOKEN_CURVE_SUPPLY: int = 800_000_000
TOKEN_DECIMALS: int = 6

# ── Collection parameters ─────────────────────────────────────────────────
PAGE_SIZE: int = 1000              # max rows per Bitquery query
RATE_LIMIT_CALLS: int = 8          # calls per period
RATE_LIMIT_PERIOD: int = 60        # period in seconds
MAX_RETRIES: int = 5               # exponential backoff retries
FIRST_BUYERS_LIMIT: int = 100      # first N buyers to capture per token
TOP_HOLDERS_LIMIT: int = 50        # top holders snapshot

# ── Time windows for hourly pagination ────────────────────────────────────
PAGINATION_WINDOW_MINUTES: int = 60  # split queries into 1-hour windows

# ── Storage ───────────────────────────────────────────────────────────────
DATA_DIR: Path = Path(os.getenv("DATA_DIR", "./data"))
RAW_DIR: Path = DATA_DIR / "raw"
PARQUET_DIR: Path = DATA_DIR / "parquet"

# Ensure dirs exist
for d in [RAW_DIR, PARQUET_DIR]:
    d.mkdir(parents=True, exist_ok=True)
