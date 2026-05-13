"""
config.py
---------
Central configuration for the Solana RPC-based pump.fun crawler.
Copy .env.example to .env and fill in values.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Collection window ─────────────────────────────────────────────────────────
COLLECT_START: str = os.getenv("COLLECT_START", "2026-01-01")
COLLECT_END:   str = os.getenv("COLLECT_END",   "2026-05-12")

# ── Solana programs ───────────────────────────────────────────────────────────
PUMPFUN_PROGRAM:  str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMPSWAP_PROGRAM: str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

# ── Solana RPC endpoints ──────────────────────────────────────────────────────
# Add more endpoints to rotate between them and increase throughput.
# Helius free tier: https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
# QuickNode free:   https://YOUR_NAME.solana-mainnet.quiknode.pro/YOUR_KEY/
_raw_endpoints = os.getenv(
    "RPC_ENDPOINTS",
    "https://api.mainnet-beta.solana.com",
)
RPC_ENDPOINTS: list[str] = [e.strip() for e in _raw_endpoints.split(",") if e.strip()]

# ── RPC tuning ────────────────────────────────────────────────────────────────
RPC_TIMEOUT:       int   = 60     # seconds per request
MAX_RETRIES:       int   = 6
RETRY_BACKOFF:     float = 1.5    # exponential base (seconds)
SIGNATURES_LIMIT:  int   = 1000   # max sigs per getSignaturesForAddress call
TX_BATCH_SIZE:     int   = 25     # concurrent getTransaction workers (no batch arrays — Helius rejects them)
WORKERS:           int   = int(os.getenv("WORKERS", "4"))

# ── Storage ───────────────────────────────────────────────────────────────────
DATA_DIR:    Path = Path(os.getenv("DATA_DIR", "./data"))
RAW_DIR:     Path = DATA_DIR / "raw"
PARQUET_DIR: Path = DATA_DIR / "parquet"

for _d in [RAW_DIR, PARQUET_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ── Checkpoint ────────────────────────────────────────────────────────────────
CHECKPOINT_PATH: Path = DATA_DIR / "checkpoint.json"
