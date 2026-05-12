"""
config.py
---------
Central configuration. Copy .env.example to .env and fill in values.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Time window ───────────────────────────────────────────────────────────────
COLLECT_START: str = "2026-01-01"   # inclusive ISO date
COLLECT_END:   str = "2026-05-12"   # inclusive ISO date

# ── pump.fun REST API ─────────────────────────────────────────────────────────
PUMPFUN_API_BASE: str  = "https://frontend-api.pump.fun"
PUMPFUN_PAGE_SIZE: int = 50          # max coins per /coins request
TRADES_PAGE_SIZE: int  = 200         # max trades per /trades/all request

# ── Sampling thresholds ───────────────────────────────────────────────────────
# Non-graduated tokens with trade_count > ACTIVITY_THRESHOLD get full trade history.
# Those at or below only get creation row + final state snapshot.
ACTIVITY_THRESHOLD: int = 10

# ── HTTP client ───────────────────────────────────────────────────────────────
MAX_RETRIES: int        = 6
RETRY_BACKOFF: float    = 1.5   # exponential base (seconds)
REQUEST_TIMEOUT: int    = 30    # seconds per request
RATE_LIMIT_RPS: float   = 5.0   # max requests per second (conservative)

# ── Concurrency ───────────────────────────────────────────────────────────────
# Parallel workers for token processing (each worker fetches independently).
# 4–8 is a sweet spot for a single machine; increase carefully.
WORKERS: int = int(os.getenv("WORKERS", "4"))

# ── Storage ───────────────────────────────────────────────────────────────────
DATA_DIR:    Path = Path(os.getenv("DATA_DIR", "./data"))
RAW_DIR:     Path = DATA_DIR / "raw"
PARQUET_DIR: Path = DATA_DIR / "parquet"

for _d in [RAW_DIR, PARQUET_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ── Checkpoint ───────────────────────────────────────────────────────────────
CHECKPOINT_PATH: Path = DATA_DIR / "checkpoint.jsonl"
