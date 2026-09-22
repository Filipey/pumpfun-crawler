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
WAREHOUSE_DB_PATH: Path = DATA_DIR / "warehouse.duckdb"

for _d in [RAW_DIR, PARQUET_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ── Checkpoint ────────────────────────────────────────────────────────────────
CHECKPOINT_PATH: Path = DATA_DIR / "checkpoint.json"

# ── Bulk-source backfill window (Phase A/B redesign) ──────────────────────────
SOLARCHIVE_REPO:  str = "solarchive/solarchive"
SOLARCHIVE_START: str = os.getenv("SOLARCHIVE_START", "2025-11-01")
SOLARCHIVE_END:   str = os.getenv("SOLARCHIVE_END",   "2025-12-05")   # last day SolArchive has published
SOLARCHIVE_WORKERS: int = int(os.getenv("SOLARCHIVE_WORKERS", "4"))   # concurrent shard fetches

# ── Phase B: Old Faithful / Jetstreamer ───────────────────────────────────────
OLDFAITHFUL_START: str = os.getenv("OLDFAITHFUL_START", "2025-12-06")  # day after SolArchive's coverage ends
OLDFAITHFUL_END:   str = os.getenv("OLDFAITHFUL_END",   "2026-07-04")  # extend + rerun later for the rest of July
# Path to the compiled pumpfun_filter_plugin binary (must be built first with
# `cargo build --release` inside pumpfun_filter_plugin/ — see that crate's
# README/module docstring; not compiled/verified in this environment).
JETSTREAMER_BIN: Path = Path(
    os.getenv("JETSTREAMER_BIN", "./pumpfun_filter_plugin/target/release/pumpfun-filter-plugin")
)
OLDFAITHFUL_OUT_DIR: Path = DATA_DIR / "oldfaithful_raw"  # transient plugin JSONL output, deleted per epoch after ingest

# ── Phase C: off-chain metadata + images ──────────────────────────────────────
# Resolves tokens.uri -> metadata JSON -> image, for multimodal taxonomy work.
# See docs/metadata_image_collection_plan.md for the measurements behind these
# defaults.

# `tokens.uri` is a CID carrier, not a usable URL: ~70% of rows point at
# ipfs.io, which now returns HTTP 429 for everything ("switching to a service
# worker gateway only"). Extract the CID and re-route through this pool.
# Ordered by preference; pump.fun's own Pinata gateway first, then fallbacks
# that were each measured to serve CIDs pump.mypinata.cloud 403s (CIDs pinned
# by third-party launchpads, not in pump.fun's own Pinata account).
METADATA_GATEWAYS: list[str] = [
    g.strip() for g in os.getenv(
        "METADATA_GATEWAYS",
        "https://pump.mypinata.cloud/ipfs/,"
        "https://ipfs.filebase.io/ipfs/,"
        "https://ipfs.raribleuserdata.com/ipfs/,"
        "https://gateway.pinata.cloud/ipfs/",
    ).split(",") if g.strip()
]

# Hosts that are dead or redirect-only — never fetched directly, always
# re-routed via METADATA_GATEWAYS using the CID embedded in the path.
DEAD_IPFS_HOSTS: set[str] = {
    "ipfs.io", "dweb.link", "cloudflare-ipfs.com", "cf-ipfs.com",
    "w3s.link", "nftstorage.link", "4everland.io", "flk-ipfs.xyz",
}

# ── Image pipeline ────────────────────────────────────────────────────────────
# Default strategy is "original": fetch the source image once and resize
# locally. Measured faster than the server-side gateway transform (26.7 vs
# 19.6 tok/s — one round trip beats two), 21% smaller on disk, and it keeps the
# original dimensions, which the gateway transform destroys (it resizes before
# we see any bytes, and leaks no source size in the response headers).
# "transform" trades ~1.5 days and ~40GB for ~10x less download volume.
IMAGE_STRATEGY: str = os.getenv("IMAGE_STRATEGY", "original")   # original | transform

IMG_TARGET_PX:    int = int(os.getenv("IMG_TARGET_PX", "384"))   # max side; 384 keeps SigLIP-384/Qwen2-VL open
IMG_WEBP_QUALITY: int = int(os.getenv("IMG_WEBP_QUALITY", "85"))

# Cloudflare Images params on pump.mypinata.cloud. Every one of these matters:
# without img-anim=false an animated GIF comes back as a 1.5MB animated WebP
# (vs 30KB flattened); without img-height + scale-down, img-width alone leaves
# the other axis unbounded (tall images came back 384x477).
IMG_TRANSFORM_PARAMS: str = os.getenv(
    "IMG_TRANSFORM_PARAMS",
    "img-width={px}&img-height={px}&img-fit=scale-down&img-format=webp&img-anim=false",
)

# Above this, don't pull the whole original: read only the header prefix for
# dimensions, abort the stream, and use the gateway transform instead.
# p99 of observed originals is ~2MB, so this affects <1% of images.
IMG_MAX_ORIG_BYTES: int = int(os.getenv("IMG_MAX_ORIG_BYTES", str(4 * 1024 * 1024)))

# Bytes of header needed to parse dimensions without decoding. Validated at
# 99/99 correct across PNG/JPEG/GIF/WebP/AVIF at 4096; smaller prefixes miss
# JPEGs (they never answer *wrong*, just "can't parse yet").
IMG_HEADER_PREFIX: int = int(os.getenv("IMG_HEADER_PREFIX", "4096"))

# Rows per image blob shard. ~10k x ~17.6KB ≈ 176MB/shard.
IMG_SHARD_ROWS: int = int(os.getenv("IMG_SHARD_ROWS", "10000"))

# Image blob shards land here. Separate from DATA_DIR on purpose — this is the
# only component measured in hundreds of GB (~145GB at ~9.3M tokens), so it
# often belongs on a different disk than the rest of data/.
IMAGES_DIR: Path = Path(os.getenv("IMAGES_DIR", str(DATA_DIR / "images")))

# ── pump.fun frontend API (moderation flags) ──────────────────────────────────
# Sole source of nsfw / is_banned, which are mutable moderation labels read at
# fetch time (NOT creation time) — token_pumpfun_api.fetched_at records when.
# There is no batch endpoint: /coins/multi|list|mints all 404, and
# GET /coins?mints=... returns 200 while silently ignoring the parameter. So
# this is one request per mint.
PUMP_API_COIN: str = os.getenv("PUMP_API_COIN", "https://frontend-api-v3.pump.fun/coins/")

# ── Phase C tuning ────────────────────────────────────────────────────────────
METADATA_WORKERS: int = int(os.getenv("METADATA_WORKERS", "32"))
IMAGE_WORKERS:    int = int(os.getenv("IMAGE_WORKERS", "32"))
API_WORKERS:      int = int(os.getenv("API_WORKERS", "32"))

# Per-gateway rate cap. We pull ~2.5TB through gateways we don't pay for; the
# probe saw no 429s, but a 100-request burst proves nothing about 9M requests.
GATEWAY_RATE_LIMIT: float = float(os.getenv("GATEWAY_RATE_LIMIT", "30"))   # req/s per gateway
PUMP_API_RATE_LIMIT: float = float(os.getenv("PUMP_API_RATE_LIMIT", "50")) # req/s (measured 99 without 429)

HTTP_TIMEOUT:   int = int(os.getenv("HTTP_TIMEOUT", "30"))
HTTP_RETRIES:   int = int(os.getenv("HTTP_RETRIES", "3"))
FLUSH_EVERY_MINTS: int = int(os.getenv("FLUSH_EVERY_MINTS", "5000"))

# A long-running academic crawl should identify itself. Set a contact address.
CRAWLER_UA: str = os.getenv(
    "CRAWLER_UA",
    "pumpfun-research-crawler/0.1 (UFMG LOCUS; academic research; +https://github.com/Filipey)",
)

IMAGES_DIR.mkdir(parents=True, exist_ok=True)
