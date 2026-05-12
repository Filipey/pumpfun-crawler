"""
collectors/metadata_fetcher.py
-------------------------------
Downloads off-chain metadata (image, description) from token URIs.
Used for Research Axis 1: building the input corpus for multi-label classification.

pump.fun token URIs point to JSON files (typically on IPFS/Arweave) with structure:
{
  "name": "PEPESTEIN",
  "symbol": "PEPESTEIN",
  "description": "Pepe meets Einstein...",
  "image": "https://ipfs.io/ipfs/Qm...",
  "showName": true,
  "createdOn": "https://pump.fun"
}

This module:
  1. Reads token creation Parquets to get URIs
  2. Fetches each JSON metadata file
  3. Optionally downloads the image
  4. Saves a consolidated Parquet with text + image path for classification
"""

import json
import logging
import time
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests

from config import PARQUET_DIR, RAW_DIR

logger = logging.getLogger(__name__)

# Common IPFS gateways to try if the original fails
IPFS_GATEWAYS = [
    "https://ipfs.io/ipfs/",
    "https://cloudflare-ipfs.com/ipfs/",
    "https://gateway.pinata.cloud/ipfs/",
]


class MetadataFetcher:
    """
    Fetches off-chain metadata (JSON + images) for pump.fun tokens.
    Input: token creation Parquets (from TokenCollector).
    Output: enriched Parquet with description, image_path columns.
    """

    def __init__(self, download_images: bool = True, timeout: int = 15):
        self.download_images = download_images
        self.timeout = timeout
        self.metadata_dir = RAW_DIR / "metadata"
        self.image_dir = RAW_DIR / "images"
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        if download_images:
            self.image_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "pumpfun-research/1.0"

    def _resolve_ipfs_url(self, url: str) -> str:
        """Convert ipfs:// scheme or raw CID to HTTP gateway URL."""
        if not url:
            return url
        if url.startswith("ipfs://"):
            cid = url.replace("ipfs://", "")
            return IPFS_GATEWAYS[0] + cid
        if url.startswith("Qm") or url.startswith("bafy"):
            return IPFS_GATEWAYS[0] + url
        return url

    def _fetch_json(self, uri: str) -> dict | None:
        """Fetch and parse a JSON metadata file from URI."""
        uri = self._resolve_ipfs_url(uri)
        if not uri or not uri.startswith("http"):
            return None

        for attempt in range(3):
            try:
                resp = self.session.get(uri, timeout=self.timeout)
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    return None
            except (requests.RequestException, json.JSONDecodeError) as e:
                if attempt < 2:
                    # Try alternative IPFS gateway
                    parsed = urlparse(uri)
                    path = parsed.path
                    if "/ipfs/" in path:
                        cid = path.split("/ipfs/")[-1]
                        gateway = IPFS_GATEWAYS[min(attempt + 1, len(IPFS_GATEWAYS) - 1)]
                        uri = gateway + cid
                    time.sleep(1)
                else:
                    logger.debug(f"Failed to fetch {uri}: {e}")
                    return None
        return None

    def _download_image(self, image_url: str, mint: str) -> str | None:
        """Download token image and save locally. Returns local path."""
        image_url = self._resolve_ipfs_url(image_url)
        if not image_url or not image_url.startswith("http"):
            return None

        # Determine extension
        ext = ".png"
        if ".jpg" in image_url or ".jpeg" in image_url:
            ext = ".jpg"
        elif ".gif" in image_url:
            ext = ".gif"
        elif ".webp" in image_url:
            ext = ".webp"

        local_path = self.image_dir / f"{mint[:32]}{ext}"
        if local_path.exists():
            return str(local_path)

        try:
            resp = self.session.get(image_url, timeout=self.timeout, stream=True)
            if resp.status_code == 200:
                with open(local_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                return str(local_path)
        except requests.RequestException:
            pass
        return None

    def fetch_for_tokens(
        self,
        token_df: pd.DataFrame | None = None,
        max_tokens: int = 5000,
        delay: float = 0.3,
    ) -> Path:
        """
        Fetch metadata for all tokens in the DataFrame (or from collected Parquets).
        Adds columns: description, image_url, image_local_path, metadata_json.
        Returns path to enriched Parquet.
        """
        if token_df is None:
            token_dir = PARQUET_DIR / "tokens"
            files = sorted(token_dir.glob("creations_*.parquet"))
            if not files:
                logger.error("No token creation files found")
                return Path()
            token_df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

        if "uri" not in token_df.columns:
            logger.error("'uri' column not found in token data")
            return Path()

        # Deduplicate by mint
        if "mint_address" in token_df.columns:
            token_df = token_df.drop_duplicates(subset=["mint_address"])

        token_df = token_df.head(max_tokens).copy()
        logger.info(f"Fetching metadata for {len(token_df)} tokens...")

        descriptions = []
        image_urls = []
        image_paths = []
        metadata_jsons = []

        for idx, row in token_df.iterrows():
            uri = row.get("uri", "")
            mint = row.get("mint_address", f"unknown_{idx}")

            meta = self._fetch_json(uri)

            if meta:
                desc = meta.get("description", "")
                img_url = meta.get("image", "")
                descriptions.append(desc)
                image_urls.append(img_url)
                metadata_jsons.append(json.dumps(meta))

                if self.download_images and img_url:
                    local = self._download_image(img_url, mint)
                    image_paths.append(local or "")
                else:
                    image_paths.append("")
            else:
                descriptions.append("")
                image_urls.append("")
                image_paths.append("")
                metadata_jsons.append("")

            if (idx + 1) % 100 == 0:
                logger.info(f"  Fetched {idx + 1}/{len(token_df)} metadata entries")

            time.sleep(delay)

        token_df["description"] = descriptions
        token_df["image_url"] = image_urls
        token_df["image_local_path"] = image_paths
        token_df["metadata_json"] = metadata_jsons

        out_path = PARQUET_DIR / "tokens" / "tokens_with_metadata.parquet"
        token_df.to_parquet(out_path, index=False)
        logger.info(f"Saved enriched metadata to {out_path} ({len(token_df)} tokens)")

        # Stats
        has_desc = token_df["description"].str.len().gt(0).sum()
        has_img = token_df["image_local_path"].str.len().gt(0).sum()
        logger.info(f"  Tokens with description: {has_desc}/{len(token_df)}")
        logger.info(f"  Tokens with downloaded image: {has_img}/{len(token_df)}")

        return out_path
