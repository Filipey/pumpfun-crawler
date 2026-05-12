"""
collectors/api_client.py
------------------------
Thin HTTP client for the pump.fun REST API with:
  - token-bucket rate limiting (RATE_LIMIT_RPS)
  - exponential backoff on 429 / 5xx
  - structured logging of every call
"""

import logging
import time
import threading
from typing import Any

import requests

from config import (
    PUMPFUN_API_BASE,
    MAX_RETRIES,
    RETRY_BACKOFF,
    REQUEST_TIMEOUT,
    RATE_LIMIT_RPS,
)

logger = logging.getLogger(__name__)


class _TokenBucket:
    """Simple thread-safe token bucket for rate limiting."""

    def __init__(self, rate: float):
        self._rate = rate          # tokens per second
        self._tokens = rate
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            self._last = now
            if self._tokens < 1:
                sleep_for = (1 - self._tokens) / self._rate
                time.sleep(sleep_for)
                self._tokens = 0
            else:
                self._tokens -= 1


_bucket = _TokenBucket(RATE_LIMIT_RPS)


class PumpFunClient:
    """
    HTTP client for https://frontend-api.pump.fun.

    Methods raise on unrecoverable errors; callers should handle exceptions.
    """

    def __init__(self, base_url: str = PUMPFUN_API_BASE):
        self._base = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "User-Agent": "pumpfun-research-crawler/2.0",
        })

    def _get(self, path: str, params: dict | None = None) -> Any:
        url = f"{self._base}{path}"
        for attempt in range(MAX_RETRIES + 1):
            _bucket.acquire()
            try:
                resp = self._session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    wait = RETRY_BACKOFF ** attempt * 2
                    logger.warning(f"429 rate-limited on {path}, sleeping {wait:.1f}s")
                    time.sleep(wait)
                    continue
                if resp.status_code >= 500:
                    wait = RETRY_BACKOFF ** attempt
                    logger.warning(f"{resp.status_code} server error on {path}, retry {attempt+1}")
                    time.sleep(wait)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
            except requests.exceptions.ConnectionError as exc:
                wait = RETRY_BACKOFF ** attempt
                logger.warning(f"Connection error on {path}: {exc}. Retry {attempt+1} in {wait:.1f}s")
                time.sleep(wait)
            except requests.exceptions.Timeout:
                wait = RETRY_BACKOFF ** attempt
                logger.warning(f"Timeout on {path}. Retry {attempt+1} in {wait:.1f}s")
                time.sleep(wait)

        raise RuntimeError(f"All {MAX_RETRIES} retries exhausted for {path}")

    # ── High-level API helpers ────────────────────────────────────────────────

    def get_coins_page(
        self,
        offset: int = 0,
        limit: int = 50,
        sort: str = "created_timestamp",
        order: str = "DESC",
        include_nsfw: bool = True,
    ) -> list[dict]:
        """Paginated list of tokens sorted by creation time (newest first)."""
        data = self._get("/coins", params={
            "offset":       offset,
            "limit":        limit,
            "sort":         sort,
            "order":        order,
            "includeNsfw":  str(include_nsfw).lower(),
        })
        if data is None:
            return []
        if isinstance(data, list):
            return data
        # some versions wrap in a dict
        if isinstance(data, dict):
            return data.get("coins", data.get("data", []))
        return []

    def get_coin(self, mint: str) -> dict | None:
        """Single token metadata by mint address."""
        return self._get(f"/coins/{mint}")

    def get_trades_page(
        self,
        mint: str,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict]:
        """One page of trades for a token (newest first)."""
        data = self._get(f"/trades/all/{mint}", params={
            "limit":  limit,
            "offset": offset,
        })
        if data is None:
            return []
        if isinstance(data, list):
            return data
        return []

    def get_all_trades(self, mint: str, page_size: int = 200) -> list[dict]:
        """Fetch every trade for a token via offset pagination."""
        all_trades: list[dict] = []
        offset = 0
        while True:
            page = self.get_trades_page(mint, limit=page_size, offset=offset)
            if not page:
                break
            all_trades.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
        logger.debug(f"Fetched {len(all_trades)} trades for {mint}")
        return all_trades

    def close(self) -> None:
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
