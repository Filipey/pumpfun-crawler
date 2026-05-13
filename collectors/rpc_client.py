"""
collectors/rpc_client.py
------------------------
Multi-endpoint Solana JSON-RPC client with:
  - Primary/fallback endpoint selection (endpoints tried in declaration order)
  - Per-endpoint 429 cooldown tracking (rate-limited endpoints are skipped
    until their backoff window expires; if all endpoints are cooling down,
    the client sleeps until the soonest one recovers)
  - Exponential backoff on network errors and 5xx responses
  - Concurrent individual getTransaction requests (TX_BATCH_SIZE workers)
    NOTE: JSON-RPC batch arrays are NOT used — Helius free tier rejects them
"""

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

import requests

from config import (
    MAX_RETRIES,
    RETRY_BACKOFF,
    RPC_ENDPOINTS,
    RPC_TIMEOUT,
    SIGNATURES_LIMIT,
    TX_BATCH_SIZE,
)

logger = logging.getLogger(__name__)


def _mask(url: str) -> str:
    """Redact API key values from URLs so they never appear in logs or tracebacks."""
    return re.sub(r'(?i)(api[-_]?key=)[^&\s]+', r'\1***', url)


class SolanaRPC:
    def __init__(self, endpoints: list[str] | None = None):
        self._endpoints = endpoints or RPC_ENDPOINTS
        if not self._endpoints:
            raise ValueError("No RPC endpoints configured.")
        self._lock = threading.Lock()
        # unix-monotonic timestamp until which each endpoint is in 429 cooldown
        self._cooldown_until: dict[str, float] = {}
        # thread-local sessions so concurrent workers don't share one socket
        self._local = threading.local()

        n = len(self._endpoints)
        logger.info(f"RPC client ready — {n} endpoint(s) configured")

    def _session(self) -> requests.Session:
        if not getattr(self._local, "session", None):
            s = requests.Session()
            s.headers.update({"Content-Type": "application/json"})
            self._local.session = s
        return self._local.session

    # ── Endpoint selection ────────────────────────────────────────────────────

    def _mark_cooldown(self, ep: str, seconds: float) -> None:
        with self._lock:
            self._cooldown_until[ep] = time.monotonic() + seconds

    def _next(self) -> str:
        """
        Return the first endpoint in the list that is not in 429 cooldown.
        Endpoints are tried in declaration order (primary → fallback), so put
        your best endpoint first in RPC_ENDPOINTS.

        If every endpoint is cooling down, sleep until the soonest one recovers.
        """
        with self._lock:
            now = time.monotonic()

            for ep in self._endpoints:
                if self._cooldown_until.get(ep, 0) <= now:
                    return ep

            # All endpoints are cooling — wait for the soonest recovery
            ep   = min(self._endpoints, key=lambda e: self._cooldown_until.get(e, 0))
            wait = max(0.0, self._cooldown_until[ep] - now)

        # Sleep outside the lock so other threads can proceed
        logger.warning(f"All endpoints rate-limited — waiting {wait:.1f}s")
        time.sleep(wait)
        return ep

    # ── HTTP ──────────────────────────────────────────────────────────────────

    def _post(self, payload: Any, endpoint: str | None = None) -> Any:
        ep = endpoint or self._next()

        for attempt in range(MAX_RETRIES + 1):
            safe_ep = _mask(ep)

            # ── Network-level errors ──────────────────────────────────────────
            try:
                resp = self._session().post(ep, json=payload, timeout=RPC_TIMEOUT)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as exc:
                wait = RETRY_BACKOFF ** attempt
                logger.warning(
                    f"Network error from {safe_ep} "
                    f"({type(exc).__name__}), retry {attempt + 1} in {wait:.1f}s"
                )
                time.sleep(wait)
                ep = self._next()
                continue

            # ── Rate limited ──────────────────────────────────────────────────
            if resp.status_code == 429:
                wait = RETRY_BACKOFF ** attempt * 4
                logger.warning(f"429 from {safe_ep} — cooling {wait:.1f}s then rotating")
                self._mark_cooldown(ep, wait)
                ep = self._next()
                continue

            # ── Server error ──────────────────────────────────────────────────
            if resp.status_code >= 500:
                wait = RETRY_BACKOFF ** attempt
                logger.warning(
                    f"HTTP {resp.status_code} from {safe_ep}, "
                    f"retry {attempt + 1} in {wait:.1f}s"
                )
                time.sleep(wait)
                ep = self._next()
                continue

            # ── Permanent client error (4xx, not 429) ─────────────────────────
            if not resp.ok:
                # Raise with a sanitized message so API keys never appear in logs
                raise requests.exceptions.HTTPError(
                    f"HTTP {resp.status_code} {resp.reason} from {safe_ep}",
                    response=resp,
                )

            return resp.json()

        raise RuntimeError(
            f"Gave up after {MAX_RETRIES + 1} attempts (last endpoint: {safe_ep})"
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def get_signatures_for_address(
        self,
        address: str,
        before: str | None = None,
        until: str | None = None,
        limit: int = SIGNATURES_LIMIT,
        commitment: str = "finalized",
    ) -> list[dict]:
        """
        Paginate transaction signatures for a program address.
        Returns list of {signature, slot, blockTime, err, memo}, newest → oldest.
        """
        config: dict = {"limit": limit, "commitment": commitment}
        if before:
            config["before"] = before
        if until:
            config["until"] = until

        result = self._post({
            "jsonrpc": "2.0", "id": 1,
            "method": "getSignaturesForAddress",
            "params": [address, config],
        })

        if isinstance(result, dict):
            return result.get("result") or []
        return []

    def get_transactions(
        self,
        signatures: list[str],
        commitment: str = "finalized",
    ) -> list[Optional[dict]]:
        """
        Fetch full transaction data for a list of signatures.

        Sends TX_BATCH_SIZE concurrent individual getTransaction requests.
        JSON-RPC batch arrays are intentionally avoided — Helius free tier
        rejects them with 413/403.
        """
        if not signatures:
            return []

        results: list[Optional[dict]] = [None] * len(signatures)
        params = {"encoding": "json", "commitment": commitment, "maxSupportedTransactionVersion": 0}

        def _fetch_one(idx: int, sig: str) -> tuple[int, Optional[dict]]:
            try:
                raw = self._post({
                    "jsonrpc": "2.0", "id": idx,
                    "method": "getTransaction",
                    "params": [sig, params],
                })
                return idx, raw.get("result") if isinstance(raw, dict) else None
            except Exception as exc:
                logger.warning(f"Failed to fetch tx {sig[:20]}…: {exc}")
                return idx, None

        with ThreadPoolExecutor(max_workers=TX_BATCH_SIZE) as pool:
            futures = {pool.submit(_fetch_one, i, sig): i for i, sig in enumerate(signatures)}
            for fut in as_completed(futures):
                idx, tx = fut.result()
                results[idx] = tx

        return results

    def close(self) -> None:
        s = getattr(self._local, "session", None)
        if s:
            s.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
