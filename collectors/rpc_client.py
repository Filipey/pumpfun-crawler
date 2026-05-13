"""
collectors/rpc_client.py
------------------------
Multi-endpoint Solana JSON-RPC client with:
  - Round-robin endpoint rotation
  - Per-endpoint 429 cooldown tracking (rate-limited endpoints are skipped
    until their backoff window expires; if all endpoints are cooling down,
    the client sleeps until the soonest one recovers)
  - Exponential backoff on network errors and 5xx responses
  - Batch requests for getTransaction (up to TX_BATCH_SIZE per HTTP call)
"""

import logging
import threading
import time
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


class SolanaRPC:
    def __init__(self, endpoints: list[str] | None = None):
        self._endpoints = endpoints or RPC_ENDPOINTS
        if not self._endpoints:
            raise ValueError("No RPC endpoints configured.")
        self._lock = threading.Lock()
        self._idx  = 0
        # unix-monotonic timestamp until which each endpoint is in 429 cooldown
        self._cooldown_until: dict[str, float] = {}
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

        n = len(self._endpoints)
        logger.info(f"RPC client ready — {n} endpoint(s) configured")

    # ── Endpoint selection ────────────────────────────────────────────────────

    def _mark_cooldown(self, ep: str, seconds: float) -> None:
        with self._lock:
            self._cooldown_until[ep] = time.monotonic() + seconds

    def _next(self) -> str:
        """
        Return the next endpoint that is not in 429 cooldown.
        If every endpoint is cooling down, sleep until the soonest one recovers.
        """
        with self._lock:
            n   = len(self._endpoints)
            now = time.monotonic()

            for i in range(n):
                ep = self._endpoints[self._idx % n]
                self._idx += 1
                if self._cooldown_until.get(ep, 0) <= now:
                    return ep

            # All endpoints are cooling — find the one that recovers soonest
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

            # ── Network-level errors ──────────────────────────────────────────
            try:
                resp = self._session.post(ep, json=payload, timeout=RPC_TIMEOUT)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as exc:
                wait = RETRY_BACKOFF ** attempt
                logger.warning(
                    f"Network error from {ep[:50]} "
                    f"({type(exc).__name__}), retry {attempt + 1} in {wait:.1f}s"
                )
                time.sleep(wait)
                ep = self._next()
                continue

            # ── Rate limited ──────────────────────────────────────────────────
            if resp.status_code == 429:
                wait = RETRY_BACKOFF ** attempt * 4
                logger.warning(
                    f"429 from {ep[:50]} — cooling {wait:.1f}s then rotating"
                )
                self._mark_cooldown(ep, wait)
                ep = self._next()
                continue

            # ── Server error ──────────────────────────────────────────────────
            if resp.status_code >= 500:
                wait = RETRY_BACKOFF ** attempt
                logger.warning(
                    f"HTTP {resp.status_code} from {ep[:50]}, "
                    f"retry {attempt + 1} in {wait:.1f}s"
                )
                time.sleep(wait)
                ep = self._next()
                continue

            # ── Permanent client error (4xx, not 429) ─────────────────────────
            if not resp.ok:
                resp.raise_for_status()

            return resp.json()

        raise RuntimeError(
            f"Gave up after {MAX_RETRIES + 1} attempts "
            f"(last endpoint: {ep[:50]})"
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
        Uses JSON-RPC batching (TX_BATCH_SIZE per HTTP request).
        Returns a list in the same order as `signatures`.
        """
        results: list[Optional[dict]] = []

        for batch_start in range(0, len(signatures), TX_BATCH_SIZE):
            batch   = signatures[batch_start: batch_start + TX_BATCH_SIZE]
            payload = [
                {
                    "jsonrpc": "2.0",
                    "id": i,
                    "method": "getTransaction",
                    "params": [
                        sig,
                        {
                            "encoding": "json",
                            "commitment": commitment,
                            "maxSupportedTransactionVersion": 0,
                        },
                    ],
                }
                for i, sig in enumerate(batch)
            ]
            raw = self._post(payload)

            if isinstance(raw, list):
                raw.sort(key=lambda r: r.get("id", 0) if isinstance(r, dict) else 0)
                results.extend(
                    r.get("result") if isinstance(r, dict) else None
                    for r in raw
                )
            else:
                results.extend([None] * len(batch))

        return results

    def close(self) -> None:
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
