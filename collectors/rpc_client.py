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
import re
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
                resp = self._session.post(ep, json=payload, timeout=RPC_TIMEOUT)
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

        Tries TX_BATCH_SIZE per HTTP request. On 413, splits the batch in half
        and retries each half recursively, down to individual calls. If a single
        transaction still 413s, it is returned as None and a warning is logged.
        """
        results: list[Optional[dict]] = [None] * len(signatures)
        if signatures:
            self._fetch_txs(signatures, list(range(len(signatures))), results, commitment)
        return results

    def _fetch_txs(
        self,
        all_sigs: list[str],
        indices: list[int],
        results: list[Optional[dict]],
        commitment: str,
    ) -> None:
        """Recursively fetch transactions, halving the batch on 413."""
        if not indices:
            return

        batch = [all_sigs[i] for i in indices]
        params = {"encoding": "json", "commitment": commitment, "maxSupportedTransactionVersion": 0}

        if len(batch) == 1:
            # Single request — plain object, not array (maximum compatibility)
            try:
                raw = self._post({
                    "jsonrpc": "2.0", "id": 0,
                    "method": "getTransaction",
                    "params": [batch[0], params],
                })
                results[indices[0]] = raw.get("result") if isinstance(raw, dict) else None
            except requests.exceptions.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 413:
                    logger.warning(f"Skipping oversized transaction {batch[0][:20]}… (413)")
                else:
                    raise
            return

        # Batch request
        payload = [
            {"jsonrpc": "2.0", "id": j, "method": "getTransaction",
             "params": [sig, params]}
            for j, sig in enumerate(batch)
        ]
        try:
            raw = self._post(payload)
            if isinstance(raw, list):
                raw.sort(key=lambda r: r.get("id", 0) if isinstance(r, dict) else 0)
                for j, r in enumerate(raw):
                    results[indices[j]] = r.get("result") if isinstance(r, dict) else None
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 413:
                mid = len(indices) // 2
                logger.warning(
                    f"413 on batch of {len(indices)}, splitting → {mid} + {len(indices)-mid}"
                )
                self._fetch_txs(all_sigs, indices[:mid], results, commitment)
                self._fetch_txs(all_sigs, indices[mid:], results, commitment)
            else:
                raise

    def close(self) -> None:
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
