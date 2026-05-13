"""
collectors/rpc_client.py
------------------------
Multi-endpoint Solana JSON-RPC client with:
  - Round-robin endpoint rotation
  - Exponential backoff on errors and 429s
  - Batch requests for getTransaction (up to TX_BATCH_SIZE per HTTP call)
"""

import logging
import threading
import time
from typing import Any, Optional

import requests

from config import (
    RPC_ENDPOINTS,
    RPC_TIMEOUT,
    MAX_RETRIES,
    RETRY_BACKOFF,
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
        self._idx = 0
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def _next(self) -> str:
        with self._lock:
            ep = self._endpoints[self._idx % len(self._endpoints)]
            self._idx += 1
            return ep

    def _post(self, payload: Any, endpoint: str | None = None) -> Any:
        ep = endpoint or self._next()
        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = self._session.post(ep, json=payload, timeout=RPC_TIMEOUT)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response else 0
                if status == 429:
                    wait = RETRY_BACKOFF ** attempt * 4
                    logger.warning(f"429 from {ep}, sleeping {wait:.1f}s then rotating")
                    time.sleep(wait)
                    ep = self._next()
                    continue
                if status >= 500:
                    wait = RETRY_BACKOFF ** attempt
                    logger.warning(f"{status} from {ep}, retry {attempt+1} in {wait:.1f}s")
                    time.sleep(wait)
                    ep = self._next()
                    continue
                raise
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as exc:
                wait = RETRY_BACKOFF ** attempt
                logger.warning(f"Network error ({exc}), retry {attempt+1} in {wait:.1f}s")
                time.sleep(wait)
                ep = self._next()

        raise RuntimeError(f"All {MAX_RETRIES} retries exhausted")

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

        Returns list of {signature, slot, blockTime, err, memo}.
        Sorted newest → oldest.
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
            data = result.get("result") or []
        else:
            data = []
        return data

    def get_transactions(
        self,
        signatures: list[str],
        commitment: str = "finalized",
    ) -> list[Optional[dict]]:
        """
        Fetch full transaction data for a list of signatures.

        Uses JSON-RPC batching (TX_BATCH_SIZE per HTTP request) to minimise
        the number of round trips.

        Returns a list in the same order as `signatures`.
        """
        results: list[Optional[dict]] = []

        for batch_start in range(0, len(signatures), TX_BATCH_SIZE):
            batch = signatures[batch_start: batch_start + TX_BATCH_SIZE]
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

            # raw is a list of RPC response objects; sort by id to preserve order
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
