"""
pumpfun_collector/client.py
----------------------------
Thin GraphQL client for Bitquery with rate limiting and exponential backoff.
"""

import logging
import time

import backoff
import requests
from ratelimit import limits, sleep_and_retry

from config import (BITQUERY_ENDPOINT, BITQUERY_TOKEN, MAX_RETRIES,
                    RATE_LIMIT_CALLS, RATE_LIMIT_PERIOD)

logger = logging.getLogger(__name__)


class BitqueryError(Exception):
    """Raised when the Bitquery API returns errors."""
    pass


class BitqueryClient:
    """
    GraphQL client for Bitquery Streaming API (V2).
    Features:
      - Automatic rate limiting (configurable calls/period)
      - Exponential backoff on transient failures (429, 500, 502, 503, 504)
      - Structured error extraction from GraphQL responses
    """

    def __init__(self, token: str | None = None, endpoint: str | None = None):
        self.token = token or BITQUERY_TOKEN
        self.endpoint = endpoint or BITQUERY_ENDPOINT
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        })
        self._call_count = 0

    @sleep_and_retry
    @limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
    def _rate_limited_post(self, payload: dict) -> requests.Response:
        """POST with rate limiting enforced."""
        return self.session.post(self.endpoint, json=payload, timeout=120)

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, BitqueryError),
        max_tries=MAX_RETRIES,
        giveup=lambda e: isinstance(e, BitqueryError) and "auth" in str(e).lower(),
    )
    def query(self, gql: str, variables: dict | None = None) -> dict:
        """
        Execute a GraphQL query/mutation and return the `data` dict.
        Raises BitqueryError on GraphQL-level errors.
        Retries on HTTP transport errors with exponential backoff.
        """
        payload = {"query": gql}
        if variables:
            payload["variables"] = variables

        self._call_count += 1
        resp = self._rate_limited_post(payload)

        # Retry on server-side HTTP errors
        if resp.status_code in (429, 500, 502, 503, 504):
            logger.warning(
                f"HTTP {resp.status_code} from Bitquery (call #{self._call_count}), retrying..."
            )
            raise requests.exceptions.RequestException(
                f"HTTP {resp.status_code}"
            )

        resp.raise_for_status()
        body = resp.json()

        if "errors" in body and body["errors"]:
            err_msg = "; ".join(e.get("message", str(e)) for e in body["errors"])
            logger.error(f"GraphQL errors: {err_msg}")
            raise BitqueryError(err_msg)

        return body.get("data", {})

    @property
    def call_count(self) -> int:
        return self._call_count
