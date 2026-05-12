"""
collectors/token_processor.py
------------------------------
Per-token processing logic:

  graduated   →  fetch token metadata + ALL trades
  active      →  trade_count > ACTIVITY_THRESHOLD → full trades
  dead        →  trade_count <= ACTIVITY_THRESHOLD → metadata only (no trades)

A "token" is classified as:
  - graduated  if token["complete"] == True
  - active     if not graduated and trade_count > ACTIVITY_THRESHOLD
  - dead       if not graduated and trade_count <= ACTIVITY_THRESHOLD
"""

import logging
import time
from typing import Literal

from config import ACTIVITY_THRESHOLD, TRADES_PAGE_SIZE
from collectors.api_client import PumpFunClient
from collectors.checkpoint import Checkpoint
from collectors.storage import ParquetStore

logger = logging.getLogger(__name__)

Category = Literal["graduated", "active", "dead"]


def _classify(token: dict) -> tuple[Category, int]:
    """Return (category, trade_count) for a token dict."""
    graduated: bool = bool(token.get("complete", False))
    # pump.fun returns trade count in the coin detail endpoint
    trade_count: int = int(token.get("trade_count", token.get("reply_count", 0)) or 0)

    if graduated:
        return "graduated", trade_count
    if trade_count > ACTIVITY_THRESHOLD:
        return "active", trade_count
    return "dead", trade_count


def process_token(
    token: dict,
    client: PumpFunClient,
    checkpoint: Checkpoint,
    store: ParquetStore,
) -> None:
    """
    Full processing for a single token dict from the /coins list.

    Fetches enriched detail, classifies, then fetches trades if needed.
    Marks checkpoint done/error when finished.
    """
    mint: str = token.get("mint", "")
    if not mint:
        return

    if checkpoint.is_done(mint):
        return

    try:
        # Enrich with full coin detail (includes trade_count, reserves, etc.)
        detail = client.get_coin(mint)
        if detail is None:
            logger.warning(f"Token {mint} returned 404, skipping")
            checkpoint.mark_error(mint, "404")
            return

        token_row = {**token, **detail}  # detail wins on overlapping keys
        token_row["collected_at"] = int(time.time() * 1000)

        category, trade_count = _classify(token_row)
        token_row["trade_count"] = trade_count
        token_row["category"]    = category

        store.add_token(token_row)

        if category in ("graduated", "active"):
            trades = client.get_all_trades(mint, page_size=TRADES_PAGE_SIZE)
            # Stamp each trade with the mint (API doesn't always include it)
            for t in trades:
                t["mint"] = mint
            store.add_trades(category, trades)
            logger.info(f"[{category}] {mint} — {len(trades)} trades")
        else:
            logger.debug(f"[dead] {mint} — {trade_count} trades (skipping)")

        checkpoint.mark_done(mint)

    except Exception as exc:
        logger.error(f"Error processing {mint}: {exc}")
        checkpoint.mark_error(mint, str(exc))
        raise
