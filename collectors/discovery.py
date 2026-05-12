"""
collectors/discovery.py
------------------------
Discovers all pump.fun tokens created in [COLLECT_START, COLLECT_END].

Strategy: paginate /coins sorted by created_timestamp DESC (newest first).
Stop once we see a token older than COLLECT_START.

Because the API sorts newest-first, the window is contiguous at the start
of the pagination — no need to scan the full history.

Yields token dicts from the /coins listing endpoint (shallow metadata only;
full enrichment happens in token_processor.py).
"""

import logging
from datetime import datetime, timezone
from typing import Generator

from config import COLLECT_START, COLLECT_END, PUMPFUN_PAGE_SIZE
from collectors.api_client import PumpFunClient

logger = logging.getLogger(__name__)


def _parse_date(iso: str) -> int:
    """ISO date string → unix milliseconds (start of that UTC day)."""
    dt = datetime.strptime(iso, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


START_MS: int = _parse_date(COLLECT_START)
END_MS:   int = _parse_date(COLLECT_END) + 86_400_000 - 1  # end of day


def discover_tokens(
    client: PumpFunClient,
    page_size: int = PUMPFUN_PAGE_SIZE,
) -> Generator[dict, None, None]:
    """
    Yield every token created between COLLECT_START and COLLECT_END.

    Paginates through /coins DESC by created_timestamp; stops when tokens
    are older than COLLECT_START.
    """
    offset = 0
    total_yielded = 0
    total_scanned = 0

    logger.info(
        f"Discovering tokens between {COLLECT_START} and {COLLECT_END} …"
    )

    while True:
        page = client.get_coins_page(offset=offset, limit=page_size)
        if not page:
            logger.info("Empty page — discovery complete")
            break

        stop_after = False
        for token in page:
            ts: int = int(token.get("created_timestamp", 0) or 0)
            total_scanned += 1

            if ts > END_MS:
                # Too new — skip but keep paginating
                continue

            if ts < START_MS:
                # Too old — everything after this is also too old
                stop_after = True
                break

            yield token
            total_yielded += 1

        if total_scanned % 500 == 0:
            logger.info(
                f"Scanned {total_scanned} tokens, yielded {total_yielded} "
                f"(offset={offset})"
            )

        if stop_after:
            logger.info(
                f"Reached tokens older than {COLLECT_START} at offset={offset}"
            )
            break

        offset += page_size

    logger.info(
        f"Discovery finished: scanned {total_scanned}, yielded {total_yielded} tokens"
    )
