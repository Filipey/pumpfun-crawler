"""
pumpfun_collector/utils.py
---------------------------
Shared utilities: time window generation, JSON flattening, Parquet I/O.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Generator

import pandas as pd

from config import PAGINATION_WINDOW_MINUTES

logger = logging.getLogger(__name__)


def time_windows(
    start: datetime,
    end: datetime,
    window_minutes: int = PAGINATION_WINDOW_MINUTES,
) -> Generator[tuple[str, str], None, None]:
    """
    Yield (after_iso, before_iso) pairs covering [start, end) in
    `window_minutes`-sized chunks. All times in UTC ISO format.
    """
    cursor = start
    delta = timedelta(minutes=window_minutes)
    while cursor < end:
        win_end = min(cursor + delta, end)
        yield (
            cursor.strftime("%Y-%m-%dT%H:%M:%SZ"),
            win_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        cursor = win_end


def flatten_json(nested: dict, prefix: str = "") -> dict:
    """Recursively flatten a nested dict with dot-separated keys."""
    out = {}
    for k, v in nested.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.update(flatten_json(v, key))
        elif isinstance(v, list):
            # Store lists as JSON strings for Parquet compatibility
            out[key] = json.dumps(v)
        else:
            out[key] = v
    return out


def records_to_df(records: list[dict], flatten: bool = True) -> pd.DataFrame:
    """Convert a list of nested dicts to a flat DataFrame."""
    if not records:
        return pd.DataFrame()
    if flatten:
        flat = [flatten_json(r) for r in records]
    else:
        flat = records
    return pd.DataFrame(flat)


def save_parquet(df: pd.DataFrame, path: Path, append: bool = False) -> None:
    """Save DataFrame to Parquet. If append=True and file exists, concatenate."""
    if df.empty:
        logger.debug(f"Empty DataFrame, skipping save to {path}")
        return
    if append and path.exists():
        existing = pd.read_parquet(path)
        df = pd.concat([existing, df], ignore_index=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info(f"Saved {len(df)} rows to {path}")


def load_parquet(path: Path) -> pd.DataFrame:
    """Load a Parquet file, return empty DataFrame if not found."""
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def parse_iso(s: str) -> datetime:
    """Parse ISO datetime string to timezone-aware datetime."""
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def date_range_days(
    start: datetime, end: datetime
) -> Generator[tuple[datetime, datetime], None, None]:
    """Yield (day_start, day_end) for each day in range."""
    cursor = start.replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor < end:
        day_end = cursor + timedelta(days=1)
        yield cursor, min(day_end, end)
        cursor = day_end
