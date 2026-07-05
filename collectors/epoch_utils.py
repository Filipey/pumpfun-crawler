"""
collectors/epoch_utils.py
--------------------------
Maps calendar dates to approximate Solana epoch ranges, for driving
Jetstreamer's epoch/slot-range CLI arguments (Phase B).

Precision doesn't need to be tight: Jetstreamer replays whole epochs, and
collectors/oldfaithful_source.py buckets each transaction into its correct
calendar day using that transaction's own on-chain block_time (exactly like
Phase A's SolArchive ingestion does) regardless of which epoch it came from.
So this only needs to answer "roughly which epochs cover this date range",
padded generously — a few epochs of harmless overlap is free (checkpointing
is per-epoch, and every downstream table dedups on signature+tx_type), while
under-covering the range would silently lose data at the boundaries.

Uses a couple of lightweight RPC calls (getEpochInfo/getBlockTime) against a
public endpoint to calibrate actual average slot duration empirically,
rather than trusting Solana's nominal 400ms/slot target — a first pass using
the nominal rate alone was off by ~2 days over a 7-month span in testing.
"""

import datetime
import logging

from collectors.rpc_client import SolanaRPC

logger = logging.getLogger(__name__)

SLOTS_PER_EPOCH = 432_000
CALIBRATION_SLOT_SPAN = 50_000_000  # ~a few months, for measuring real avg slot time
PAD_EPOCHS = 3  # generous safety margin on each side of the estimated range


def _block_time_near(rpc: SolanaRPC, slot: int, max_probe: int = 20) -> tuple[int, int]:
    """getBlockTime for `slot`, probing forward a few slots if it was skipped. Returns (slot, block_time)."""
    for offset in range(max_probe):
        result = rpc._post({"jsonrpc": "2.0", "id": 1, "method": "getBlockTime", "params": [slot + offset]})
        bt = result.get("result")
        if bt:
            return slot + offset, bt
    raise RuntimeError(f"Could not find a block time near slot {slot} after {max_probe} probes")


def estimate_epoch_range(
    start_date: str,
    end_date: str,
    rpc: SolanaRPC | None = None,
    pad_epochs: int = PAD_EPOCHS,
) -> tuple[int, int]:
    """
    Return (start_epoch, end_epoch) inclusive, generously padded, estimated
    to cover [start_date, end_date] (YYYY-MM-DD, UTC).
    """
    owns_rpc = rpc is None
    rpc = rpc or SolanaRPC(endpoints=["https://api.mainnet-beta.solana.com"])

    try:
        info = rpc._post({"jsonrpc": "2.0", "id": 1, "method": "getEpochInfo", "params": []})["result"]
        current_epoch = info["epoch"]
        anchor_slot, anchor_bt = _block_time_near(rpc, info["absoluteSlot"])

        calib_slot = max(0, anchor_slot - CALIBRATION_SLOT_SPAN)
        calib_slot, calib_bt = _block_time_near(rpc, calib_slot)

        actual_slot_seconds = (anchor_bt - calib_bt) / (anchor_slot - calib_slot)
        logger.info(
            f"Calibrated avg slot time: {actual_slot_seconds:.5f}s "
            f"(anchor slot {anchor_slot} @ {anchor_bt}, calib slot {calib_slot} @ {calib_bt})"
        )

        def date_to_slot(date_str: str) -> int:
            target_ts = int(datetime.datetime.fromisoformat(date_str).replace(tzinfo=datetime.timezone.utc).timestamp())
            delta_seconds = anchor_bt - target_ts
            return anchor_slot - int(delta_seconds / actual_slot_seconds)

        start_slot = date_to_slot(start_date)
        end_slot = date_to_slot(end_date)

        start_epoch = max(0, start_slot // SLOTS_PER_EPOCH - pad_epochs)
        # Old Faithful lags the chain tip by ~1 epoch (per its own docs) — clamp so we
        # never request an epoch that doesn't exist yet on either the live chain or the archive.
        end_epoch = min(end_slot // SLOTS_PER_EPOCH + pad_epochs, current_epoch - 1)

        logger.info(f"Estimated epoch range for {start_date}..{end_date}: {start_epoch}..{end_epoch}")
        return start_epoch, end_epoch
    finally:
        if owns_rpc:
            rpc.close()


def epoch_to_slot_range(epoch: int) -> tuple[int, int]:
    """(start_slot_inclusive, end_slot_exclusive) for a given epoch, assuming the fixed 432,000-slot epoch length that has held since early in Solana's history (irrelevant warmup-epoch exceptions only affect epoch 0-var, long before our range)."""
    return epoch * SLOTS_PER_EPOCH, (epoch + 1) * SLOTS_PER_EPOCH
