"""
collectors/decoder.py
---------------------
Decodes pump.fun Anchor events from Solana transaction log messages.

How it works
------------
The pump.fun program (Anchor framework) emits events via the `emit!()` macro.
Each emitted event appears in the transaction's `logMessages` as a base64 line:

    "Program data: <base64>"

The decoded bytes start with an 8-byte Anchor event discriminator:
    discriminator = sha256("event:{EventName}".encode())[:8]

Followed by Borsh-serialised fields in the order defined in the IDL.

Events handled
--------------
  CreateEvent  – token creation (mint, creator, name, symbol, uri, reserves)
  TradeEvent   – buy or sell on the bonding curve (amounts, reserves, user)
  CompleteEvent – graduation to PumpSwap AMM

isBot detection (paper §IV)
---------------------------
isBot = True  → pump.fun program invoked at depth [1] (direct call, no outer program)
isBot = False → invoked at depth [2]+ (called through another program / dApp router)
isBot = None  → depth not found in logs
"""

import base64
import hashlib
import logging
import struct
from typing import Optional

logger = logging.getLogger(__name__)

# ── Pump.fun program ID ───────────────────────────────────────────────────────
PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


# ── Anchor event discriminators ───────────────────────────────────────────────

def _event_disc(name: str) -> bytes:
    return hashlib.sha256(f"event:{name}".encode()).digest()[:8]


TRADE_DISC    = _event_disc("TradeEvent")
CREATE_DISC   = _event_disc("CreateEvent")
COMPLETE_DISC = _event_disc("CompleteEvent")


# ── Borsh reader ──────────────────────────────────────────────────────────────

class _Reader:
    """Minimal Borsh deserialiser for the types used in pump.fun events."""

    __slots__ = ("_d", "_p")

    def __init__(self, data: bytes):
        self._d = data
        self._p = 0

    # ── primitives ──────────────────────────────────────────────────────────

    def _read(self, n: int) -> bytes:
        chunk = self._d[self._p: self._p + n]
        if len(chunk) < n:
            raise BufferError(
                f"Buffer underflow at pos {self._p}: need {n}, have {len(chunk)}"
            )
        self._p += n
        return chunk

    def u8(self)  -> int:  return struct.unpack_from("<B", self._read(1))[0]
    def u16(self) -> int:  return struct.unpack_from("<H", self._read(2))[0]
    def u32(self) -> int:  return struct.unpack_from("<I", self._read(4))[0]
    def u64(self) -> int:  return struct.unpack_from("<Q", self._read(8))[0]
    def i64(self) -> int:  return struct.unpack_from("<q", self._read(8))[0]
    def bool_(self) -> bool: return self.u8() != 0

    def pubkey(self) -> str:
        """32-byte public key → base58 string."""
        raw = self._read(32)
        return _b58encode(raw)

    def string(self) -> str:
        """Borsh string: u32 length + UTF-8 bytes."""
        length = self.u32()
        return self._read(length).decode("utf-8", errors="replace")

    @property
    def remaining(self) -> int:
        return len(self._d) - self._p


# ── Base58 encoding (avoids adding the base58 package) ───────────────────────
_B58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(data: bytes) -> str:
    n = int.from_bytes(data, "big")
    result = []
    while n > 0:
        n, rem = divmod(n, 58)
        result.append(_B58_ALPHABET[rem])
    # count leading zero bytes
    pad = len(data) - len(data.lstrip(b"\x00"))
    result.extend([_B58_ALPHABET[0]] * pad)
    return bytes(reversed(result)).decode("ascii")


# ── Event decoders ────────────────────────────────────────────────────────────

def _decode_trade(r: _Reader) -> dict:
    # Field order verified against pump-fun/pump-public-docs idl/pump.json history
    # (stable prefix from commit 33590255fe, 2025-10-17, onward — later revisions
    # only append fields after creator_fee, e.g. track_volume, total_unclaimed_
    # tokens, ix_name, mayhem_mode, cashback/buyback fields — safe to ignore).
    # The `creator` field lets us fill coin_creator directly on every trade
    # instead of relying solely on a later join against the CreateEvent.
    mint                 = r.pubkey()
    sol_amount           = r.u64()
    token_amount         = r.u64()
    is_buy               = r.bool_()
    user                 = r.pubkey()
    timestamp            = r.i64()
    v_sol                = r.u64()   # virtual_sol_reserves
    v_tok                = r.u64()   # virtual_token_reserves
    _real_sol_reserves   = r.u64()
    _real_token_reserves = r.u64()
    _fee_recipient       = r.pubkey()
    _fee_basis_points    = r.u64()
    fee                  = r.u64()
    creator              = r.pubkey()
    _creator_fee_bps     = r.u64()
    creator_fee          = r.u64()
    return {
        "tx_type":                  "buy" if is_buy else "sell",
        "is_buy":                   is_buy,
        "mint":                     mint,
        "trader":                   user,
        "coin_creator":             creator,
        "bonding_curve":            None,
        "sol_amount":               sol_amount,
        "token_amount":             token_amount,
        "in_bonding_curve":         True,
        "v_sol_in_bonding_curve":   v_sol,
        "v_tok_in_bonding_curve":   v_tok,
        "protocol_fee":             fee,
        "coin_creator_fee":         creator_fee,
    }


def _decode_create(r: _Reader) -> dict:
    # Field order verified against pump-fun/pump-public-docs idl/pump.json history
    # (stable from commit 33590255fe, 2025-10-17, through at least 2026-05-18 —
    # every later revision only appends fields after virtual_sol_reserves, e.g.
    # real_token_reserves, token_total_supply, token_program, is_mayhem_mode,
    # is_cashback_enabled, quote_mint, virtual_quote_reserves — safe to ignore
    # since Borsh reads don't require consuming the full buffer).
    # A prior version of this decoder read `timestamp` immediately after `user`,
    # which silently misaligned every field that followed once the program added
    # a distinct `creator` field between `user` and `timestamp`.
    name          = r.string()
    symbol        = r.string()
    uri           = r.string()
    mint          = r.pubkey()
    bonding_curve = r.pubkey()
    user          = r.pubkey()   # wallet that submitted the create tx
    creator       = r.pubkey()   # wallet entitled to creator fee revenue (usually == user)
    timestamp     = r.i64()      # we'll overwrite with blockTime for consistency
    v_tok         = r.u64()      # virtual_token_reserves
    v_sol         = r.u64()      # virtual_sol_reserves
    return {
        "tx_type":                  "create",
        "is_buy":                   None,
        "mint":                     mint,
        "trader":                   user,
        "coin_creator":             creator,
        "bonding_curve":            bonding_curve,
        "sol_amount":               0,
        "token_amount":             0,
        "in_bonding_curve":         True,
        "v_sol_in_bonding_curve":   v_sol,
        "v_tok_in_bonding_curve":   v_tok,
        # Extra metadata only on create records
        "_name":   name,
        "_symbol": symbol,
        "_uri":    uri,
    }


def _decode_complete(r: _Reader) -> dict:
    user          = r.pubkey()
    mint          = r.pubkey()
    bonding_curve = r.pubkey()
    timestamp     = r.i64()
    return {
        "tx_type":                  "complete",
        "is_buy":                   None,
        "mint":                     mint,
        "trader":                   user,
        "coin_creator":             None,
        "bonding_curve":            bonding_curve,
        "sol_amount":               0,
        "token_amount":             0,
        "in_bonding_curve":         False,   # just left the bonding curve
        "v_sol_in_bonding_curve":   0,
        "v_tok_in_bonding_curve":   0,
    }


# ── is_bot detection ──────────────────────────────────────────────────────────

def _detect_is_bot_for(log_messages: list[str], program_id: str) -> Optional[bool]:
    """
    depth == 1 → called directly (no outer program) → likely bot
    depth >= 2 → called through another program / dApp router → likely human
    """
    marker = f"{program_id} invoke ["
    for msg in log_messages:
        if marker in msg:
            try:
                depth_str = msg.split("invoke [")[1].rstrip("]").strip()
                return int(depth_str) == 1
            except (IndexError, ValueError):
                pass
    return None


def _detect_is_bot(log_messages: list[str]) -> Optional[bool]:
    return _detect_is_bot_for(log_messages, PUMPFUN_PROGRAM)


# ── Main entry point ──────────────────────────────────────────────────────────

def decode_transaction(tx: dict, signature: str) -> list[dict]:
    """
    Decode all pump.fun events from a single transaction.

    Returns a list of flat record dicts ready to be saved to Parquet.
    Returns [] for failed transactions or transactions without pump.fun events.
    """
    if tx is None:
        return []

    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        return []   # transaction failed on-chain

    log_messages: list[str] = meta.get("logMessages") or []
    slot       = tx.get("slot", 0)
    block_time = tx.get("blockTime") or 0

    is_bot = _detect_is_bot(log_messages)
    records: list[dict] = []

    for msg in log_messages:
        if not msg.startswith("Program data: "):
            continue

        try:
            raw = base64.b64decode(msg[len("Program data: "):])
        except Exception:
            continue

        if len(raw) < 8:
            continue

        disc = raw[:8]
        r    = _Reader(raw[8:])

        try:
            if disc == TRADE_DISC:
                event = _decode_trade(r)
            elif disc == CREATE_DISC:
                event = _decode_create(r)
            elif disc == COMPLETE_DISC:
                event = _decode_complete(r)
            else:
                continue
        except Exception as exc:
            logger.debug(f"Could not decode event in {signature}: {exc}")
            continue

        event["signature"] = signature
        event["slot"]      = slot
        event["timestamp"] = block_time
        event["is_bot"]    = is_bot
        event["source"]    = "pumpfun"
        records.append(event)

    return records


# ── PumpSwap AMM (post-graduation) ────────────────────────────────────────────
#
# Program: pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA
# Terminology: base = token (memecoin), quote = SOL (WSOL)
#
# Field layout verified against pump-fun/pump-public-docs idl/pump_amm.json
# history (stable prefix from commit 33590255fe, 2025-10-17, through at least
# commit 3c6721a67c, 2026-05-18 — every revision in that range only appends
# fields after coin_creator_fee, e.g. track_volume, ix_name, cashback/buyback
# fields — safe to ignore since Borsh reads don't require consuming the full
# buffer). A prior version of this decoder had both the wrong field count and
# wrong field order (confirmed by decoding a real Dec 2025 mainnet transaction:
# the discriminator matched but parsing overran the buffer) — re-verify against
# a real transaction (see collectors/decoder.py git history for the probe
# script) if a BufferError ever reappears here, since the program can still
# change its event layout going forward.

PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

PS_BUY_DISC         = _event_disc("BuyEvent")
PS_SELL_DISC        = _event_disc("SellEvent")
PS_CREATE_POOL_DISC = _event_disc("CreatePoolEvent")


def _decode_ps_buy(r: _Reader) -> dict:
    timestamp                  = r.i64()
    base_amount_out            = r.u64()   # tokens received by user
    _max_quote_amount_in       = r.u64()   # max SOL willing to pay (slippage limit)
    _user_base_token_reserves  = r.u64()
    _user_quote_token_reserves = r.u64()
    pool_base_reserves         = r.u64()   # token reserves in pool after trade
    pool_quote_reserves        = r.u64()   # SOL reserves in pool after trade
    quote_amount_in            = r.u64()   # actual SOL paid (incl. fees)
    _lp_fee_bps                = r.u64()
    lp_fee                     = r.u64()
    _protocol_fee_bps          = r.u64()
    protocol_fee               = r.u64()
    _quote_amount_in_w_lp_fee  = r.u64()
    _user_quote_amount_in      = r.u64()
    pool                       = r.pubkey()
    user                       = r.pubkey()
    _user_base_ta              = r.pubkey()
    _user_quote_ta             = r.pubkey()
    _prot_fee_recip            = r.pubkey()
    _prot_fee_recip_ta         = r.pubkey()
    coin_creator                = r.pubkey()
    _coin_creator_fee_bps       = r.u64()
    coin_creator_fee            = r.u64()
    return {
        "tx_type":               "buy",
        "is_buy":                True,
        "mint":                  None,   # filled by pool→mint lookup in DayStore
        "pool":                  pool,
        "trader":                user,
        "coin_creator":          coin_creator,
        "bonding_curve":         None,
        "sol_amount":            quote_amount_in,
        "token_amount":          base_amount_out,
        "in_bonding_curve":      False,
        "v_sol_in_bonding_curve": pool_quote_reserves,
        "v_tok_in_bonding_curve": pool_base_reserves,
        "lp_fee":                lp_fee,
        "protocol_fee":          protocol_fee,
        "coin_creator_fee":      coin_creator_fee,
        "source":                "pumpswap",
    }


def _decode_ps_sell(r: _Reader) -> dict:
    timestamp                  = r.i64()
    base_amount_in             = r.u64()   # tokens sold by user
    _min_quote_amount_out      = r.u64()   # min SOL willing to accept (slippage limit)
    _user_base_token_reserves  = r.u64()
    _user_quote_token_reserves = r.u64()
    pool_base_reserves         = r.u64()
    pool_quote_reserves        = r.u64()
    quote_amount_out           = r.u64()   # actual SOL received
    _lp_fee_bps                = r.u64()
    lp_fee                     = r.u64()
    _protocol_fee_bps          = r.u64()
    protocol_fee               = r.u64()
    _quote_amount_out_wo_lp    = r.u64()
    _user_quote_amount_out     = r.u64()
    pool                       = r.pubkey()
    user                       = r.pubkey()
    _user_base_ta              = r.pubkey()
    _user_quote_ta             = r.pubkey()
    _prot_fee_recip            = r.pubkey()
    _prot_fee_recip_ta         = r.pubkey()
    coin_creator                = r.pubkey()
    _coin_creator_fee_bps       = r.u64()
    coin_creator_fee            = r.u64()
    return {
        "tx_type":               "sell",
        "is_buy":                False,
        "mint":                  None,
        "pool":                  pool,
        "trader":                user,
        "coin_creator":          coin_creator,
        "bonding_curve":         None,
        "sol_amount":            quote_amount_out,
        "token_amount":          base_amount_in,
        "in_bonding_curve":      False,
        "v_sol_in_bonding_curve": pool_quote_reserves,
        "v_tok_in_bonding_curve": pool_base_reserves,
        "lp_fee":                lp_fee,
        "protocol_fee":          protocol_fee,
        "coin_creator_fee":      coin_creator_fee,
        "source":                "pumpswap",
    }


def _decode_ps_create_pool(r: _Reader) -> dict:
    # `creator` (field 3) is the wallet/PDA that submitted the pool-creation tx
    # (graduation is protocol-triggered, so this is typically pump.fun's own
    # authority, not the token's creator). `coin_creator` (field 20) is the
    # token's actual creator, entitled to post-graduation fee revenue — these
    # were previously conflated by reusing one misaligned `creator` variable
    # for both, which also silently shifted every field read after
    # pool_quote_amount since three u64 fields and a u8 (minimum_liquidity,
    # initial_liquidity, lp_token_amount_out, pool_bump) sit between it and
    # `pool` in the actual IDL.
    timestamp            = r.i64()
    _index               = r.u16()   # pool index (auto-incrementing)
    creator              = r.pubkey()
    base_mint            = r.pubkey()   # the graduated token's mint
    _quote_mint          = r.pubkey()   # WSOL
    _base_decimals       = r.u8()
    _quote_decimals      = r.u8()
    base_amount_in       = r.u64()   # tokens seeded into pool
    quote_amount_in      = r.u64()   # SOL seeded into pool
    pool_base_reserves   = r.u64()
    pool_quote_reserves  = r.u64()
    _minimum_liquidity   = r.u64()
    _initial_liquidity   = r.u64()
    _lp_token_amount_out = r.u64()
    _pool_bump           = r.u8()
    pool                 = r.pubkey()
    _lp_mint             = r.pubkey()
    _user_base_ta        = r.pubkey()
    _user_quote_ta       = r.pubkey()
    coin_creator         = r.pubkey()
    return {
        "tx_type":               "pool_create",
        "is_buy":                None,
        "mint":                  base_mint,
        "pool":                  pool,
        "trader":                creator,
        "coin_creator":          coin_creator,
        "bonding_curve":         None,
        "sol_amount":            quote_amount_in,
        "token_amount":          base_amount_in,
        "in_bonding_curve":      False,
        "v_sol_in_bonding_curve": pool_quote_reserves,
        "v_tok_in_bonding_curve": pool_base_reserves,
        "lp_fee":                0,
        "protocol_fee":          0,
        "coin_creator_fee":      0,
        "source":                "pumpswap",
    }


def decode_pumpswap_transaction(tx: dict, signature: str) -> list[dict]:
    """
    Decode all PumpSwap AMM events from a single transaction.

    Returns [] for failed transactions or those without PumpSwap events.
    """
    if tx is None:
        return []

    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        return []

    log_messages: list[str] = meta.get("logMessages") or []
    slot       = tx.get("slot", 0)
    block_time = tx.get("blockTime") or 0
    is_bot     = _detect_is_bot_for(log_messages, PUMPSWAP_PROGRAM)
    records: list[dict] = []

    for msg in log_messages:
        if not msg.startswith("Program data: "):
            continue
        try:
            raw = base64.b64decode(msg[len("Program data: "):])
        except Exception:
            continue

        if len(raw) < 8:
            continue

        disc = raw[:8]
        r    = _Reader(raw[8:])

        try:
            if disc == PS_BUY_DISC:
                event = _decode_ps_buy(r)
            elif disc == PS_SELL_DISC:
                event = _decode_ps_sell(r)
            elif disc == PS_CREATE_POOL_DISC:
                event = _decode_ps_create_pool(r)
            else:
                continue
        except Exception as exc:
            logger.debug(f"Could not decode PumpSwap event in {signature}: {exc}")
            continue

        event["signature"] = signature
        event["slot"]      = slot
        event["timestamp"] = block_time
        event["is_bot"]    = is_bot
        records.append(event)

    return records
