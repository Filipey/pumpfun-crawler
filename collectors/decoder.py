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
    mint                 = r.pubkey()
    sol_amount           = r.u64()
    token_amount         = r.u64()
    is_buy               = r.bool_()
    user                 = r.pubkey()
    timestamp            = r.i64()
    v_sol                = r.u64()
    v_tok                = r.u64()
    # Some program versions include real reserves — read if bytes remain
    return {
        "tx_type":                  "buy" if is_buy else "sell",
        "is_buy":                   is_buy,
        "mint":                     mint,
        "trader":                   user,
        "coin_creator":             None,   # enriched later via CreateEvent join
        "bonding_curve":            None,
        "sol_amount":               sol_amount,
        "token_amount":             token_amount,
        "in_bonding_curve":         True,
        "v_sol_in_bonding_curve":   v_sol,
        "v_tok_in_bonding_curve":   v_tok,
    }


def _decode_create(r: _Reader) -> dict:
    name          = r.string()
    symbol        = r.string()
    uri           = r.string()
    mint          = r.pubkey()
    bonding_curve = r.pubkey()
    user          = r.pubkey()   # creator
    timestamp     = r.i64()      # we'll overwrite with blockTime for consistency
    v_sol         = r.u64()
    v_tok         = r.u64()
    return {
        "tx_type":                  "create",
        "is_buy":                   None,
        "mint":                     mint,
        "trader":                   user,
        "coin_creator":             user,   # same wallet creates and is the creator
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
# Field layout matches the PumpSwap Anchor IDL (published March 2025).
# VALIDATE against a real transaction if you suspect a field mismatch:
#   python - <<'EOF'
#   import base64, requests
#   tx = requests.post(RPC_URL, json={"jsonrpc":"2.0","id":1,
#       "method":"getTransaction","params":[KNOWN_SIG,
#       {"encoding":"json","maxSupportedTransactionVersion":0}]}).json()
#   for m in tx["result"]["meta"]["logMessages"]:
#       if m.startswith("Program data: "):
#           print(base64.b64decode(m[14:]).hex())
#   EOF

PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

PS_BUY_DISC         = _event_disc("BuyEvent")
PS_SELL_DISC        = _event_disc("SellEvent")
PS_CREATE_POOL_DISC = _event_disc("CreatePoolEvent")


def _decode_ps_buy(r: _Reader) -> dict:
    timestamp            = r.i64()
    base_amount_out      = r.u64()   # tokens received by user
    _max_quote_in        = r.u64()   # max SOL willing to pay (slippage limit)
    _user_base_reserves  = r.u64()
    _user_quote_reserves = r.u64()
    pool_base_reserves   = r.u64()   # token reserves in pool after trade
    pool_quote_reserves  = r.u64()   # SOL reserves in pool after trade
    quote_amount_in      = r.u64()   # actual SOL paid (incl. fees)
    _lp_fee_bps          = r.u64()
    _protocol_fee_bps    = r.u64()
    _quote_input         = r.u64()
    lp_fee               = r.u64()
    protocol_fee         = r.u64()
    user                 = r.pubkey()
    pool                 = r.pubkey()
    _user_base_ta        = r.pubkey()
    _user_quote_ta       = r.pubkey()
    _pool_base_ta        = r.pubkey()
    _pool_quote_ta       = r.pubkey()
    _prot_fee_recip      = r.pubkey()
    _prot_fee_recip_ta   = r.pubkey()
    coin_creator         = r.pubkey()
    _creator_fee_ta      = r.pubkey()
    coin_creator_fee     = r.u64()
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
    timestamp            = r.i64()
    base_amount_in       = r.u64()   # tokens sold by user
    _min_quote_out       = r.u64()   # min SOL willing to accept (slippage limit)
    _user_base_reserves  = r.u64()
    _user_quote_reserves = r.u64()
    pool_base_reserves   = r.u64()
    pool_quote_reserves  = r.u64()
    quote_amount_out     = r.u64()   # actual SOL received
    _lp_fee_bps          = r.u64()
    _protocol_fee_bps    = r.u64()
    _quote_output        = r.u64()
    lp_fee               = r.u64()
    protocol_fee         = r.u64()
    user                 = r.pubkey()
    pool                 = r.pubkey()
    _user_base_ta        = r.pubkey()
    _user_quote_ta       = r.pubkey()
    _pool_base_ta        = r.pubkey()
    _pool_quote_ta       = r.pubkey()
    _prot_fee_recip      = r.pubkey()
    _prot_fee_recip_ta   = r.pubkey()
    coin_creator         = r.pubkey()
    _creator_fee_ta      = r.pubkey()
    coin_creator_fee     = r.u64()
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
    pool                 = r.pubkey()
    _lp_mint             = r.pubkey()
    _user_pool_ta        = r.pubkey()
    _pool_base_ta        = r.pubkey()
    _pool_quote_ta       = r.pubkey()
    _user_base_ta        = r.pubkey()
    _user_quote_ta       = r.pubkey()
    return {
        "tx_type":               "pool_create",
        "is_buy":                None,
        "mint":                  base_mint,
        "pool":                  pool,
        "trader":                creator,
        "coin_creator":          creator,
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
