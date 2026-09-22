"""
collectors/metadata_source.py
-----------------------------
Phase C ingestion adapter: resolves `tokens.uri` -> off-chain metadata JSON ->
token image, plus pump.fun's mutable moderation flags.

The three things that are not obvious, all established by measurement (see
docs/metadata_image_collection_plan.md):

1. `tokens.uri` is a CID carrier, not a URL. ~70% of rows point at ipfs.io,
   which now returns HTTP 429 for everything. The CID is extracted and walked
   through config.METADATA_GATEWAYS. pump.mypinata.cloud (pump.fun's own
   gateway) serves 93%; the rest are CIDs pinned by third-party launchpads,
   and every one of them was recoverable from the fallback gateways.

2. Images are downloaded whole and resized locally, rather than using the
   gateway's server-side transform. The transform is 12x less bandwidth, but
   it resizes before we see any bytes and leaks no source dimensions in the
   response headers, so preserving the original dimensions would cost a second
   request — at which point downloading the original outright is *faster*
   (26.7 vs 19.6 tok/s), smaller on disk (local q85 beats Cloudflare's default
   quality) and simpler. IMAGE_STRATEGY="transform" selects the low-bandwidth
   path for metered connections.

3. Dimensions come from a header-only parse of the first few KB (see
   image_headers.parse_dims), which is what makes the streaming size guard
   possible: oversized originals are identified from their headers, aborted
   mid-stream, and fetched through the transform instead.
"""

import io
import json
import logging
import re
import threading
import time
from typing import Iterator

import httpx
from PIL import Image

from collectors.image_headers import parse_dims
from config import (
    CRAWLER_UA,
    DEAD_IPFS_HOSTS,
    GATEWAY_RATE_LIMIT,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    IMG_HEADER_PREFIX,
    IMG_MAX_ORIG_BYTES,
    IMG_TARGET_PX,
    IMG_TRANSFORM_PARAMS,
    IMG_WEBP_QUALITY,
    METADATA_GATEWAYS,
    PUMP_API_COIN,
    PUMP_API_RATE_LIMIT,
)

logger = logging.getLogger(__name__)

# CIDv0 is 46 chars (Qm…), CIDv1 base32 is 59 (bafy…/bafk…). The {40,} floor
# keeps short path segments from being mistaken for a CID.
CID_RE = re.compile(r"/ipfs/([A-Za-z0-9]{40,})")
IPFS_SCHEME_RE = re.compile(r"^ipfs://(?:ipfs/)?([A-Za-z0-9]{40,})")

# Retried with backoff; anything else is taken at face value.
TRANSIENT_STATUS = {408, 425, 429, 500, 502, 503, 504, 522, 524}


# ── Rate limiting ─────────────────────────────────────────────────────────────

class RateLimiter:
    """Thread-safe token bucket. One per host, so gateways are capped separately."""

    def __init__(self, rate_per_sec: float, burst: float | None = None):
        self.rate = max(rate_per_sec, 0.01)
        self.capacity = burst if burst is not None else max(self.rate, 1.0)
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self.rate
            time.sleep(min(wait, 1.0))


class _Limiters:
    def __init__(self):
        self._by_host: dict[str, RateLimiter] = {}
        self._lock = threading.Lock()

    def for_url(self, url: str) -> RateLimiter:
        try:
            host = url.split("/")[2]
        except IndexError:
            host = "?"
        with self._lock:
            lim = self._by_host.get(host)
            if lim is None:
                rate = PUMP_API_RATE_LIMIT if "pump.fun" in host else GATEWAY_RATE_LIMIT
                lim = RateLimiter(rate)
                self._by_host[host] = lim
            return lim


LIMITERS = _Limiters()


# ── HTTP ──────────────────────────────────────────────────────────────────────

def make_client(max_connections: int = 32) -> httpx.Client:
    return httpx.Client(
        timeout=HTTP_TIMEOUT,
        headers={"user-agent": CRAWLER_UA},
        follow_redirects=True,
        limits=httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_connections,
        ),
    )


def _sleep_for_retry(resp: httpx.Response | None, attempt: int) -> None:
    """Honour Retry-After when the server sends it, else exponential backoff."""
    if resp is not None:
        ra = resp.headers.get("retry-after")
        if ra:
            try:
                time.sleep(min(float(ra), 60.0))
                return
            except ValueError:
                pass
    time.sleep(min(2.0 ** attempt, 30.0))


def get(client: httpx.Client, url: str, **kwargs) -> tuple[httpx.Response | None, str | None]:
    """
    Rate-limited GET with retries on transient failures.
    Returns (response, error_kind). A non-transient HTTP error still comes back
    as a response — the caller decides what a 404 means.
    """
    last_err = None
    for attempt in range(HTTP_RETRIES):
        LIMITERS.for_url(url).acquire()
        try:
            resp = client.get(url, **kwargs)
        except httpx.TimeoutException:
            last_err = "timeout"
            _sleep_for_retry(None, attempt)
            continue
        except httpx.HTTPError:
            last_err = "http_error"
            _sleep_for_retry(None, attempt)
            continue
        if resp.status_code in TRANSIENT_STATUS and attempt < HTTP_RETRIES - 1:
            _sleep_for_retry(resp, attempt)
            continue
        return resp, None
    return None, last_err


# ── URL / CID handling ────────────────────────────────────────────────────────

def extract_cid(url: str | None) -> str | None:
    if not url:
        return None
    m = IPFS_SCHEME_RE.match(url)
    if m:
        return m.group(1)
    m = CID_RE.search(url)
    return m.group(1) if m else None


def _host_of(url: str) -> str:
    try:
        return url.split("/")[2]
    except IndexError:
        return ""


def candidate_urls(uri: str) -> Iterator[tuple[str, str]]:
    """
    Yield (url, gateway_label) to try, in order.

    A CID means the content is addressable anywhere, so we prefer the gateway
    pool and ignore whatever host the token happened to record. Without a CID
    there is only the original URL.
    """
    cid = extract_cid(uri)
    host = _host_of(uri)

    if cid:
        for gw in METADATA_GATEWAYS:
            yield gw + cid, _host_of(gw)
        # Only worth re-trying the recorded host if it isn't known-dead and
        # isn't already in the pool.
        if host and host not in DEAD_IPFS_HOSTS and not any(host in gw for gw in METADATA_GATEWAYS):
            yield uri, host
    elif uri.startswith("http"):
        yield uri, host


# ── Metadata JSON ─────────────────────────────────────────────────────────────

_KNOWN_KEYS = {
    "name", "symbol", "description", "image", "showName", "createdOn",
    "twitter", "telegram", "website", "coin_community", "video", "banner",
}


def fetch_metadata(client: httpx.Client, mint: str, uri: str | None) -> dict:
    """Resolve a token's metadata JSON. Always returns a token_metadata row."""
    now = int(time.time())
    row = {
        "mint": mint, "uri": uri, "fetched_url": None, "gateway": None,
        "meta_name": None, "meta_symbol": None, "description": None,
        "image_url": None, "image_cid": None, "twitter": None, "telegram": None,
        "website": None, "coin_community": None, "video_url": None,
        "banner_url": None, "show_name": None, "created_on": None,
        "extra_json": None, "status": "no_uri", "http_status": None,
        "attempts": 0, "resolved_at": now,
    }
    if not uri or not isinstance(uri, str) or not uri.strip():
        return row

    attempts = 0
    last_status = None
    last_err = None

    for url, gw in candidate_urls(uri):
        attempts += 1
        resp, err = get(client, url)
        if resp is None:
            last_err = err
            continue
        last_status = resp.status_code
        if resp.status_code != 200:
            continue
        try:
            js = resp.json()
        except (json.JSONDecodeError, ValueError):
            row.update(status="invalid_json", http_status=200,
                       fetched_url=url, gateway=gw, attempts=attempts)
            return row
        if not isinstance(js, dict):
            row.update(status="invalid_json", http_status=200,
                       fetched_url=url, gateway=gw, attempts=attempts)
            return row

        image_url = js.get("image")
        extra = {k: v for k, v in js.items() if k not in _KNOWN_KEYS}
        row.update(
            fetched_url=url, gateway=gw, status="ok", http_status=200,
            attempts=attempts, resolved_at=int(time.time()),
            meta_name=js.get("name"), meta_symbol=js.get("symbol"),
            description=js.get("description"), image_url=image_url,
            image_cid=extract_cid(image_url), twitter=js.get("twitter"),
            telegram=js.get("telegram"), website=js.get("website"),
            coin_community=js.get("coin_community"), video_url=js.get("video"),
            banner_url=js.get("banner"), show_name=js.get("showName"),
            created_on=js.get("createdOn"),
            extra_json=json.dumps(extra, separators=(",", ":")) if extra else None,
        )
        return row

    row.update(
        status="timeout" if last_err == "timeout" else (
            "all_gateways_failed" if attempts > 1 else "http_error"),
        http_status=last_status, attempts=attempts,
    )
    return row


# ── Images ────────────────────────────────────────────────────────────────────

def transform_url(cid: str) -> str:
    params = IMG_TRANSFORM_PARAMS.format(px=IMG_TARGET_PX)
    return f"{METADATA_GATEWAYS[0]}{cid}?{params}"


def local_resize(blob: bytes) -> tuple[bytes, int, int, int, bool, int, int, str | None]:
    """
    Re-encode to a max-IMG_TARGET_PX-side WebP.
    Returns (encoded, width, height, n_frames, is_animated, orig_w, orig_h, orig_format).

    The original size comes from the decoded image rather than a header parse:
    when we hold the whole file, Pillow's answer is authoritative and costs
    nothing extra. parse_dims() is for the paths where we deliberately don't
    have the whole file.

    Animation is flattened to frame 0 — the flag and frame count survive in
    the ledger even though the animation does not.
    """
    im = Image.open(io.BytesIO(blob))
    orig_w, orig_h = im.size
    orig_format = im.format
    n_frames = getattr(im, "n_frames", 1) or 1
    is_animated = bool(getattr(im, "is_animated", False)) or n_frames > 1
    try:
        im.seek(0)
    except (EOFError, ValueError):
        pass
    # Palette images carrying transparency in a tCRN/tRNS byte warn (and lose
    # the alpha) on a direct ->RGB convert; go through RGBA first.
    if im.mode in ("P", "PA"):
        im = im.convert("RGBA")
    if im.mode != "RGB":
        im = im.convert("RGB")
    im.thumbnail((IMG_TARGET_PX, IMG_TARGET_PX), Image.LANCZOS)
    out = io.BytesIO()
    im.save(out, "WEBP", quality=IMG_WEBP_QUALITY, method=4)
    return (out.getvalue(), im.width, im.height, n_frames, is_animated,
            orig_w, orig_h, orig_format)


def _blank_image_row(mint: str, image_url: str | None, cid: str | None, status: str) -> dict:
    return {
        "mint": mint, "image_url": image_url, "image_cid": cid, "sha256": None,
        "shard": None, "status": status, "http_status": None, "strategy": None,
        "orig_bytes": None, "orig_width": None, "orig_height": None,
        "orig_format": None, "is_animated": None, "n_frames": None,
        "stored_bytes": None, "stored_width": None, "stored_height": None,
        "fetched_at": int(time.time()),
    }


def _fetch_transform(client: httpx.Client, cid: str) -> tuple[bytes | None, int | None, str | None]:
    """Gateway-side resize. Returns (webp_bytes, http_status, error_kind)."""
    resp, err = get(client, transform_url(cid))
    if resp is None:
        return None, None, err or "http_error"
    if resp.status_code != 200:
        return None, resp.status_code, "http_error"
    return resp.content, 200, None


def fetch_image(
    client: httpx.Client,
    mint: str,
    image_url: str | None,
    cid: str | None,
    strategy: str = "original",
) -> tuple[dict, bytes | None, str | None]:
    """
    Fetch and re-encode a token image.
    Returns (token_images row, encoded_bytes|None, content_type|None).
    The caller stores the bytes and fills in sha256/shard.
    """
    # Callers read these back out of Parquet, where a missing value arrives as
    # NaN rather than None — never trust them to be str-or-None.
    if not isinstance(cid, str) or not cid:
        cid = None
    if not image_url or not isinstance(image_url, str) or not image_url.strip():
        return _blank_image_row(mint, image_url if isinstance(image_url, str) else None,
                                cid, "no_url"), None, None

    row = _blank_image_row(mint, image_url, cid, "http_error")

    # ── low-bandwidth path: let the gateway resize ────────────────────────────
    if strategy == "transform" and cid:
        # One 4KB range read keeps the original dimensions, which the transform
        # itself destroys. Costs a second request — that's the whole reason
        # "original" is the default strategy.
        hdr_resp, _ = get(client, METADATA_GATEWAYS[0] + cid,
                          headers={"Range": f"bytes=0-{IMG_HEADER_PREFIX - 1}"})
        if hdr_resp is not None and hdr_resp.status_code in (200, 206):
            _apply_header_dims(row, hdr_resp)
        data, status, err = _fetch_transform(client, cid)
        row["http_status"] = status
        row["strategy"] = "gateway_transform"
        if data is None:
            row["status"] = "timeout" if err == "timeout" else "http_error"
            return row, None, None
        try:
            im = Image.open(io.BytesIO(data))
            row.update(status="ok", stored_bytes=len(data),
                       stored_width=im.width, stored_height=im.height)
        except Exception:
            row["status"] = "decode_error"
            return row, None, None
        return row, data, "image/webp"

    # ── default path: stream the original, guard on size, resize locally ─────
    candidates: list[str] = []
    if cid:
        candidates = [gw + cid for gw in METADATA_GATEWAYS]
        if not any(_host_of(image_url) in gw for gw in METADATA_GATEWAYS) \
                and _host_of(image_url) not in DEAD_IPFS_HOSTS:
            candidates.append(image_url)
    else:
        candidates = [image_url]

    last_status = None
    last_err = None
    for url in candidates:
        blob, status, err, too_big = _stream_original(client, url, row)
        last_status, last_err = status, err
        if too_big:
            # Header prefix already recorded the original dimensions; now get
            # a bounded rendition instead of the multi-MB source.
            if cid:
                data, tstatus, terr = _fetch_transform(client, cid)
                if data is not None:
                    try:
                        im = Image.open(io.BytesIO(data))
                        row.update(status="ok", strategy="gateway_transform",
                                   http_status=tstatus, stored_bytes=len(data),
                                   stored_width=im.width, stored_height=im.height)
                        return row, data, "image/webp"
                    except Exception:
                        pass
            row.update(status="too_large", http_status=status, strategy="original_local")
            return row, None, None
        if blob is None:
            continue
        try:
            enc, w, h, n_frames, is_anim, ow, oh, ofmt = local_resize(blob)
        except Exception as exc:
            logger.debug(f"{mint}: resize failed for {url}: {type(exc).__name__}")
            row.update(status="decode_error", http_status=status, strategy="original_local")
            return row, None, None
        row.update(
            status="ok", http_status=status, strategy="original_local",
            stored_bytes=len(enc), stored_width=w, stored_height=h,
            n_frames=n_frames, is_animated=is_anim,
            orig_width=ow, orig_height=oh, orig_format=ofmt,
            orig_bytes=len(blob),
        )
        return row, enc, "image/webp"

    row.update(status="timeout" if last_err == "timeout" else "http_error",
               http_status=last_status, strategy="original_local")
    return row, None, None


def _apply_parsed_dims(row: dict, parsed) -> None:
    if not parsed:
        return
    w, h, fmt, anim = parsed
    row["orig_width"] = w
    row["orig_height"] = h
    row["orig_format"] = fmt
    if anim is not None and row.get("is_animated") is None:
        row["is_animated"] = anim


def _apply_header_dims(row: dict, resp: httpx.Response) -> None:
    """Record original size/dimensions from a header-prefix response."""
    cr = resp.headers.get("content-range")
    total = None
    if cr and "/" in cr:
        try:
            total = int(cr.rsplit("/", 1)[-1])
        except ValueError:
            pass
    if total is None:
        try:
            total = int(resp.headers.get("content-length"))
        except (TypeError, ValueError):
            total = None
    if total is not None:
        row["orig_bytes"] = total
    _apply_parsed_dims(row, parse_dims(resp.content))


def _stream_original(
    client: httpx.Client, url: str, row: dict
) -> tuple[bytes | None, int | None, str | None, bool]:
    """
    Stream an original image, aborting if it exceeds IMG_MAX_ORIG_BYTES.

    Streaming rather than a HEAD probe is deliberate: headers arrive on the
    same connection as the body, so the size guard costs no extra round trip.
    Returns (blob|None, http_status, error_kind, too_big).
    """
    for attempt in range(HTTP_RETRIES):
        LIMITERS.for_url(url).acquire()
        try:
            with client.stream("GET", url) as resp:
                if resp.status_code in TRANSIENT_STATUS and attempt < HTTP_RETRIES - 1:
                    resp.read()
                    _sleep_for_retry(resp, attempt)
                    continue
                if resp.status_code != 200:
                    resp.read()
                    return None, resp.status_code, "http_error", False

                declared = resp.headers.get("content-length")
                try:
                    declared_n = int(declared) if declared is not None else None
                except ValueError:
                    declared_n = None

                if declared_n is not None and declared_n > IMG_MAX_ORIG_BYTES:
                    # Read just enough for the header parse, then drop the
                    # connection rather than pulling the whole thing.
                    prefix = b""
                    for chunk in resp.iter_bytes(IMG_HEADER_PREFIX):
                        prefix += chunk
                        if len(prefix) >= IMG_HEADER_PREFIX:
                            break
                    row["orig_bytes"] = declared_n
                    _apply_parsed_dims(row, parse_dims(prefix))
                    return None, resp.status_code, None, True

                buf = bytearray()
                for chunk in resp.iter_bytes(65536):
                    buf += chunk
                    if len(buf) > IMG_MAX_ORIG_BYTES:
                        # Server lied about (or omitted) content-length.
                        row["orig_bytes"] = len(buf)
                        _apply_parsed_dims(row, parse_dims(bytes(buf[:IMG_HEADER_PREFIX])))
                        return None, resp.status_code, None, True
                return bytes(buf), resp.status_code, None, False
        except httpx.TimeoutException:
            _sleep_for_retry(None, attempt)
            last = "timeout"
        except httpx.HTTPError:
            _sleep_for_retry(None, attempt)
            last = "http_error"
    return None, None, locals().get("last", "http_error"), False


# ── pump.fun frontend API ─────────────────────────────────────────────────────

def fetch_pump_api(client: httpx.Client, mint: str) -> dict:
    """
    Fetch pump.fun's own record for a mint — the only source of nsfw and
    is_banned. These are mutable moderation labels as of `fetched_at`, not
    attributes of the token at creation time.
    """
    row = {
        "mint": mint, "nsfw": None, "is_banned": None, "reply_count": None,
        "market_cap": None, "ath_market_cap": None,
        "ath_market_cap_timestamp": None, "total_supply": None,
        "is_currently_live": None, "hide_banner": None, "api_image_uri": None,
        "api_metadata_uri": None, "status": "http_error", "http_status": None,
        "fetched_at": int(time.time()),
    }
    resp, err = get(client, PUMP_API_COIN + mint)
    if resp is None:
        row["status"] = "timeout" if err == "timeout" else "http_error"
        return row
    row["http_status"] = resp.status_code
    if resp.status_code == 404:
        row["status"] = "not_found"
        return row
    if resp.status_code != 200:
        return row
    try:
        js = resp.json()
    except (json.JSONDecodeError, ValueError):
        return row
    if not isinstance(js, dict):
        return row
    row.update(
        status="ok",
        nsfw=js.get("nsfw"), is_banned=js.get("is_banned"),
        reply_count=js.get("reply_count"), market_cap=js.get("market_cap"),
        ath_market_cap=js.get("ath_market_cap"),
        ath_market_cap_timestamp=js.get("ath_market_cap_timestamp"),
        total_supply=js.get("total_supply"),
        is_currently_live=js.get("is_currently_live"),
        hide_banner=js.get("hide_banner"),
        api_image_uri=js.get("image_uri"), api_metadata_uri=js.get("metadata_uri"),
        fetched_at=int(time.time()),
    )
    return row
