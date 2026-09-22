"""
collectors/image_headers.py
---------------------------
Header-only image dimension parser. Reads width/height/format straight out of
the container header, without decoding pixels and without needing the whole
file — a 4 KB prefix is enough for every format observed in pump.fun token
images (PNG, JPEG, GIF, WebP, AVIF).

Why not Pillow
--------------
Pillow cannot parse a truncated WebP: libwebp wants the complete file, so
`Image.open()` on a 4 KB prefix raised OSError on 34/34 WebP images in the
sampling run. Since WebP is ~1/3 of pump.fun token images, that makes Pillow
unusable for the "fetch only the header to learn the original size" path that
`metadata_source.fetch_image()` needs when it declines to download a huge
original in full.

Validated against full-download ground truth on 99 real token images:

    prefix   parsed   dimensions correct   format correct
      512B    82/99             82               82
     1024B    96/99             96               96
     4096B    99/99             99               99

It returns None rather than guessing — at smaller prefixes it misses JPEGs
(whose SOF marker can sit deep behind long EXIF segments) but it never
returned a *wrong* answer at any prefix.
"""

import struct

__all__ = ["parse_dims"]


def parse_dims(b: bytes) -> tuple[int, int, str, bool | None] | None:
    """
    Parse (width, height, format, is_animated) from an image header prefix.

    `is_animated` is None when the container cannot answer cheaply (GIF, where
    the frame count needs a full block scan — take n_frames from Pillow during
    re-encode instead). Returns None if the bytes aren't a recognised image or
    the prefix is too short to be sure.
    """
    n = len(b)

    # ── PNG: 8-byte signature, then the IHDR chunk at a fixed offset ─────────
    if n >= 24 and b[:8] == b"\x89PNG\r\n\x1a\n" and b[12:16] == b"IHDR":
        w, h = struct.unpack(">II", b[16:24])
        return w, h, "PNG", b"acTL" in b[:n]          # acTL chunk => APNG

    # ── GIF: dimensions live in the logical screen descriptor ────────────────
    if n >= 10 and b[:3] == b"GIF":
        w, h = struct.unpack("<HH", b[6:10])
        return w, h, "GIF", None                       # frame count needs a full scan

    # ── WebP: RIFF <u32 size> WEBP <chunk fourcc> ────────────────────────────
    if n >= 16 and b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        chunk = b[12:16]
        if chunk == b"VP8 " and n >= 30 and b[23:26] == b"\x9d\x01\x2a":   # lossy
            w = struct.unpack("<H", b[26:28])[0] & 0x3FFF
            h = struct.unpack("<H", b[28:30])[0] & 0x3FFF
            return w, h, "WEBP", False
        if chunk == b"VP8L" and n >= 25 and b[20] == 0x2F:                 # lossless
            bits = struct.unpack("<I", b[21:25])[0]
            return (bits & 0x3FFF) + 1, ((bits >> 14) & 0x3FFF) + 1, "WEBP", False
        if chunk == b"VP8X" and n >= 30:                                   # extended
            w = int.from_bytes(b[24:27], "little") + 1
            h = int.from_bytes(b[27:30], "little") + 1
            return w, h, "WEBP", bool(b[20] & 0x02)    # bit 1 of the flags byte = ANIM

    # ── JPEG: walk the marker segments looking for any SOFn ──────────────────
    if n >= 4 and b[:2] == b"\xff\xd8":
        i = 2
        while i + 9 < n:
            if b[i] != 0xFF:
                i += 1
                continue
            marker = b[i + 1]
            if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7:   # standalone, no length
                i += 2
                continue
            if i + 4 > n:
                break
            seglen = struct.unpack(">H", b[i + 2:i + 4])[0]
            # SOF0..SOF15, excluding DHT (C4), JPG (C8) and DAC (CC)
            if 0xC0 <= marker <= 0xCF and marker not in (0xC4, 0xC8, 0xCC):
                if i + 9 <= n:
                    h, w = struct.unpack(">HH", b[i + 5:i + 9])
                    return w, h, "JPEG", False
                break
            i += 2 + seglen

    # ── AVIF / HEIF (ISOBMFF): find the 'ispe' (image spatial extents) box ───
    if n >= 12 and b[4:8] == b"ftyp":
        brand = b[8:12]
        k = b.find(b"ispe")
        if k != -1 and k + 16 <= n:
            w, h = struct.unpack(">II", b[k + 8:k + 16])
            fmt = "AVIF" if brand in (b"avif", b"avis") else "HEIF"
            return w, h, fmt, brand == b"avis"

    # ── BMP ──────────────────────────────────────────────────────────────────
    if n >= 26 and b[:2] == b"BM":
        w, h = struct.unpack("<ii", b[18:26])
        return abs(w), abs(h), "BMP", False

    # SVG carries no guaranteed intrinsic pixel size — treat as unparseable.
    head = b[:512].lstrip()
    if head[:5] == b"<?xml" or head[:4] == b"<svg":
        return None

    return None
