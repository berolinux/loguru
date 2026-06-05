"""Binary frame encoding / decoding for log records.

Frame layout written into the ring buffer
------------------------------------------

Every frame is 8-byte aligned.  The on-disk byte layout is::

    Offset  Size   Field
    ------  -----  -------------------------------------------
    0       1      status   (0x00 free, 0x01 writing, 0x02 ready, 0xFE padding)
    1       4      payload_len  (uint32 LE – byte length of *payload* only)
    5       4      crc32        (uint32 LE – CRC-32 of *payload* bytes)
    9       N      payload      (N = payload_len)
    9+N     pad    zero-padding to next 8-byte boundary

Total frame size = align8(9 + payload_len).

Log-record payload (inside the frame)
--------------------------------------

All multi-byte integers are **little-endian**.

    Offset  Size   Field
    ------  -----  -------------------------
    0       8      timestamp_ns   (int64  – nanoseconds since Unix epoch)
    8       1      level_no       (uint8)
    9       4      pid            (uint32)
    13      8      tid            (uint64)
    21      4      line_no        (uint32)
    25      4      msg_len        (uint32)
    29      msg_len  message      (UTF-8 bytes)
    …       2      src_len        (uint16)
    …       src_len  source       (UTF-8  "filename:function")
"""

import struct
import zlib

# ── frame-level constants ───────────────────────────────────────────
FRAME_HDR_SIZE = 9          # status(1) + payload_len(4) + crc32(4)
FRAME_ALIGN = 8

STATUS_FREE = 0x00
STATUS_WRITING = 0x01
STATUS_READY = 0x02
STATUS_PADDING = 0xFE

# ── record-level constants ──────────────────────────────────────────
_RECORD_HDR = struct.Struct("<qBIQI")   # 8+1+4+8+4 = 25 bytes


def frame_total_size(payload_len):
    """Aligned total size of a frame (header + payload + padding)."""
    return (FRAME_HDR_SIZE + payload_len + FRAME_ALIGN - 1) & ~(FRAME_ALIGN - 1)


def compute_crc32(data):
    return zlib.crc32(data) & 0xFFFFFFFF


# ── record encoding ─────────────────────────────────────────────────

def encode_record(record):
    """Pack a loguru *record* dict into a compact binary payload (bytes).

    The returned bytes are the *payload* that goes inside a ring-buffer frame.
    """
    ts_ns = int(record["time"].timestamp() * 1_000_000_000)
    level_no = record["level"].no & 0xFF
    pid = record["process"].id & 0xFFFFFFFF
    tid = record["thread"].id & 0xFFFFFFFFFFFFFFFF
    line = (record["line"] or 0) & 0xFFFFFFFF

    msg = str(record["message"]).encode("utf-8", errors="replace")
    fname = record["file"].name if record["file"] else ""
    func = record["function"] or ""
    source = f"{fname}:{func}".encode("utf-8", errors="replace")

    hdr = _RECORD_HDR.pack(ts_ns, level_no, pid, tid, line)
    return b"".join([
        hdr,
        struct.pack("<I", len(msg)),
        msg,
        struct.pack("<H", len(source)),
        source,
    ])


def decode_record(data):
    """Unpack bytes produced by *encode_record* back into a plain dict."""
    if len(data) < _RECORD_HDR.size:
        raise ValueError("payload too short for record header")
    ts_ns, level_no, pid, tid, line = _RECORD_HDR.unpack_from(data, 0)
    off = _RECORD_HDR.size

    msg_len = struct.unpack_from("<I", data, off)[0]
    off += 4
    message = data[off : off + msg_len].decode("utf-8", errors="replace")
    off += msg_len

    src_len = struct.unpack_from("<H", data, off)[0]
    off += 2
    source = data[off : off + src_len].decode("utf-8", errors="replace")

    return {
        "timestamp_ns": ts_ns,
        "level_no": level_no,
        "pid": pid,
        "tid": tid,
        "line": line,
        "message": message,
        "source": source,
    }


def encode_raw_message(msg_bytes):
    """Minimal encoder: just wraps an arbitrary byte string as the payload.

    Useful for tests that don't need full record structure.
    """
    return msg_bytes


def decode_raw_message(data):
    """Inverse of *encode_raw_message*."""
    return data
