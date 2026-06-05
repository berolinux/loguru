"""Tests for the binary frame encoding / decoding layer."""

import struct
import time
from datetime import datetime, timezone

import pytest

from ring_buffer_sink._frames import (
    FRAME_ALIGN,
    FRAME_HDR_SIZE,
    STATUS_FREE,
    STATUS_PADDING,
    STATUS_READY,
    STATUS_WRITING,
    compute_crc32,
    decode_record,
    encode_record,
    frame_total_size,
)


# ── helpers ─────────────────────────────────────────────────────────

class _FakeLevel:
    def __init__(self, no):
        self.no = no

class _FakeProcess:
    def __init__(self, pid):
        self.id = pid

class _FakeThread:
    def __init__(self, tid):
        self.id = tid

class _FakeFile:
    def __init__(self, name):
        self.name = name


def _mock_record(message="hello", level_no=20, pid=1234, tid=9999,
                 line=42, filename="app.py", function="run"):
    return {
        "time": datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
        "level": _FakeLevel(level_no),
        "process": _FakeProcess(pid),
        "thread": _FakeThread(tid),
        "line": line,
        "message": message,
        "file": _FakeFile(filename),
        "function": function,
    }


# ── frame_total_size ────────────────────────────────────────────────

class TestFrameTotalSize:
    def test_zero_payload(self):
        assert frame_total_size(0) == 16  # ceil(9/8)*8

    def test_small_payload(self):
        assert frame_total_size(7) == 16  # 9+7 = 16

    def test_exact_alignment(self):
        assert frame_total_size(15) == 24  # 9+15 = 24

    def test_needs_padding(self):
        assert frame_total_size(10) == 24  # 9+10 = 19 → 24

    def test_large_payload(self):
        assert frame_total_size(1000) == 1016  # 9+1000 = 1009 → 1016

    def test_always_multiple_of_align(self):
        for n in range(0, 500):
            assert frame_total_size(n) % FRAME_ALIGN == 0


# ── CRC-32 ──────────────────────────────────────────────────────────

class TestCRC32:
    def test_deterministic(self):
        data = b"loguru ring buffer test"
        assert compute_crc32(data) == compute_crc32(data)

    def test_different_data(self):
        assert compute_crc32(b"aaa") != compute_crc32(b"bbb")

    def test_empty(self):
        crc = compute_crc32(b"")
        assert isinstance(crc, int) and 0 <= crc < 2**32


# ── encode / decode round-trip ──────────────────────────────────────

class TestRecordRoundTrip:
    def test_basic(self):
        rec = _mock_record(message="price tick 42.5")
        data = encode_record(rec)
        out = decode_record(data)
        assert out["message"] == "price tick 42.5"
        assert out["level_no"] == 20
        assert out["pid"] == 1234
        assert out["tid"] == 9999
        assert out["line"] == 42
        assert "app.py" in out["source"]
        assert "run" in out["source"]

    def test_unicode(self):
        rec = _mock_record(message="日本語テスト 🚀✨")
        data = encode_record(rec)
        out = decode_record(data)
        assert out["message"] == "日本語テスト 🚀✨"

    def test_empty_message(self):
        rec = _mock_record(message="")
        out = decode_record(encode_record(rec))
        assert out["message"] == ""

    def test_large_message(self):
        big = "X" * 200_000
        rec = _mock_record(message=big)
        out = decode_record(encode_record(rec))
        assert out["message"] == big

    def test_newlines_and_tabs(self):
        rec = _mock_record(message="line1\nline2\ttab")
        out = decode_record(encode_record(rec))
        assert out["message"] == "line1\nline2\ttab"

    def test_null_bytes_in_message(self):
        rec = _mock_record(message="before\x00after")
        out = decode_record(encode_record(rec))
        assert out["message"] == "before\x00after"

    def test_level_no_range(self):
        for lvl in (0, 10, 20, 25, 30, 40, 50, 255):
            rec = _mock_record(level_no=lvl)
            out = decode_record(encode_record(rec))
            assert out["level_no"] == lvl

    def test_timestamp_preserved(self):
        rec = _mock_record()
        data = encode_record(rec)
        out = decode_record(data)
        expected_ns = int(rec["time"].timestamp() * 1_000_000_000)
        assert out["timestamp_ns"] == expected_ns

    def test_none_function(self):
        rec = _mock_record(function=None)
        out = decode_record(encode_record(rec))
        assert ":" in out["source"]

    def test_none_file(self):
        rec = _mock_record()
        rec["file"] = None
        out = decode_record(encode_record(rec))
        assert isinstance(out["source"], str)


# ── decode edge cases ───────────────────────────────────────────────

class TestDecodeEdgeCases:
    def test_truncated_raises(self):
        with pytest.raises(ValueError, match="too short"):
            decode_record(b"\x00" * 5)

    def test_binary_stability(self):
        """The same record always encodes to the exact same bytes."""
        rec = _mock_record()
        a = encode_record(rec)
        b = encode_record(rec)
        assert a == b


# ── constants sanity ────────────────────────────────────────────────

class TestConstants:
    def test_header_size(self):
        assert FRAME_HDR_SIZE == 9

    def test_alignment(self):
        assert FRAME_ALIGN == 8

    def test_status_values_distinct(self):
        values = {STATUS_FREE, STATUS_WRITING, STATUS_READY, STATUS_PADDING}
        assert len(values) == 4
