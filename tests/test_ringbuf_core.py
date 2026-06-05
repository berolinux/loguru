"""Tests for the core lock-free ring buffer (single-process)."""

import os
import struct
import zlib

import pytest

from ring_buffer_sink._frames import (
    FRAME_ALIGN,
    FRAME_HDR_SIZE,
    STATUS_PADDING,
    STATUS_READY,
    STATUS_WRITING,
    frame_total_size,
)
from ring_buffer_sink._ring_buffer import (
    HEADER_SIZE,
    MAGIC,
    OFF_WRITE_POS,
    OFF_READ_POS,
    RingBuffer,
)


@pytest.fixture()
def rb_path(tmp_path):
    """Return a fresh path in /dev/shm (cleaned up after test)."""
    path = f"/dev/shm/_test_rb_{os.getpid()}_{id(tmp_path)}"
    yield path
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


# ── creation / attachment ───────────────────────────────────────────

class TestCreateAttach:
    def test_create(self, rb_path):
        rb = RingBuffer(rb_path, capacity=4096, create=True)
        assert rb.capacity == 4096
        s = rb.stats()
        assert s["write_pos"] == 0
        assert s["read_pos"] == 0
        assert s["drop_count"] == 0
        assert s["write_count"] == 0
        rb.close()

    def test_attach(self, rb_path):
        rb1 = RingBuffer(rb_path, capacity=4096, create=True)
        rb2 = RingBuffer(rb_path, create=False)
        assert rb2.capacity == 4096
        rb1.close()
        rb2.close()

    def test_attach_bad_file_raises(self, tmp_path):
        bad = str(tmp_path / "garbage")
        with open(bad, "wb") as f:
            f.write(b"\x00" * 128)
        with pytest.raises(ValueError, match="Bad magic"):
            RingBuffer(bad, create=False)

    def test_capacity_rounded_to_alignment(self, rb_path):
        rb = RingBuffer(rb_path, capacity=1000, create=True)
        assert rb.capacity % FRAME_ALIGN == 0
        assert rb.capacity >= 1000
        rb.close()

    def test_create_requires_capacity(self, rb_path):
        with pytest.raises(ValueError, match="capacity"):
            RingBuffer(rb_path, create=True)


# ── single write / read ────────────────────────────────────────────

class TestWriteRead:
    def test_single(self, rb_path):
        rb = RingBuffer(rb_path, capacity=4096, create=True)
        assert rb.write(b"hello")
        assert rb.try_read() == b"hello"
        rb.close()

    def test_empty_read(self, rb_path):
        rb = RingBuffer(rb_path, capacity=4096, create=True)
        assert rb.try_read() is None
        rb.close()

    def test_empty_payload(self, rb_path):
        rb = RingBuffer(rb_path, capacity=4096, create=True)
        assert rb.write(b"")
        assert rb.try_read() == b""
        rb.close()

    def test_write_count(self, rb_path):
        rb = RingBuffer(rb_path, capacity=4096, create=True)
        for i in range(10):
            rb.write(f"msg{i}".encode())
        assert rb.stats()["write_count"] == 10
        rb.close()


# ── multiple writes / drain ─────────────────────────────────────────

class TestMultipleWrites:
    def test_fifo_order(self, rb_path):
        rb = RingBuffer(rb_path, capacity=8192, create=True)
        msgs = [f"msg-{i}".encode() for i in range(50)]
        for m in msgs:
            assert rb.write(m)
        out = rb.drain()
        assert out == msgs
        rb.close()

    def test_interleaved_write_read(self, rb_path):
        rb = RingBuffer(rb_path, capacity=1024, create=True)
        for i in range(100):
            assert rb.write(f"m{i}".encode())
            assert rb.try_read() == f"m{i}".encode()
        rb.close()


# ── buffer full → drop ──────────────────────────────────────────────

class TestBufferFull:
    def test_drops_when_full(self, rb_path):
        cap = 256
        rb = RingBuffer(rb_path, capacity=cap, create=True)
        written = 0
        dropped = 0
        for _ in range(1000):
            if rb.write(b"X" * 50):
                written += 1
            else:
                dropped += 1
        assert written > 0
        assert dropped > 0
        s = rb.stats()
        assert s["write_count"] == written
        assert s["drop_count"] == dropped
        rb.close()

    def test_after_drain_can_write_again(self, rb_path):
        rb = RingBuffer(rb_path, capacity=512, create=True)
        for _ in range(100):
            rb.write(b"fill")
        rb.drain()
        assert rb.write(b"after drain")
        assert rb.try_read() == b"after drain"
        rb.close()


# ── wrap-around ─────────────────────────────────────────────────────

class TestWrapAround:
    def test_write_read_across_boundary(self, rb_path):
        """Fill, drain, refill – forces data to wrap around offset 0."""
        cap = 256
        rb = RingBuffer(rb_path, capacity=cap, create=True)
        payload = b"A" * 20  # frame = align8(29) = 32

        # Phase 1: fill most of the buffer
        phase1 = []
        while rb.write(payload):
            phase1.append(payload)
        assert len(phase1) > 0

        # Drain
        out = rb.drain()
        assert out == phase1

        # Phase 2: write again (wraps around)
        phase2 = []
        for _ in range(len(phase1)):
            if rb.write(payload):
                phase2.append(payload)
        out2 = rb.drain()
        assert out2 == phase2
        rb.close()

    def test_many_wrap_cycles(self, rb_path):
        """Many fill/drain cycles to exercise wrap-around repeatedly."""
        rb = RingBuffer(rb_path, capacity=512, create=True)
        payload = b"cycle"
        for cycle in range(20):
            batch = []
            for _ in range(10):
                if rb.write(payload):
                    batch.append(payload)
            out = rb.drain()
            assert out == batch, f"mismatch on cycle {cycle}"
        rb.close()


# ── gap at buffer end (remaining < FRAME_HDR_SIZE) ──────────────────

class TestGap:
    def test_gap_skipped_correctly(self, rb_path):
        """Manually engineer a gap at the buffer tail."""
        # frame_total_size(15) = align8(24) = 24 bytes per frame
        # capacity = 128 → 128/24 = 5.33 → 5 frames fill 120 bytes → 8 left
        # 8 < FRAME_HDR_SIZE(9) → gap
        cap = 128
        rb = RingBuffer(rb_path, capacity=cap, create=True)
        payload = b"x" * 15  # 24-byte frame
        for i in range(5):
            assert rb.write(payload), f"write {i} failed"
        # Drain the 5 frames so read_pos catches up
        for i in range(5):
            assert rb.try_read() == payload
        # Now both cursors at 120, remaining = 8 < 9
        assert rb.stats()["write_pos"] == 120
        # Write one more – should skip the 8-byte gap and write at offset 0
        assert rb.write(b"after_gap")
        assert rb.try_read() == b"after_gap"
        rb.close()


# ── padding frames ──────────────────────────────────────────────────

class TestPadding:
    def test_padding_transparent_to_reader(self, rb_path):
        """A frame that doesn't fit triggers padding; reader skips it."""
        cap = 256
        rb = RingBuffer(rb_path, capacity=cap, create=True)
        small = b"s" * 7   # frame = 16
        big = b"B" * 60    # frame = align8(69) = 72

        # Write 9 small, then read 6 to advance read_pos while keeping used low.
        # Then write 6 more to reach offset 240 with remaining=16 < 72.
        # At that point: used=144, free=112, needed for padding+big=88.
        # 144+88=232 <= 256, so padding path succeeds.
        for _ in range(9):
            assert rb.write(small)
        for _ in range(6):
            assert rb.try_read() == small
        for _ in range(6):
            assert rb.write(small)

        # remaining at end is 16 < 72, so this write triggers padding
        assert rb.write(big)

        out = rb.drain()
        assert big in out
        rb.close()


# ── CRC integrity ───────────────────────────────────────────────────

class TestCRCIntegrity:
    def test_corrupted_payload_skipped(self, rb_path):
        """Flip a byte in a committed frame; reader must skip it."""
        rb = RingBuffer(rb_path, capacity=4096, create=True)
        rb.write(b"good_before")
        rb.write(b"will_corrupt")
        rb.write(b"good_after")

        # Corrupt the payload of the second frame
        # First frame: starts at HEADER_SIZE, size = align8(9+11) = 24
        # Second frame: starts at HEADER_SIZE + 24
        second_off = HEADER_SIZE + frame_total_size(len(b"good_before"))
        payload_off = second_off + FRAME_HDR_SIZE
        old = rb._mm[payload_off]
        rb._mm[payload_off] = (old + 1) % 256

        out = rb.drain()
        messages = [d for d in out]
        # The corrupted frame should be skipped
        assert b"good_before" in messages
        assert b"good_after" in messages
        assert b"will_corrupt" not in messages
        rb.close()

    def test_corrupted_length_skipped(self, rb_path):
        """A bogus payload_len in the header should not crash the reader."""
        rb = RingBuffer(rb_path, capacity=4096, create=True)
        rb.write(b"sentinel_a")
        rb.write(b"target")
        rb.write(b"sentinel_b")

        # Corrupt the payload_len of the second frame to something huge
        second_off = HEADER_SIZE + frame_total_size(len(b"sentinel_a"))
        struct.pack_into("<I", rb._mm, second_off + 1, 999999)

        out = rb.drain()
        assert b"sentinel_a" in out
        # sentinel_b may or may not survive depending on reader recovery,
        # but the reader must not crash or loop forever.
        rb.close()


# ── stats ───────────────────────────────────────────────────────────

class TestStats:
    def test_initial(self, rb_path):
        rb = RingBuffer(rb_path, capacity=1024, create=True)
        s = rb.stats()
        assert s["write_pos"] == 0
        assert s["read_pos"] == 0
        assert s["write_count"] == 0
        assert s["drop_count"] == 0
        assert s["capacity"] == 1024
        rb.close()

    def test_after_writes(self, rb_path):
        rb = RingBuffer(rb_path, capacity=4096, create=True)
        for _ in range(5):
            rb.write(b"data")
        s = rb.stats()
        assert s["write_count"] == 5
        assert s["write_pos"] > 0
        rb.close()


# ── unlink ──────────────────────────────────────────────────────────

class TestUnlink:
    def test_unlink_removes_file(self, rb_path):
        rb = RingBuffer(rb_path, capacity=1024, create=True)
        rb.close()
        assert os.path.exists(rb_path)
        rb.unlink()
        assert not os.path.exists(rb_path)

    def test_unlink_idempotent(self, rb_path):
        rb = RingBuffer(rb_path, capacity=1024, create=True)
        rb.close()
        rb.unlink()
        rb.unlink()  # should not raise
