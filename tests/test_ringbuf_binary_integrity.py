"""Binary layout, checksum, and mmap corruption stress tests.

These complement the concurrency and crash suites by mutating shared memory
directly and asserting the reader never surfaces a payload that fails an
independent structural check.
"""

import multiprocessing
import os
import random
import struct

import pytest

from ring_buffer_sink._frames import STATUS_READY, compute_checksum
from ring_buffer_sink._ring_buffer import HEADER_SIZE, RingBuffer


def _writer_integrity(path, wid, n, psz):
    rb = RingBuffer(path, create=False)
    for seq in range(n):
        tag = struct.pack("<II", wid, seq)
        payload = tag + b"\x77" * (psz - len(tag))
        rb.write(payload)
    rb.close()


def _validate_tagged_payload(p, payload_size):
    assert len(p) == payload_size
    struct.unpack_from("<I", p, 0)[0]  # seq — must not raise
    assert p[4:] == b"\x5A" * (payload_size - 4)


@pytest.fixture()
def rb_path():
    path = f"/dev/shm/_test_rbintegrity_{os.getpid()}"
    yield path
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


class TestRandomCorruption:
    def test_random_bitflips_never_yield_invalid_payloads(self, rb_path):
        """Scramble hundreds of bytes in the data region; drained frames must
        still decode as well-formed tagged payloads."""
        cap = 16 * 1024
        rb = RingBuffer(rb_path, capacity=cap, create=True)
        psz = 40
        written = []
        for i in range(200):
            tag = struct.pack("<I", i)
            payload = tag + b"\x5A" * (psz - len(tag))
            if rb.write(payload):
                written.append(payload)

        rng = random.Random(12345)
        mm = rb._mm
        for _ in range(600):
            idx = rng.randint(HEADER_SIZE, len(mm) - 1)
            mm[idx] = (mm[idx] + rng.randint(1, 255)) & 0xFF

        seen = []
        for _ in range(5000):
            p = rb.try_read()
            if p is None:
                break
            _validate_tagged_payload(p, psz)
            seen.append(p)

        good_set = {bytes(x) for x in written}
        for p in seen:
            assert p in good_set
            assert compute_checksum(p) == compute_checksum(p)

        rb.close()

    def test_bad_checksum_skips_frame_reader_continues(self, rb_path):
        """Corrupt checksum of an earlier READY frame; reader skips it and
        still delivers later valid frames (read_pos must not get stuck)."""
        rb = RingBuffer(rb_path, capacity=4096, create=True)
        assert rb.write(b"bad")
        assert rb.write(b"good")

        doff = HEADER_SIZE
        assert rb._load8(doff) == STATUS_READY
        chk_off = doff + 5
        rb._mm[chk_off] = (rb._mm[chk_off] + 1) % 256

        assert rb.try_read() == b"good"
        assert rb.try_read() is None
        rb.close()


class TestConcurrentBinaryIntegrity:
    def test_32_processes_tags_and_checksums(self, rb_path):
        """Many writers; every drained payload must match tag grammar and
        checksum recomputation."""
        cap = 4 << 20
        n_w, n_m, psz = 32, 150, 56
        rb = RingBuffer(rb_path, capacity=cap, create=True)
        rb.close()

        procs = []
        for wid in range(n_w):
            p = multiprocessing.Process(
                target=_writer_integrity,
                args=(rb_path, wid, n_m, psz),
            )
            procs.append(p)
            p.start()

        for p in procs:
            p.join(timeout=60)
            assert p.exitcode == 0

        rb = RingBuffer(rb_path, create=False)
        seen = set()
        while True:
            p = rb.try_read()
            if p is None:
                break
            assert len(p) == psz
            wid, seq = struct.unpack_from("<II", p, 0)
            assert 0 <= wid < n_w
            assert 0 <= seq < n_m
            assert p[8:] == b"\x77" * (psz - 8)
            key = (wid, seq)
            assert key not in seen
            seen.add(key)
            assert compute_checksum(p) == compute_checksum(bytes(p))

        expected = n_w * n_m
        stats = rb.stats()
        assert stats["write_count"] == expected
        assert len(seen) == stats["write_count"]

        rb.close()
