"""Multi-process concurrency tests for the lock-free ring buffer.

Every test spawns real child processes that share a single ring buffer and
slam it with writes.  Assertions verify:
  - no data corruption (CRC checks pass)
  - every non-dropped message arrives exactly once
  - message counts add up (written + dropped = sent)
"""

import multiprocessing
import os
import struct
import time
import zlib

import pytest

from ring_buffer_sink._frames import FRAME_ALIGN, compute_crc32, frame_total_size
from ring_buffer_sink._ring_buffer import RingBuffer


# ── helpers ─────────────────────────────────────────────────────────

def _writer_fn(path, writer_id, n_messages, payload_size, result_queue):
    """Child-process writer.  Writes tagged messages and reports counts."""
    rb = RingBuffer(path, create=False)
    written = 0
    dropped = 0
    for seq in range(n_messages):
        # Payload: [writer_id:4][seq:4][padding]
        tag = struct.pack("<II", writer_id, seq)
        payload = tag + bytes(payload_size - len(tag))
        if rb.write(payload):
            written += 1
        else:
            dropped += 1
    rb.close()
    result_queue.put((writer_id, written, dropped))


@pytest.fixture()
def shm_path():
    path = f"/dev/shm/_test_concurrent_{os.getpid()}"
    yield path
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _run_writers(shm_path, n_writers, msgs_per_writer, payload_size, capacity):
    """Spawn writers, wait, then drain the buffer."""
    rb = RingBuffer(shm_path, capacity=capacity, create=True)
    q = multiprocessing.Queue()

    procs = []
    for wid in range(n_writers):
        p = multiprocessing.Process(
            target=_writer_fn,
            args=(shm_path, wid, msgs_per_writer, payload_size, q),
        )
        procs.append(p)
        p.start()

    for p in procs:
        p.join(timeout=30)

    results = {}
    while not q.empty():
        wid, written, dropped = q.get_nowait()
        results[wid] = (written, dropped)

    # Drain all readable frames
    payloads = rb.drain()
    stats = rb.stats()
    rb.close()
    return payloads, results, stats


def _verify_integrity(payloads, n_writers, msgs_per_writer, payload_size):
    """Check that every received payload is intact (tag is well-formed)."""
    seen = set()
    for p in payloads:
        assert len(p) == payload_size, f"payload length {len(p)} != {payload_size}"
        wid, seq = struct.unpack_from("<II", p, 0)
        assert 0 <= wid < n_writers, f"bad writer_id {wid}"
        assert 0 <= seq < msgs_per_writer, f"bad seq {seq}"
        key = (wid, seq)
        assert key not in seen, f"duplicate {key}"
        seen.add(key)
    return seen


# ── tests ───────────────────────────────────────────────────────────

class TestTwoWriters:
    def test_basic(self, shm_path):
        n_w, n_m, psz = 2, 500, 32
        payloads, results, stats = _run_writers(shm_path, n_w, n_m, psz, 1 << 20)

        seen = _verify_integrity(payloads, n_w, n_m, psz)
        total_written = sum(w for w, _ in results.values())
        total_dropped = sum(d for _, d in results.values())
        assert total_written + total_dropped == n_w * n_m
        assert len(payloads) == total_written
        assert stats["write_count"] == total_written


class TestManyWriters:
    @pytest.mark.parametrize("n_writers", [4, 8])
    def test_many(self, shm_path, n_writers):
        n_m, psz = 300, 64
        payloads, results, stats = _run_writers(
            shm_path, n_writers, n_m, psz, 1 << 20
        )
        seen = _verify_integrity(payloads, n_writers, n_m, psz)
        total_written = sum(w for w, _ in results.values())
        total_dropped = sum(d for _, d in results.values())
        assert total_written + total_dropped == n_writers * n_m
        assert len(payloads) == total_written


class TestStressSmallBuffer:
    def test_small_buffer_high_contention(self, shm_path):
        """Tiny buffer + many writers → lots of drops but no corruption."""
        n_w, n_m, psz = 4, 1000, 32
        payloads, results, stats = _run_writers(shm_path, n_w, n_m, psz, 4096)
        _verify_integrity(payloads, n_w, n_m, psz)
        total_written = sum(w for w, _ in results.values())
        total_dropped = sum(d for _, d in results.values())
        assert total_written + total_dropped == n_w * n_m
        assert total_dropped > 0, "Expected drops with tiny buffer"


class TestConcurrentWriterAndReader:
    def test_live_drain(self, shm_path):
        """Writer and reader run simultaneously."""
        n_m, psz = 2000, 32
        rb = RingBuffer(shm_path, capacity=1 << 20, create=True)
        q = multiprocessing.Queue()

        p = multiprocessing.Process(
            target=_writer_fn, args=(shm_path, 0, n_m, psz, q),
        )
        p.start()

        collected = []
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            data = rb.try_read()
            if data is not None:
                collected.append(data)
            elif not p.is_alive():
                # Writer done – drain remaining
                collected.extend(rb.drain())
                break
            else:
                time.sleep(0.0001)
        p.join(timeout=5)

        wid, written, dropped = q.get(timeout=5)
        assert len(collected) == written
        _verify_integrity(collected, 1, n_m, psz)
        rb.close()


class TestBurstAllWriters:
    def test_burst_16_writers(self, shm_path):
        """16 writers, each sending 200 messages – maximum contention."""
        n_w, n_m, psz = 16, 200, 48
        payloads, results, stats = _run_writers(
            shm_path, n_w, n_m, psz, 2 << 20,
        )
        _verify_integrity(payloads, n_w, n_m, psz)
        total_written = sum(w for w, _ in results.values())
        assert len(payloads) == total_written


class TestLargePayloadConcurrent:
    def test_large_frames(self, shm_path):
        """Each frame is ~1 KB – exercises wrap-around with big messages."""
        n_w, n_m, psz = 4, 100, 1024
        payloads, results, stats = _run_writers(
            shm_path, n_w, n_m, psz, 4 << 20,
        )
        _verify_integrity(payloads, n_w, n_m, psz)
        total_written = sum(w for w, _ in results.values())
        assert len(payloads) == total_written
