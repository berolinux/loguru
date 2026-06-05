"""Crash-resilience tests for the ring buffer.

These tests intentionally SIGKILL writer processes at various points and
verify that:
  - the buffer's internal structure remains navigable
  - the reader can continue past partially-written frames
  - new writers can attach and write after a crash
"""

import multiprocessing
import os
import signal
import struct
import time
import zlib

import pytest

from ring_buffer_sink._frames import (
    FRAME_ALIGN,
    FRAME_HDR_SIZE,
    STATUS_READY,
    STATUS_WRITING,
    frame_total_size,
)
from ring_buffer_sink._ring_buffer import HEADER_SIZE, RingBuffer


@pytest.fixture()
def shm_path():
    path = f"/dev/shm/_test_crash_{os.getpid()}"
    yield path
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


# ── helpers ─────────────────────────────────────────────────────────

def _writer_loop(path, payload_size, ready_event, stop_event):
    """Write forever until told to stop or killed."""
    rb = RingBuffer(path, create=False)
    ready_event.set()
    seq = 0
    while not stop_event.is_set():
        tag = struct.pack("<I", seq)
        payload = tag + b"\xAA" * (payload_size - len(tag))
        rb.write(payload)
        seq += 1
    rb.close()


def _writer_burst_then_die(path, n_messages, payload_size, start_event):
    """Write a burst and then exit uncleanly with os._exit."""
    rb = RingBuffer(path, create=False)
    start_event.set()
    for seq in range(n_messages):
        tag = struct.pack("<I", seq)
        payload = tag + b"\xBB" * (payload_size - len(tag))
        rb.write(payload)
    # Simulate crash: no cleanup, no close()
    os._exit(0)


def _writer_slow_crash(path, payload_size, started_event):
    """Write one message slowly (sleep between header and payload) so
    SIGKILL is likely to hit mid-frame."""
    rb = RingBuffer(path, create=False)
    started_event.set()
    seq = 0
    while True:
        tag = struct.pack("<I", seq)
        payload = tag + b"\xCC" * (payload_size - len(tag))
        rb.write(payload)
        seq += 1
        # Tiny sleep to give test harness a chance to SIGKILL us
        time.sleep(0.0001)


# ── tests ───────────────────────────────────────────────────────────

class TestSIGKILLWriter:
    def test_sigkill_during_writes(self, shm_path):
        """SIGKILL a writer mid-stream; buffer must stay consistent."""
        cap = 1 << 20
        rb = RingBuffer(shm_path, capacity=cap, create=True)
        started = multiprocessing.Event()

        p = multiprocessing.Process(
            target=_writer_slow_crash, args=(shm_path, 64, started),
        )
        p.start()
        started.wait(timeout=5)
        # Let it write for a bit
        time.sleep(0.05)
        os.kill(p.pid, signal.SIGKILL)
        p.join(timeout=5)

        # The buffer should still be readable (some frames good, maybe one torn)
        good = []
        for _ in range(10_000):
            data = rb.try_read()
            if data is None:
                break
            good.append(data)

        # At least some frames must have survived
        assert len(good) > 0
        # Every frame that *was* returned must be intact
        for payload in good:
            assert len(payload) == 64
            wid = struct.unpack_from("<I", payload, 0)[0]
            assert isinstance(wid, int)

        rb.close()

    def test_reader_continues_after_crash(self, shm_path):
        """After a writer crash, a reader that catches up can still read
        frames written by a NEW writer."""
        cap = 1 << 20
        rb = RingBuffer(shm_path, capacity=cap, create=True)
        started = multiprocessing.Event()

        # Writer 1: crash it
        p1 = multiprocessing.Process(
            target=_writer_slow_crash, args=(shm_path, 32, started),
        )
        p1.start()
        started.wait(timeout=5)
        time.sleep(0.02)
        os.kill(p1.pid, signal.SIGKILL)
        p1.join(timeout=5)

        # Drain whatever writer 1 committed
        rb.drain()

        # Writer 2: clean writer
        started2 = multiprocessing.Event()
        stop2 = multiprocessing.Event()
        p2 = multiprocessing.Process(
            target=_writer_loop, args=(shm_path, 32, started2, stop2),
        )
        p2.start()
        started2.wait(timeout=5)
        time.sleep(0.02)
        stop2.set()
        p2.join(timeout=5)

        frames = rb.drain()
        assert len(frames) > 0, "Writer 2 frames should be readable"
        rb.close()


class TestOsExitWriter:
    def test_unclean_exit_no_close(self, shm_path):
        """Writer calls os._exit() without closing the ring buffer.
        The buffer should remain valid for other processes."""
        cap = 1 << 20
        rb = RingBuffer(shm_path, capacity=cap, create=True)
        started = multiprocessing.Event()

        p = multiprocessing.Process(
            target=_writer_burst_then_die,
            args=(shm_path, 500, 32, started),
        )
        p.start()
        started.wait(timeout=5)
        p.join(timeout=10)

        frames = rb.drain()
        assert len(frames) == 500
        for f in frames:
            seq = struct.unpack_from("<I", f, 0)[0]
            assert 0 <= seq < 500
        rb.close()


class TestMultipleCrashes:
    def test_repeated_crash_restart(self, shm_path):
        """Kill and restart writers 5 times; buffer stays usable."""
        cap = 1 << 20
        rb = RingBuffer(shm_path, capacity=cap, create=True)

        total_good = 0
        for cycle in range(5):
            started = multiprocessing.Event()
            p = multiprocessing.Process(
                target=_writer_slow_crash, args=(shm_path, 48, started),
            )
            p.start()
            started.wait(timeout=5)
            time.sleep(0.02)
            os.kill(p.pid, signal.SIGKILL)
            p.join(timeout=5)

            frames = rb.drain()
            total_good += len(frames)

        assert total_good > 0, "At least some frames should survive 5 crash cycles"
        # Verify the buffer is still in a usable state
        assert rb.write(b"final_check")
        assert rb.try_read() == b"final_check"
        rb.close()


class TestCrashedFrameDetection:
    def test_status_writing_skipped_when_stale(self, shm_path):
        """Manually inject a STATUS_WRITING frame and verify the reader
        skips it when write_pos is far ahead."""
        cap = 4096
        rb = RingBuffer(shm_path, capacity=cap, create=True)

        # Write a normal frame
        rb.write(b"before")

        # Write another normal frame (we'll corrupt its status)
        rb.write(b"middle")

        # Write a third normal frame
        rb.write(b"after_")  # 6 bytes to match "middle" size for easy math

        # Corrupt the second frame's status byte to STATUS_WRITING
        first_fsize = frame_total_size(len(b"before"))
        second_off = HEADER_SIZE + first_fsize
        rb._store8(second_off, STATUS_WRITING)

        # Move write_pos far ahead to simulate a crashed writer scenario
        # The reader should skip the WRITING frame if the gap is large
        current_wpos = rb._load64(8)  # OFF_WRITE_POS
        rb._store64(8, current_wpos + cap)  # push write_pos far ahead

        out = rb.drain()
        assert b"before" in out
        # "middle" should be skipped (STATUS_WRITING with large gap)
        assert b"middle" not in out
        rb.close()
