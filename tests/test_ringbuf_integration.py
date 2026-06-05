"""End-to-end integration tests: Loguru → RingBufferSink → RingBufferReader."""

import multiprocessing
import os
import time

import pytest
from loguru import logger

from ring_buffer_sink import RingBufferReader, RingBufferSink
from ring_buffer_sink._frames import decode_record, encode_record
from ring_buffer_sink._ring_buffer import RingBuffer


@pytest.fixture(autouse=True)
def _clean_loguru():
    """Remove all loguru handlers before and after every test."""
    logger.remove()
    yield
    logger.remove()


@pytest.fixture()
def shm_path():
    path = f"/dev/shm/_test_integ_{os.getpid()}"
    yield path
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


# ── basic sink usage ────────────────────────────────────────────────

class TestSinkBasic:
    def test_add_and_log(self, shm_path):
        sink = RingBufferSink("integ", capacity=1 << 20, create=True, path=shm_path)
        logger.add(sink, format="{message}")

        logger.info("hello from loguru")

        reader = RingBufferReader(path=shm_path)
        rec = reader.read_record()
        assert rec is not None
        assert rec["message"] == "hello from loguru"
        assert rec["level_no"] == 20  # INFO
        reader.close()
        sink.close()

    def test_multiple_messages(self, shm_path):
        sink = RingBufferSink("integ", capacity=1 << 20, create=True, path=shm_path)
        logger.add(sink, format="{message}")

        for i in range(100):
            logger.info(f"msg-{i}")

        reader = RingBufferReader(path=shm_path)
        records = reader.drain_records(limit=200)
        assert len(records) == 100
        messages = [r["message"] for r in records]
        for i in range(100):
            assert f"msg-{i}" in messages
        reader.close()
        sink.close()


# ── log levels ──────────────────────────────────────────────────────

class TestLevels:
    def test_levels_preserved(self, shm_path):
        sink = RingBufferSink("integ", capacity=1 << 20, create=True, path=shm_path)
        logger.add(sink, level="TRACE", format="{message}")

        logger.trace("t")
        logger.debug("d")
        logger.info("i")
        logger.warning("w")
        logger.error("e")
        logger.critical("c")

        reader = RingBufferReader(path=shm_path)
        records = reader.drain_records()
        levels = [(r["message"], r["level_no"]) for r in records]
        assert ("t", 5) in levels
        assert ("d", 10) in levels
        assert ("i", 20) in levels
        assert ("w", 30) in levels
        assert ("e", 40) in levels
        assert ("c", 50) in levels
        reader.close()
        sink.close()


# ── source info ─────────────────────────────────────────────────────

class TestSourceInfo:
    def test_file_and_function_in_source(self, shm_path):
        sink = RingBufferSink("integ", capacity=1 << 20, create=True, path=shm_path)
        logger.add(sink, format="{message}")

        logger.info("source_check")

        reader = RingBufferReader(path=shm_path)
        rec = reader.read_record()
        assert "test_ringbuf_integration" in rec["source"]
        assert rec["line"] > 0
        reader.close()
        sink.close()


# ── multi-process with loguru ───────────────────────────────────────

def _loguru_writer(shm_path, writer_id, n_messages):
    """Child process: attach to buffer, configure loguru, write messages."""
    from loguru import logger as child_logger

    child_logger.remove()
    sink = RingBufferSink("integ", create=False, path=shm_path)
    child_logger.add(sink, format="{message}")
    for i in range(n_messages):
        child_logger.info(f"w{writer_id}-{i}")
    sink.close()


class TestMultiProcess:
    def test_two_process_loguru(self, shm_path):
        """Two separate processes write logs through loguru to the same buffer."""
        sink = RingBufferSink("integ", capacity=1 << 20, create=True, path=shm_path)
        # The main process also writes
        logger.add(sink, format="{message}")
        for i in range(50):
            logger.info(f"main-{i}")

        p = multiprocessing.Process(
            target=_loguru_writer, args=(shm_path, 1, 50),
        )
        p.start()
        p.join(timeout=15)
        assert p.exitcode == 0

        reader = RingBufferReader(path=shm_path)
        records = reader.drain_records()
        messages = [r["message"] for r in records]
        main_msgs = [m for m in messages if m.startswith("main-")]
        child_msgs = [m for m in messages if m.startswith("w1-")]
        assert len(main_msgs) == 50
        assert len(child_msgs) == 50
        reader.close()
        sink.close()

    def test_four_process_loguru(self, shm_path):
        n_per = 100
        sink = RingBufferSink("integ", capacity=4 << 20, create=True, path=shm_path)
        sink.close()  # just create the file

        procs = []
        for wid in range(4):
            p = multiprocessing.Process(
                target=_loguru_writer, args=(shm_path, wid, n_per),
            )
            procs.append(p)
            p.start()

        for p in procs:
            p.join(timeout=15)
            assert p.exitcode == 0

        reader = RingBufferReader(path=shm_path)
        records = reader.drain_records()
        assert len(records) == 4 * n_per
        for wid in range(4):
            wmsgs = [r for r in records if r["message"].startswith(f"w{wid}-")]
            assert len(wmsgs) == n_per
        reader.close()


# ── repr / stats ────────────────────────────────────────────────────

class TestSinkMisc:
    def test_repr(self, shm_path):
        sink = RingBufferSink("x", capacity=1024, create=True, path=shm_path)
        r = repr(sink)
        assert "RingBufferSink" in r
        assert "x" in r
        sink.close()

    def test_stats(self, shm_path):
        sink = RingBufferSink("x", capacity=1 << 20, create=True, path=shm_path)
        logger.add(sink, format="{message}")
        logger.info("one")
        logger.info("two")
        s = sink.stats()
        assert s["write_count"] == 2
        assert s["drop_count"] == 0
        sink.close()
