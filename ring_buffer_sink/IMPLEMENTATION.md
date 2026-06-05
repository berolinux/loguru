# Lock-Free Ring Buffer Sink for Loguru — Implementation Guide

## Overview

This document describes a high-performance, lock-free, shared-memory ring buffer sink designed for Loguru. It enables multiple independent Python processes (e.g., in a high-frequency trading system) to write logs to a single, pre-allocated buffer **without blocking, without locks, and without any serialization library** (no pickle, no json).

Key properties:
- **Lock-free**: All synchronization uses atomic compare-and-swap (CAS) and atomic loads/stores via a tiny C helper library.
- **Non-blocking**: Writers never wait; if the buffer is full, the write is dropped.
- **Crash-resilient**: A per-frame status byte plus CRC-32 checksum allows readers to detect and skip torn or corrupt frames after hard crashes (SIGKILL, `os._exit`).
- **Binary frames**: Variable-length frames with a simple header (status + length + CRC) — no external serialization.
- **Multi-producer, single-consumer (MPSC)**: Many writers, one reader.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Shared Memory File                       │
│                    (e.g., /dev/shm/loguru_rb_trading)           │
├─────────────────────────────────────────────────────────────────┤
│  Header (64 bytes)                                              │
│   0x00  magic          uint64  0x4C4F47555F524230 ("LOGU_RB0")  │
│   0x08  write_pos      uint64  monotonic cursor (atomic)        │
│   0x10  read_pos       uint64  monotonic cursor (atomic)        │
│   0x18  capacity       uint64  data region size                 │
│   0x20  drop_count     uint64  atomic counter                   │
│   0x28  write_count    uint64  atomic counter                   │
│   0x30  reserved (16)                                           │
├─────────────────────────────────────────────────────────────────┤
│  Data Region (capacity bytes, 8-byte aligned)                   │
│   ... variable-length frames ...                                │
└─────────────────────────────────────────────────────────────────┘

Frame Layout (8-byte aligned):
┌───────────┬──────────────┬──────────┬────────────────────────────┐
│ status(1) │ payload_len  │  crc32   │      payload (N bytes)     │
│           │    (4 LE)    │  (4 LE)  │                            │
└───────────┴──────────────┴──────────┴────────────────────────────┘
            │<--- FRAME_HDR_SIZE=9 --->│<--- payload_len --->│
Total frame size = align8(9 + payload_len)
```

### Status Byte Values

| Value | Name | Meaning |
|-------|------|---------|
| 0x00  | FREE | Slot never written or already consumed |
| 0x01  | WRITING | Writer has claimed space and is filling payload |
| 0x02  | READY | Writer finished; payload + CRC are valid |
| 0xFE  | PADDING | Gap filler to next frame at buffer start after wrap |

Memory ordering:
- Status byte stores use **release** semantics (`__ATOMIC_RELEASE`).
- Status byte loads use **acquire** semantics (`__ATOMIC_ACQUIRE`).
- 64-bit positions and counters use `SEQ_CST`.
- A full `SEQ_CST` fence is issued after writing payload and before marking READY.

---

## Components

### 1. `_atomic_ops.c` + `_atomics.py`

A minimal C library exposing:
- `rb_atomic_load_u64` / `store_u64` / `fetch_add_u64` / `cas_u64`
- `rb_atomic_load_u8` / `store_u8` (with acquire/release)
- `rb_memory_fence`

Compiled on first import via `gcc -shared -fPIC -O2`. All operations act directly on pointers into `mmap`'d memory, giving true cross-process atomicity without OS primitives.

### 2. `_frames.py`

Pure-Python binary encoding for Loguru records — **no pickle, no json**.

Record payload layout (little-endian):
```
0   int64   timestamp_ns
8   uint8   level_no
9   uint32  pid
13  uint64  tid
21  uint32  line
25  uint32  msg_len
29  ...     message (UTF-8)
... uint16  src_len
... ...     source ("filename:function")
```

Frame header adds: `status(1) | payload_len(4) | crc32(4)`.

CRC-32 is computed with `zlib.crc32` over the **payload bytes only**.

`frame_total_size(n)` returns the aligned size for a payload of `n` bytes.

### 3. `_ring_buffer.py`

The core lock-free MPSC ring buffer.

**Write path** (never blocks):
1. Load `wpos`, `rpos` atomically.
2. Compute offset in ring and `remaining` to end.
3. Compute `needed` bytes to advance `wpos`:
   - Normal: `fsize`
   - Gap at end (`remaining < FRAME_HDR_SIZE`): `remaining + fsize`
   - Padding needed (`remaining < fsize`): `remaining + fsize`
4. Capacity check: `if wpos - rpos + needed > cap` → drop, increment `drop_count`.
5. CAS `wpos` from old to new.
6. If CAS won:
   - Gap case: write a 1-byte STATUS_PADDING marker, continue.
   - Padding case: write STATUS_PADDING frame using `remaining` bytes, continue.
   - Normal: mark STATUS_WRITING, pack header (len, CRC), copy payload, fence, mark STATUS_READY, increment `write_count`.

**Read path** (single consumer, no CAS on `read_pos`):
1. Load positions; if `rpos >= wpos` → empty.
2. Handle gap bytes and STATUS_FREE/WRITING/PADDING states.
3. Special WRITING recovery: if `wpos - rpos > cap // 2`, assume writer crashed; skip using declared length (or FRAME_ALIGN).
4. Verify CRC over payload; on mismatch, advance past frame and continue.
5. On success, advance `read_pos` and return payload bytes.

### 4. `sink.py`

`RingBufferSink` implements both the callable and file-like interfaces expected by `logger.add()`:
- `__call__(message)` and `write(message)` extract `message.record`, call `encode_record`, and `rb.write(payload)`.
- `close()` detaches the mmap (does **not** delete the file).
- `stats()` and `ring_buffer` expose diagnostics.

### 5. `reader.py`

`RingBufferReader` is the consumer API:
- `read_record()` → decoded dict or `None`
- `read_raw()` → raw payload bytes or `None`
- `drain_records(limit=None)`
- `iter_records(poll_interval=0.001)`
- Same `stats()`/`close()` surface as the sink.

---

## Key Design Decisions

### No Locks By Design

All coordination is via monotonic cursors updated with CAS. The only "lock-like" behavior is the single-consumer assumption on the read side (no concurrent readers).

### Drop on Full

Writers drop rather than block or spin forever. This is intentional for a trading system: latency spikes from backpressure are unacceptable. `drop_count` lets operators observe loss.

### Status_WRITING + Crash Heuristic

A writer sets STATUS_WRITING before touching payload, then fences, then STATUS_READY. If a reader sees WRITING and the write cursor is far ahead (`wpos - rpos > cap // 2`), it assumes the writer died and skips the frame using the declared length. This bounds how far a single torn frame can poison the stream.

### CRC-32 Checksum

Covers only the payload. Combined with the status byte lifecycle, it detects:
- Torn writes (partial payload visible)
- Bit flips / memory corruption
- Accidental overwrites by a buggy writer

On CRC mismatch, the reader advances past the frame and continues — no corruption propagates.

### 8-Byte Alignment

Simplifies pointer math, ensures headers are naturally aligned for 32/64-bit loads on most architectures, and makes wrap-around padding calculations trivial.

### Pre-allocated Capacity

Capacity is fixed at creation. The backing file is `ftruncate`'d once. No reallocations, no resizes, no surprises under load.

### Pure Struct Packing for Records

`encode_record` / `decode_record` use `struct.pack`/`unpack` with explicit widths and UTF-8 encoding for strings. Deterministic, compact, and free of external dependencies or serialization attack surface.

---

## Testing Strategy

The test suite (`tests/test_ringbuf_*.py`) is designed to be ruthless:

### Core Correctness (`test_ringbuf_core.py`)
- Create/attach, header validation, magic numbers
- Single and multi write/read, FIFO order
- Wrap-around, gap handling, padding frames
- Buffer-full drop behavior and counters
- CRC integrity: corrupted payloads and bogus lengths are skipped
- Unlink semantics

### Frame Layer (`test_ringbuf_frames.py`)
- Alignment math for all payload sizes
- Deterministic CRC
- Full round-trips for Loguru-shaped records (unicode, newlines, nulls, huge messages)
- Edge cases (truncated, None file/function)

### Concurrency (`test_ringbuf_concurrent.py`)
- 2/4/8/16 real processes writing simultaneously
- Tagged payloads to prove no duplicates and no cross-writer corruption
- Small buffer + high contention → many drops but zero corruption
- Live reader draining while writers run
- Variable-length frames under concurrency (8B..2KB mix)
- Extreme size range (1B to ~2KB)

### Crash Resilience (`test_ringbuf_crash.py`)
- `SIGKILL` during steady writes
- `os._exit(0)` without `close()` (simulates instant death)
- Repeated crash/restart cycles
- Manual injection of STATUS_WRITING + far-ahead `write_pos`
- **Variable-length crash**: 2000 frames with sizes cycling 8..200 bytes, then crash; reader must only return well-formed frames
- `SIGKILL` during slow variable writes (higher chance of tearing mid-frame)
- Reader actively draining while a writer is killed

All crash tests verify that **every byte returned by the reader** is consistent with what some writer intended to send — no silent corruption.

### Integration (`test_ringbuf_integration.py`)
- End-to-end: `logger.add(RingBufferSink(...))` → `RingBufferReader`
- Multi-process Loguru writers
- All log levels and source info preserved

---

## Usage Example

```python
from loguru import logger
from ring_buffer_sink import RingBufferSink, RingBufferReader

# Process A (or any first writer) — creates the buffer
sink = RingBufferSink("trading", capacity=64*1024*1024, create=True)
logger.add(sink, level="INFO", format="{time} | {level} | {message}")

logger.info("engine started")

# Process B, C, D... — attach to existing buffer
sink_b = RingBufferSink("trading", create=False)
logger_b = logger.bind()
logger_b.add(sink_b, format="{message}")
logger_b.warning("price tick 12345")

# Reader (separate monitoring process)
reader = RingBufferReader("trading")
for rec in reader.iter_records(poll_interval=0.0005):
    if rec["level_no"] >= 30:  # WARNING+
        print(rec["message"])
```

---

## Performance Considerations

- **Hot path writes** are a handful of atomic ops + a memcpy into mmap'd memory.
- **No syscalls** on the steady-state write path (after the initial mmap).
- **Batching**: Readers can `drain()` many frames at once to amortize overhead.
- **Backpressure**: monitor `drop_count` and `write_count`. If drops rise, increase capacity or slow the writer.
- **Memory footprint**: exactly `HEADER_SIZE + capacity`. For 64 MiB that's ~64 MiB + a small constant.
- **NUMA / huge pages**: for extreme workloads, place `/dev/shm` on a tmpfs or hugetlbfs mount and size capacity to huge-page multiples.

---

## Limitations & Non-Goals

- **Not a durable log**: data lives in RAM-backed shared memory; on reboot it's gone. Persist downstream if needed.
- **Single reader**: concurrent readers are not supported (they would race on `read_pos`).
- **No ordering across processes beyond arrival order**: if you need a global sequence number, put one in the payload.
- **Python GIL still applies** within a process; the lock-free property is about *cross-process* coordination.
- **No compression / encryption** at this layer; add it above or below as needed.

---

## Implementation Notes & Gotchas

1. **Fork safety**: After `fork()`, a child inherits the parent's mmap object but the underlying file descriptor state is shared. Prefer spawning fresh processes that each `open()` and `mmap()` the same path. The existing code works with `multiprocessing.Process` (which uses fork on Linux) because each process independently opens the file in `__init__`.

2. **Reader must keep up**: If a reader lags by more than `capacity` bytes, it may encounter overwritten frames. The ring doesn't detect this case explicitly; CRC or length checks will likely cause those frames to be skipped.

3. **CAS retry bound**: `_MAX_CAS_RETRIES = 128`. Under extreme contention a writer may give up and drop even if space exists. Raising this increases latency under contention; lowering it increases drop rate.

4. **Padding frame payload_len**: For a padding frame, `payload_len` is set to `remaining - FRAME_HDR_SIZE` (the bytes *after* the header within the padding region). The reader uses `frame_total_size(payload_len)` which will equal `remaining` because `remaining` was already aligned (since we only ever advance by aligned amounts or `remaining` itself when it was the gap).

5. **Unaligned capacity**: Creation rounds capacity up to `FRAME_ALIGN`. Readers attaching to an existing buffer trust the stored capacity.

---

## File Manifest

```
ring_buffer_sink/
├── __init__.py          # Public exports
├── sink.py              # RingBufferSink (Loguru integration)
├── reader.py            # RingBufferReader (consumer)
├── _ring_buffer.py      # Core RingBuffer (lock-free MPSC)
├── _frames.py           # encode/decode + frame math + CRC
├── _atomics.py          # ctypes loader + recompile guard
└── _atomic_ops.c        # C atomics (gcc → _atomic_ops.so)
```

Tests live under `tests/test_ringbuf_*.py`.

---

## How to Build & Test

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
python -m pytest tests/test_ringbuf_*.py -v
```

The C library compiles automatically on first import if `_atomic_ops.so` is missing or older than the `.c` source.

---

## Summary

This implementation delivers a minimal, auditable, high-performance logging path suitable for the most demanding low-latency, multi-process Python workloads. By combining lock-free cursors, explicit binary framing, per-frame CRCs, and a strict never-block contract, it meets the requirements of:

- Zero locks / zero blocking for writers
- Pre-allocated ring
- Variable-length frames with a simple checksum
- No pickle/json or external serializers
- Hard-crash survival with detection of torn data
- Concurrent spamming without data corruption

All verified by an extensive, process-based test suite that intentionally crashes writers at inconvenient times and then proves the reader never returns corrupted data.
