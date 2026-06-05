# Lock-Free Shared-Memory Ring Buffer Sink for Loguru

## Overview

This document describes the design, implementation, and verification of a high-performance, lock-free, multi-producer/single-consumer ring buffer sink for Loguru. The system enables multiple independent Python processes to write high-frequency logs to a single shared sink without blocking, locks, or serialization overhead.

**Key Properties:**
- Lock-free: No mutexes, semaphores, or file locks - only atomic CPU operations
- Non-blocking: Writers never wait; messages are dropped on full buffer  
- Crash-resilient: Buffer survives hard SIGKILL crashes of writers
- Zero-copy binary frames: Variable-length frames with CRC-32 checksums, no pickle/json
- Multi-process safe: Backed by POSIX shared memory (mmap over /dev/shm)
- High performance: Designed for trading-system log rates

---

## Architecture

### Components

```
ring_buffer_sink/
+-- __init__.py          # Public exports
+-- sink.py              # RingBufferSink - Loguru integration  
+-- reader.py            # RingBufferReader - Consumer API
+-- _ring_buffer.py      # Core RingBuffer (lock-free MPMC/MPSC)
+-- _frames.py           # Binary encoding (no external serializers)
+-- _atomics.py          # ctypes loader for C atomics
+-- _atomic_ops.c        # Hardware atomic operations (gcc compiled .so)
```

### Data Flow

Multiple Python processes attach to the same shared memory file via mmap.
Writers use CAS on write_pos cursor to claim space.
Reader uses read_pos (single-consumer) to consume frames.

---

## Binary Frame Format

Every frame is 8-byte aligned:

```
Offset  Size   Field
0       1      status (0x00=FREE, 0x01=WRITING, 0x02=READY, 0xFE=PADDING)
1       4      payload_len (uint32 LE)
5       4      crc32 (uint32 LE - CRC-32 of payload)
9       N      payload (N bytes)
9+N     pad    zero-padding to 8-byte boundary
```

Total = align8(9 + payload_len).

Log record payload (inside frame, little-endian):
- timestamp_ns (int64)
- level_no (uint8)
- pid (uint32)
- tid (uint64)  
- line_no (uint32)
- msg_len (uint32) + message bytes
- src_len (uint16) + source bytes

No pickle, json, or external serializers - only struct.pack.

CRC-32 (zlib.crc32) covers payload bytes only.

---

## Lock-Free Algorithm

### Header (64 bytes at start of file)
- magic: 0x4C4F47555F524230
- write_pos, read_pos: atomic monotonic cursors
- capacity, drop_count, write_count: atomics

### Writer (multi-producer)
1. Compute fsize = align8(9 + len)
2. CAS loop on write_pos to claim [wpos, wpos+fsize)
3. Handle wrap: if frame does not fit at end, write PADDING frame then retry at offset 0
4. Store STATUS_WRITING (release)
5. Write header (len, crc) + payload bytes
6. SEQ_CST fence
7. Store STATUS_READY (release)
8. Return True, or False on full/contention (drop_count++)

### Reader (single-consumer)
1. Load rpos, wpos
2. If empty, return None
3. Load status (acquire)
4. WRITING: skip if (wpos-rpos > cap/2) OR (rpos+claimed <= wpos) [crash recovery]
5. PADDING: skip the padding frame size
6. READY: validate bounds, copy payload, check CRC, advance rpos, return data
7. On any corruption: skip FRAME_ALIGN and continue (never hang)

---

## Crash Resilience

- SIGKILL or os._exit leaves shared memory intact
- Reader detects stale WRITING via gap heuristic or behind-wpos check
- CRC catches torn payloads
- Buffer remains writable after crashes
- Tests: 10+ kill cycles, concurrent multi-writer kills, post-crash recovery

---

## Concurrent Correctness

- write_pos only advanced by successful CAS (disjoint regions claimed)
- read_pos plain stores (single consumer invariant)
- Drops are explicit (non-blocking contract)

---

## Performance

- Pure userspace atomics, no futex/syscall on fast path
- 8-byte alignment for frames
- Bounded CAS retries before drop

---

## Testing

77 ringbuf-specific tests in venv:
- core: create/attach/wrap/padding/crc/gaps
- frames: encode/decode roundtrips  
- concurrent: N-process writers, integrity, live drain
- crash: SIGKILL, os._exit, repeated crashes
- integration: real loguru.logger usage
- robustness: 32-writer spam, crash+spam mixes, CRC stress

All tests: python -m pytest tests/test_ringbuf_*.py

---

## Usage

```python
from loguru import logger
from ring_buffer_sink import RingBufferSink, RingBufferReader

# Creator
sink = RingBufferSink("trading", capacity=64*1024*1024, create=True)
logger.add(sink)

# Attacher (other processes)  
sink = RingBufferSink("trading", create=False)
logger.add(sink)

# Reader
r = RingBufferReader("trading")
for rec in r.iter_records():
    print(rec)
```

Default shm path: /dev/shm/loguru_rb_{name}

---

## Requirements

- gcc for compiling _atomic_ops.c (first import)
- Writable /dev/shm (or explicit path= on tmpfs)
- Single consumer for reading

---

*Comprehensive implementation documentation for the specialized lock-free ring buffer Loguru sink.*
