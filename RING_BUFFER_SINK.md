# Lock-free shared-memory ring buffer sink for Loguru

This document describes the `ring_buffer_sink/` package: a **multi-producer**, **single-consumer** logging path backed by a **memory-mapped file** (typically under `/dev/shm`) so many independent Python processes can append log records to **one** buffer without **mutexes, file locks, or blocking syscalls** on the hot path.

## Why this design fits a trading stack

- **Throughput**: Writers only perform a short CAS loop plus a sequential copy into shared memory. When the buffer is full, writes **fail fast** (`write()` returns `False`) instead of blocking.
- **Process isolation**: Workers attach to the same path; no shared parent process is required.
- **Crash isolation**: A status byte plus **FNV-1a** checksum means readers never return torn payloads as “successfully decoded”; corrupt or half-written frames are skipped.

## Non-goals / caveats

- **Single consumer** for `try_read()` / `RingBufferReader`: only one thread/process may advance `read_pos`. Multiple readers need an external fan-out.
- **Loguru handler lock**: `logger.add(RingBufferSink(...))` still uses Loguru’s normal handler machinery (including an internal **threading** lock in the **current** process) so that Loguru’s own formatting and filtering stay consistent. That lock is **not** shared across processes; the **ring buffer** itself stays lock-free and non-blocking for writers.
- **gcc required**: Hardware atomics are exposed through a tiny shared object built from `ring_buffer_sink/_atomic_ops.c` on first import.
- **Linux-first**: Layout and tests assume a POSIX shared-memory file (e.g. `tmpfs` at `/dev/shm`). Other platforms would need an equivalent fast, sparse-safe mmap backing file.

## Layout on disk / in memory

| Region | Size | Content |
|--------|------|---------|
| Header | 64 B | Magic, 64-bit `write_pos`, `read_pos`, `capacity`, drop/write counters (all atomics for the 64-bit fields). |
| Data | `capacity` | Ring of **variable-length frames**, 8-byte aligned. |

`write_pos` / `read_pos` are **monotonic byte cursors**; physical offsets use `% capacity`. Wrapping uses **padding frames** so a record never splits across the end of the mmap.

### Per-frame binary layout

Each frame is `frame_total_size(payload_len)` bytes (multiple of 8):

| Offset (in frame) | Size | Field |
|-------------------|------|--------|
| 0 | 1 | `status` — `FREE`, `WRITING`, `READY`, `PADDING` |
| 1 | 4 | `payload_len` — little-endian `uint32` |
| 5 | 4 | `checksum` — little-endian `uint32`, FNV-1a over payload bytes only |
| 9 | `payload_len` | Raw payload |
| … | pad | Zeros to 8-byte boundary |

**Checksum**: 32-bit FNV-1a in pure Python (`compute_checksum` in `_frames.py`). No `pickle`, `json`, or third-party serializers on the wire.

### Log record payload (inside the frame)

After the frame header, the **payload** is the concatenation of:

- Fixed record header (`struct`: `timestamp_ns` int64, `level` uint8, `pid` uint32, `tid` uint64, `line` uint32).
- `msg_len` uint32 + UTF-8 message bytes (replacement for undecodable chars).
- `src_len` uint16 + UTF-8 `"filename:function"`.

`RingBufferReader.read_record()` decodes this back into a plain `dict`.

## Writer algorithm (lock-free)

1. Load `write_pos`, `read_pos`, derive **used** bytes and the longest **contiguous** free span starting at `write_pos % capacity` (this fixes wrap-around cases where “bytes to end of mmap” ≠ “contiguous free space”).
2. If a message does not fit in total free space → increment `drop_count`, return `False`.
3. If the writer is in a **tiny physical tail** (`capacity - offset < 9`) → advance `write_pos` with a padding marker (same idea as before: reader skips).
4. If unread data sits too close for a full header (`first_contig < 9`) → advance with a single-byte padding marker.
5. Else if `first_contig < frame_size` → emit a **padding frame** consuming that span, retry loop.
6. Else CAS `write_pos` forward by `frame_size`, write `WRITING`, then payload + FNV-1a, memory fence, `READY`, increment `write_count`.

Steps 3–5 may run several times under contention; after `_MAX_CAS_RETRIES` failed CAS attempts the message is dropped so writers never livelock.

Atomic primitives live in `_atomic_ops.c` (`__atomic_*`, release/acquire for the status byte, seq_cst for 64-bit words).

## Reader algorithm

Single consumer walks frames starting at `read_pos`:

- Skips structural gaps and `PADDING`.
- Waits (`None`) on `WRITING` unless the gap vs. `write_pos` suggests a crashed writer, in which case it skips using a best-effort size decode.
- On `READY`, re-verifies FNV-1a; mismatch → skip frame (checksum trap).

## Testing

Tests live under `tests/test_ringbuf_*.py` (~70 cases), covering:

- Core FIFO, wrap, padding, “tiny tail” gaps, stats.
- **Concurrent spam** from many processes (tagged payloads, duplicate detection, drop accounting).
- **SIGKILL / `os._exit`** writers mid-stream; mmap must remain navigable and new writers usable.
- **Mmap corruption** (random flips in the data region) and bad checksum headers.
- End-to-end **Loguru** `logger.add(RingBufferSink(...))` + `RingBufferReader`.

Run inside a virtual environment (from repo root):

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/pytest tests/test_ringbuf_*.py -v
```

Ensure `gcc` is installed so `_atomic_ops.so` can be built on first import.

## Public API (short)

| Symbol | Role |
|--------|------|
| `RingBufferSink(name, capacity=…, create=True, path=…)` | Loguru sink (`write` / `__call__`). |
| `RingBufferReader(name=…, path=…)` | Consumer helper with `read_record()`, `drain_records()`, `iter_records()`. |
| `RingBuffer(path, capacity=…, create=…)` | Low-level `write(bytes)` / `try_read()` / `stats()`. |

## Files

| Path | Purpose |
|------|---------|
| `ring_buffer_sink/sink.py` | Loguru adapter |
| `ring_buffer_sink/reader.py` | Decoding reader |
| `ring_buffer_sink/_ring_buffer.py` | mmap + producer/consumer |
| `ring_buffer_sink/_frames.py` | FNV-1a + record struct helpers |
| `ring_buffer_sink/_atomics.py` | On-demand compile of `_atomic_ops.so` |
| `ring_buffer_sink/_atomic_ops.c` | GCC atomics |

## Operational hints

- Size `capacity` for peak **burst bytes in flight**, not average log rate; overfull buffers drop logs by design.
- Place the backing file on **tmpfs** (`/dev/shm/...`) to avoid SSD wear and reduce latency.
- For minimum latency to Loguru, keep formatting cheap and consider logging mostly static or numeric fields already present in the record dict.
