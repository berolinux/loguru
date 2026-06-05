"""Lock-free, multi-producer / single-consumer ring buffer over POSIX shared memory.

Design
------
* Backed by a file in ``/dev/shm`` (or any path) that is ``mmap``-ed by
  every participating process.
* Writers claim space with a CAS loop on a monotonically increasing
  *write_pos* cursor.  No mutexes, semaphores or file locks are used.
* A per-frame *status byte* (written with release semantics) gates the
  reader: ``STATUS_READY`` means the payload + CRC are fully flushed.
* When the buffer is full writers **drop** the message (non-blocking).

Shared-memory layout (64-byte header + data region)
----------------------------------------------------

    Offset  Size  Field
    ------  ----  ------------------------------------------
     0       8    magic          0x4C4F47555F524230  ("LOGU_RB0")
     8       8    write_pos      monotonic byte cursor (atomic)
    16       8    read_pos       monotonic byte cursor (atomic)
    24       8    capacity       data-region size in bytes
    32       8    drop_count     messages dropped (atomic counter)
    40       8    write_count    messages written (atomic counter)
    48      16    (reserved)
    64       …    data region    (capacity bytes)
"""

import ctypes
import mmap
import os
import struct
import zlib

from ._atomics import get_lib
from ._frames import (
    FRAME_ALIGN,
    FRAME_HDR_SIZE,
    STATUS_FREE,
    STATUS_PADDING,
    STATUS_READY,
    STATUS_WRITING,
    frame_total_size,
)

# ── header layout ───────────────────────────────────────────────────
HEADER_SIZE = 64
OFF_MAGIC = 0
OFF_WRITE_POS = 8
OFF_READ_POS = 16
OFF_CAPACITY = 24
OFF_DROP_COUNT = 32
OFF_WRITE_COUNT = 40

MAGIC = 0x4C4F47555F524230  # "LOGU_RB0"

# Maximum CAS retries per write before dropping the message.
_MAX_CAS_RETRIES = 128


class RingBuffer:
    """Shared-memory lock-free ring buffer.

    Parameters
    ----------
    path : str
        File path for the backing shared memory (e.g. ``/dev/shm/mylog``).
    capacity : int or None
        Size of the data region in bytes.  Required when *create* is True.
        Automatically rounded up to the next multiple of ``FRAME_ALIGN``.
    create : bool
        If True, create (or truncate) the file and initialise the header.
        If False, open an existing buffer and validate the header.
    """

    def __init__(self, path, capacity=None, create=False):
        self._path = path
        self._lib = get_lib()
        self._mm = None

        if create:
            if capacity is None:
                raise ValueError("capacity is required when create=True")
            capacity = (capacity + FRAME_ALIGN - 1) & ~(FRAME_ALIGN - 1)
            total = HEADER_SIZE + capacity
            fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o666)
            try:
                os.ftruncate(fd, total)
                self._mm = mmap.mmap(fd, total)
            finally:
                os.close(fd)
            self._total = total
            self._setup_base()
            self._init_header(capacity)
        else:
            fd = os.open(path, os.O_RDWR)
            try:
                total = os.fstat(fd).st_size
                self._mm = mmap.mmap(fd, total)
            finally:
                os.close(fd)
            self._total = total
            self._setup_base()
            self._validate_header()
            capacity = self._load64(OFF_CAPACITY)

        self._capacity = capacity

    # ── internal helpers ────────────────────────────────────────────

    def _setup_base(self):
        self._arr = (ctypes.c_char * self._total).from_buffer(self._mm)
        self._base = ctypes.addressof(self._arr)

    def _ptr(self, offset):
        return self._base + offset

    def _load64(self, off):
        return self._lib.rb_atomic_load_u64(self._ptr(off))

    def _store64(self, off, val):
        self._lib.rb_atomic_store_u64(self._ptr(off), val)

    def _cas64(self, off, expected, desired):
        return self._lib.rb_atomic_cas_u64(self._ptr(off), expected, desired)

    def _fetch_add64(self, off, val):
        return self._lib.rb_atomic_fetch_add_u64(self._ptr(off), val)

    def _load8(self, off):
        return self._lib.rb_atomic_load_u8(self._ptr(off))

    def _store8(self, off, val):
        self._lib.rb_atomic_store_u8(self._ptr(off), val)

    def _fence(self):
        self._lib.rb_memory_fence()

    def _init_header(self, capacity):
        self._store64(OFF_MAGIC, MAGIC)
        self._store64(OFF_WRITE_POS, 0)
        self._store64(OFF_READ_POS, 0)
        self._store64(OFF_CAPACITY, capacity)
        self._store64(OFF_DROP_COUNT, 0)
        self._store64(OFF_WRITE_COUNT, 0)
        # Zero the data region
        self._mm[HEADER_SIZE : HEADER_SIZE + capacity] = b"\x00" * capacity

    def _validate_header(self):
        magic = self._load64(OFF_MAGIC)
        if magic != MAGIC:
            raise ValueError(
                f"Bad magic {magic:#018x}, expected {MAGIC:#018x}. "
                "Not a valid ring buffer or wrong version."
            )

    # ── public API ──────────────────────────────────────────────────

    @property
    def capacity(self):
        return self._capacity

    @property
    def path(self):
        return self._path

    def write(self, payload):
        """Write *payload* bytes into the buffer.

        Returns ``True`` on success, ``False`` if the message was dropped
        (buffer full or CAS contention exhausted).  **Never blocks.**
        """
        payload_len = len(payload)
        fsize = frame_total_size(payload_len)
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        cap = self._capacity

        for _ in range(_MAX_CAS_RETRIES):
            wpos = self._load64(OFF_WRITE_POS)
            rpos = self._load64(OFF_READ_POS)

            offset = wpos % cap
            remaining = cap - offset

            # How many bytes we actually need to advance write_pos
            if remaining < FRAME_HDR_SIZE:
                needed = remaining + fsize      # gap + frame at start
            elif remaining < fsize:
                needed = remaining + fsize      # padding + frame at start
            else:
                needed = fsize

            if wpos - rpos + needed > cap:
                self._fetch_add64(OFF_DROP_COUNT, 1)
                return False

            # ---- case 1: tiny gap at the very end ----
            if remaining < FRAME_HDR_SIZE:
                new_wpos = wpos + remaining
                if self._cas64(OFF_WRITE_POS, wpos, new_wpos) == wpos:
                    # Mark gap byte so reader can see STATUS_PADDING even if
                    # remaining >= 1 (it always is: min remaining = FRAME_ALIGN)
                    self._store8(HEADER_SIZE + offset, STATUS_PADDING)
                continue

            # ---- case 2: not enough room for this frame → padding ----
            if remaining < fsize:
                new_wpos = wpos + remaining
                if self._cas64(OFF_WRITE_POS, wpos, new_wpos) == wpos:
                    doff = HEADER_SIZE + offset
                    pad_payload_len = remaining - FRAME_HDR_SIZE
                    struct.pack_into("<II", self._mm, doff + 1,
                                    pad_payload_len, 0)
                    self._fence()
                    self._store8(doff, STATUS_PADDING)
                continue

            # ---- case 3: normal write ----
            new_wpos = wpos + fsize
            if self._cas64(OFF_WRITE_POS, wpos, new_wpos) == wpos:
                doff = HEADER_SIZE + offset
                # Mark as "being written" first (crash guard)
                self._store8(doff, STATUS_WRITING)
                struct.pack_into("<II", self._mm, doff + 1, payload_len, crc)
                self._mm[doff + FRAME_HDR_SIZE : doff + FRAME_HDR_SIZE + payload_len] = payload
                self._fence()
                self._store8(doff, STATUS_READY)
                self._fetch_add64(OFF_WRITE_COUNT, 1)
                return True

        # Too much CAS contention – drop.
        self._fetch_add64(OFF_DROP_COUNT, 1)
        return False

    def try_read(self):
        """Read the next frame payload.

        Returns *bytes* on success or ``None`` when the buffer is empty,
        a frame is still being written, or the next frame is corrupt
        (in which case the read cursor is advanced past the bad frame).

        Single-consumer only – no CAS is used on ``read_pos``.
        """
        cap = self._capacity
        max_skip = 256

        for _ in range(max_skip):
            rpos = self._load64(OFF_READ_POS)
            wpos = self._load64(OFF_WRITE_POS)
            if rpos >= wpos:
                return None

            offset = rpos % cap
            remaining = cap - offset

            # Tiny gap at end of buffer
            if remaining < FRAME_HDR_SIZE:
                self._store64(OFF_READ_POS, rpos + remaining)
                continue

            doff = HEADER_SIZE + offset
            status = self._load8(doff)

            if status == STATUS_FREE:
                return None
            if status == STATUS_WRITING:
                # Writer either is still writing, or crashed mid-frame.
                # Heuristics for detecting a crashed (stale) WRITING frame:
                # 1. The write cursor is far ahead of read cursor (> cap/2).
                # 2. The claimed frame region lies entirely at or before wpos,
                #    meaning a writer reserved the space, set WRITING, but never
                #    committed READY before wpos advanced past it.
                plen_raw = struct.unpack_from("<I", self._mm, doff + 1)[0]
                skip = frame_total_size(plen_raw) if 0 < plen_raw < cap else FRAME_ALIGN
                if (wpos - rpos > cap // 2) or (rpos + skip <= wpos):
                    self._store64(OFF_READ_POS, rpos + skip)
                    continue
                return None

            payload_len = struct.unpack_from("<I", self._mm, doff + 1)[0]
            crc_stored = struct.unpack_from("<I", self._mm, doff + 5)[0]
            fsize = frame_total_size(payload_len)

            if payload_len > cap or fsize > remaining:
                self._store64(OFF_READ_POS, rpos + FRAME_ALIGN)
                continue

            if status == STATUS_PADDING:
                self._store64(OFF_READ_POS, rpos + fsize)
                continue

            if status != STATUS_READY:
                self._store64(OFF_READ_POS, rpos + FRAME_ALIGN)
                continue

            self._fence()
            payload = bytes(
                self._mm[doff + FRAME_HDR_SIZE : doff + FRAME_HDR_SIZE + payload_len]
            )

            crc_actual = zlib.crc32(payload) & 0xFFFFFFFF
            if crc_actual != crc_stored:
                self._store64(OFF_READ_POS, rpos + fsize)
                continue

            self._store64(OFF_READ_POS, rpos + fsize)
            return payload

        return None

    def drain(self, limit=None):
        """Read up to *limit* payloads (all if *limit* is ``None``)."""
        results = []
        count = 0
        while limit is None or count < limit:
            data = self.try_read()
            if data is None:
                break
            results.append(data)
            count += 1
        return results

    def stats(self):
        return {
            "write_pos": self._load64(OFF_WRITE_POS),
            "read_pos": self._load64(OFF_READ_POS),
            "capacity": self._capacity,
            "drop_count": self._load64(OFF_DROP_COUNT),
            "write_count": self._load64(OFF_WRITE_COUNT),
        }

    def close(self):
        if self._mm is not None:
            # Release the ctypes array first – it holds a buffer reference
            # that prevents mmap.close().
            self._arr = None
            self._base = 0
            self._mm.close()
            self._mm = None

    def unlink(self):
        """Remove the backing shared-memory file."""
        try:
            os.unlink(self._path)
        except FileNotFoundError:
            pass

    def __del__(self):
        self.close()
