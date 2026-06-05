"""Loguru sink that writes to the lock-free shared-memory ring buffer.

Usage::

    from loguru import logger
    from ring_buffer_sink import RingBufferSink

    # First (or only) process – creates the shared memory file.
    sink = RingBufferSink("trading", capacity=64 * 1024 * 1024, create=True)
    logger.add(sink)

    # Additional writer processes – attach to the existing buffer.
    sink = RingBufferSink("trading", create=False)
    logger.add(sink)
"""

from ._frames import encode_record
from ._ring_buffer import RingBuffer

_DEFAULT_CAPACITY = 64 * 1024 * 1024  # 64 MiB


class RingBufferSink:
    """A loguru-compatible sink backed by a lock-free ring buffer.

    Implements both the *callable* interface (``__call__``) and the
    *file-like* interface (``write`` / ``close``) so it works with every
    ``logger.add()`` call style.

    Parameters
    ----------
    name : str
        Logical name of the buffer.  The backing file is placed under
        ``/dev/shm/`` by default (override with *path*).
    capacity : int
        Data-region size in bytes (only used when *create* is True).
    create : bool
        Whether to create a fresh buffer or attach to an existing one.
    path : str or None
        Explicit file path.  When ``None``, ``/dev/shm/loguru_rb_{name}``
        is used.
    """

    def __init__(self, name, capacity=_DEFAULT_CAPACITY, create=True, path=None):
        if path is None:
            path = f"/dev/shm/loguru_rb_{name}"
        self._rb = RingBuffer(path, capacity=capacity, create=create)
        self._name = name

    def write(self, message):
        """Loguru sink entry-point (file-like interface).

        *message* is a ``str`` with a ``.record`` attribute holding the
        structured log record dict.
        """
        record = message.record
        payload = encode_record(record)
        self._rb.write(payload)

    def __call__(self, message):
        """Loguru sink entry-point (callable interface)."""
        self.write(message)

    def close(self):
        """Detach from the shared memory (does **not** delete the file)."""
        self._rb.close()

    @property
    def ring_buffer(self):
        """Direct access to the underlying :class:`RingBuffer`."""
        return self._rb

    def stats(self):
        return self._rb.stats()

    def __repr__(self):
        return (
            f"RingBufferSink(name={self._name!r}, "
            f"capacity={self._rb.capacity}, path={self._rb.path!r})"
        )
