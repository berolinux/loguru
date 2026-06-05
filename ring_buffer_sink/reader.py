"""Consumer / reader for the shared-memory ring buffer.

Usage::

    from ring_buffer_sink import RingBufferReader

    reader = RingBufferReader("trading")
    for record in reader.iter_records():
        print(record["message"])
"""

import time

from ._frames import decode_record
from ._ring_buffer import RingBuffer


class RingBufferReader:
    """Read log records that were written via :class:`RingBufferSink`.

    Parameters
    ----------
    name : str or None
        Logical buffer name (same as the one passed to the sink).
    path : str or None
        Explicit file path.  Takes precedence over *name*.
    """

    def __init__(self, name=None, path=None):
        if path is None:
            if name is None:
                raise ValueError("Provide either name or path")
            path = f"/dev/shm/loguru_rb_{name}"
        self._rb = RingBuffer(path, create=False)

    def read_record(self):
        """Return the next decoded record dict, or ``None``."""
        data = self._rb.try_read()
        if data is None:
            return None
        return decode_record(data)

    def read_raw(self):
        """Return the next raw payload bytes, or ``None``."""
        return self._rb.try_read()

    def drain_records(self, limit=None):
        """Read up to *limit* decoded records (all available if ``None``)."""
        records = []
        count = 0
        while limit is None or count < limit:
            rec = self.read_record()
            if rec is None:
                break
            records.append(rec)
            count += 1
        return records

    def iter_records(self, poll_interval=0.001):
        """Yield records forever, sleeping *poll_interval* seconds when idle."""
        while True:
            rec = self.read_record()
            if rec is not None:
                yield rec
            else:
                time.sleep(poll_interval)

    def stats(self):
        return self._rb.stats()

    def close(self):
        self._rb.close()

    @property
    def ring_buffer(self):
        return self._rb
