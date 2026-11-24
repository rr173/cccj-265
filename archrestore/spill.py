"""Out-of-core staging of decoded evidence.

A page is decoded exactly once; the decoded canonical value stream is written
to a temporary spill file.  Spill files are indexed every ``BLOCK_ROWS`` rows
so a reader can seek to a logical row without holding per-row offsets and
without re-opening or re-decoding the page.

Values are never materialised whole: a :class:`RawValue` is a small descriptor
(tag, length, offset) and content is streamed in bounded chunks.  This is what
lets values be much larger than process memory while still being comparable,
hashable and writable.
"""

import array
import os
from collections import OrderedDict

from .errors import DecodeError, EvidenceError

BLOCK_ROWS = 1024
IO_CHUNK = 64 * 1024


class FilePool(object):
    """Bounded LRU cache of read-only file descriptors (pread access)."""

    def __init__(self, max_open=64):
        self.max_open = max(1, int(max_open))
        self._fds = OrderedDict()

    def _get(self, path):
        fd = self._fds.get(path)
        if fd is not None:
            self._fds.move_to_end(path)
            return fd
        fd = os.open(path, os.O_RDONLY)
        self._fds[path] = fd
        while len(self._fds) > self.max_open:
            _, old = self._fds.popitem(last=False)
            try:
                os.close(old)
            except OSError:
                pass
        return fd

    def pread(self, path, n, offset):
        fd = self._get(path)
        parts = []
        got = 0
        while got < n:
            chunk = os.pread(fd, n - got, offset + got)
            if not chunk:
                raise EvidenceError("unexpected EOF reading spill %r at %d"
                                    % (path, offset + got))
            parts.append(chunk)
            got += len(chunk)
        return parts[0] if len(parts) == 1 else b"".join(parts)

    def pread_upto(self, path, n, offset):
        """Read up to n bytes; returns fewer only at EOF."""
        return os.pread(self._get(path), n, offset)

    def close(self):
        for fd in self._fds.values():
            try:
                os.close(fd)
            except OSError:
                pass
        self._fds.clear()


class SpoolWriter(object):
    """Append-only canonical-value writer with block-offset recording.

    Row 0 of the stream corresponds to absolute row ``start_row``.  The offset
    of every BLOCK_ROWS-th stream row is recorded, so a reader can seek any
    row with at most BLOCK_ROWS bounded reads and no re-decode.
    """

    __slots__ = ("path", "start_row", "row_count", "offsets", "_fh", "_pos",
                 "_cur")

    def __init__(self, path, start_row):
        self.path = path
        self.start_row = start_row
        self.row_count = 0
        self.offsets = array.array("Q")
        self._cur = None
        self._fh = open(path, "wb")
        self._pos = 0

    def begin_value(self):
        """Start a new value: record a block offset, then accept parts."""
        if self.row_count % BLOCK_ROWS == 0:
            self.offsets.append(self._pos)
        self.row_count += 1
        self._cur = self._pos
        return self._cur

    def write(self, data):
        if data:
            self._fh.write(data)
            self._pos += len(data)

    def current_offset(self):
        return self._cur

    def tell(self):
        return self._pos

    def close(self):
        self._fh.close()

    def abort(self):
        try:
            self._fh.close()
        finally:
            try:
                os.unlink(self.path)
            except OSError:
                pass


class RawValue(object):
    """Descriptor for one canonical value; content stays on disk."""

    __slots__ = ("tag", "inline", "path", "offset", "length")

    def __init__(self, tag, inline=b"", path=None, offset=0, length=0):
        self.tag = tag
        self.inline = inline
        self.path = path
        self.offset = offset
        self.length = length  # content bytes for strings; 0 otherwise

    def total_length(self):
        if self.tag == 0:
            return 1
        if self.tag == 1:
            return 5 + self.length
        return 9

    def header(self):
        if self.tag == 0:
            return b"\x00"
        if self.tag == 1:
            return b"\x01" + self.length.to_bytes(4, "little")
        return b"\x02" + self.inline

    def iter_chunks(self, pool, chunk=IO_CHUNK):
        """Yield only the string body; for null/integer the body is empty.

        The full canonical bytes are ``header()`` followed by these chunks.
        """
        if self.tag != 1:
            return
        pos = self.offset
        remaining = self.length
        while remaining:
            want = chunk if remaining > chunk else remaining
            data = pool.pread(self.path, want, pos)
            yield data
            pos += len(data)
            remaining -= len(data)

    def feed(self, hasher, pool):
        hasher.update_stream(self.header(), self.iter_chunks(pool))

    def equals(self, other, pool):
        if self.tag != other.tag:
            return False
        if self.tag == 0:
            return True
        if self.tag == 2:
            return self.inline == other.inline
        if self.length != other.length:
            return False
        pa, pb = self.offset, other.offset
        remaining = self.length
        while remaining:
            want = IO_CHUNK if remaining > IO_CHUNK else remaining
            a = pool.pread(self.path, want, pa)
            b = pool.pread(other.path, want, pb)
            if a != b:
                return False
            pa += len(a)
            pb += len(b)
            remaining -= len(a)
        return True

    def copy_to(self, writer, pool):
        writer.write(self.header())
        for chunk in self.iter_chunks(pool):
            writer.write(chunk)


class Spool(object):
    """Block-indexed decoded page."""

    __slots__ = ("path", "start", "end", "offsets")

    def __init__(self, path, start, end, offsets):
        self.path = path
        self.start = start
        self.end = end
        self.offsets = offsets

    @property
    def row_count(self):
        return self.end - self.start

    def reader(self, pool):
        return SpoolReader(self, pool)


class SpoolReader(object):
    """Forward cursor over a :class:`Spool`, with block-indexed seeking.

    A small read buffer amortises syscalls; a string body is skipped by
    arithmetic rather than read, and is fetched later through the pool in
    bounded chunks.  Seeking uses the block index and skips at most
    ``BLOCK_ROWS`` values, never re-reading or re-decoding the source page.
    """

    __slots__ = ("spool", "pool", "row", "pos", "_buf", "_buf_start")

    def __init__(self, spool, pool):
        self.spool = spool
        self.pool = pool
        self.row = spool.start
        self.pos = 0
        self._buf = b""
        self._buf_start = 0

    def _refill(self):
        self._buf = self.pool.pread_upto(self.spool.path, IO_CHUNK, self.pos)
        self._buf_start = self.pos
        if not self._buf:
            raise EvidenceError("spill %r ended at offset %d"
                                % (self.spool.path, self.pos))

    def _bytes(self, n):
        end = self._buf_start + len(self._buf)
        if self._buf_start <= self.pos and self.pos + n <= end:
            i = self.pos - self._buf_start
            out = self._buf[i:i + n]
            self.pos += n
            return out
        # Assemble across refills; this matters only if the buffer size is
        # smaller than the fixed-size header (null tag / u32 length / i64).
        parts = []
        need = n
        while need:
            end = self._buf_start + len(self._buf)
            if not (self._buf_start <= self.pos < end):
                self._refill()
            i = self.pos - self._buf_start
            take = min(need, len(self._buf) - i)
            if take <= 0:  # pragma: no cover - defensive
                raise EvidenceError("truncated spill %r at offset %d"
                                    % (self.spool.path, self.pos))
            parts.append(self._buf[i:i + take])
            self.pos += take
            need -= take
        return parts[0] if len(parts) == 1 else b"".join(parts)

    def seek_row(self, row):
        s = self.spool
        if not (s.start <= row <= s.end):
            raise EvidenceError("row %d outside spool [%d, %d)"
                                % (row, s.start, s.end))
        if row == self.row:
            return
        block = (row - s.start) // BLOCK_ROWS
        self.pos = s.offsets[block]
        self.row = s.start + block * BLOCK_ROWS
        self._buf = b""
        self._buf_start = self.pos
        while self.row < row:
            self._skip_one()
            self.row += 1
        if self.row != row:  # pragma: no cover - defensive
            raise EvidenceError("spool seek overshoot at row %d" % row)

    def _skip_one(self):
        tag = self._bytes(1)[0]
        if tag == 0:
            return
        if tag == 1:
            length = int.from_bytes(self._bytes(4), "little")
            self.pos += length
            return
        if tag == 2:
            self._bytes(8)
            return
        raise EvidenceError("corrupt spill tag 0x%02x in %r"
                            % (tag, self.spool.path))

    def next_value(self):
        if self.row >= self.spool.end:
            raise EvidenceError("spool exhausted at row %d" % self.row)
        start = self.pos
        tag = self._bytes(1)[0]
        if tag == 0:
            self.row += 1
            return RawValue(0)
        if tag == 1:
            length = int.from_bytes(self._bytes(4), "little")
            self.pos += length  # body is fetched lazily through the pool
            self.row += 1
            return RawValue(1, path=self.spool.path, offset=start + 5,
                            length=length)
        if tag == 2:
            body = self._bytes(8)
            self.row += 1
            return RawValue(2, inline=body)
        raise EvidenceError("corrupt spill tag 0x%02x at %d"
                            % (tag, start))


class DictSpill(object):
    """A parsed dictionary: entry bodies spilled, u64 offsets spilled.

    Memory is O(1) in the number of entries; ``entry(code)`` performs a
    bounded number of preads and returns a streaming RawValue.
    """

    __slots__ = ("digest", "byte_length", "count", "spill_path", "index_path",
                 "pool", "_cache")

    CACHE = 4096

    def __init__(self, digest, byte_length, count, spill_path, index_path,
                 pool=None):
        self.digest = digest
        self.byte_length = byte_length
        self.count = count
        self.spill_path = spill_path
        self.index_path = index_path
        self.pool = pool
        self._cache = {}

    def entry(self, code):
        if code < 0 or code >= self.count:
            raise DecodeError("dictionary code %d out of range [0, %d)"
                              % (code, self.count))
        cached = self._cache.get(code)
        if cached is not None:
            return cached
        off_raw = self.pool.pread(self.index_path, 8, code * 8)
        pos = int.from_bytes(off_raw, "little")
        tag = self.pool.pread(self.spill_path, 1, pos)[0]
        if tag == 0:
            v = RawValue(0)
        elif tag == 1:
            hdr = self.pool.pread(self.spill_path, 5, pos)
            length = int.from_bytes(hdr[1:5], "little")
            v = RawValue(1, path=self.spill_path, offset=pos + 5, length=length)
        elif tag == 2:
            body = self.pool.pread(self.spill_path, 8, pos + 1)
            v = RawValue(2, inline=body)
        else:
            raise DecodeError("corrupt dictionary entry tag 0x%02x" % tag)
        if len(self._cache) >= self.CACHE:
            self._cache.clear()
        self._cache[code] = v
        return v
