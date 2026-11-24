"""Decoding of verified evidence bytes.

A page is opened once, then read exactly once: hashing and decoding happen in
the same forward pass.  Decoded canonical values are spilled to disk and
indexed per block, so the page is never re-opened and never re-decoded and no
whole page, column or slot ever lives in memory.

A dictionary is likewise verified and parsed in one forward pass and shared
through :class:`DictRepository` by its *content identity* ``(sha256,
byte_length)`` -- never by dictionary name, page number or file position.  A
dictionary page is only usable evidence if the dictionary can be obtained
from some replica and verifies; a missing, truncated, wrong-length or
wrong-digest dictionary, and any out-of-range code, makes the page bad
evidence (it is skipped and reported), not a conflicting value.

Rejection reasons are stable tokens:
    page_truncated, page_digest_mismatch, page_body_length_mismatch,
    page_decode_error, wrong_value_type, dict_code_out_of_range,
    dict_page_length_mismatch, dictionary_unavailable, dictionary_invalid,
    io_error.
"""

import codecs
import hashlib
import os
import stat as _stat

from .errors import DecodeError, EvidenceError, PageRejected
from .format import TYPE_STRING, DICT_MAGIC
from .spill import DictSpill, Spool, SpoolWriter, IO_CHUNK

BLOCK_ROWS = 1024


# ---------------------------------------------------------------------------
# Safe evidence opening and bounded range reader
# ---------------------------------------------------------------------------


def open_evidence(directory, location, declared_length):
    """Resolve a location safely and return ``(fd, absolute_offset)``.

    The path must resolve (after symlinks) inside the replica directory, be a
    regular file at open time, and be large enough for the declared range.
    """
    if directory is None:
        raise EvidenceError("no replica directory supplied for %r"
                            % (location.get("file"),))
    root = os.path.realpath(directory)
    if not os.path.isdir(root):
        raise EvidenceError("replica directory %r is not a directory"
                            % (directory,))
    target = os.path.realpath(os.path.join(root, location["file"]))
    try:
        common = os.path.commonpath([root, target])
    except ValueError:
        common = None
    if common != root:
        raise EvidenceError("location %r escapes the replica directory"
                            % (location["file"],))
    try:
        st = os.stat(target)
    except OSError as exc:
        raise EvidenceError("cannot stat %r: %s" % (location["file"], exc))
    if not _stat.S_ISREG(st.st_mode):
        raise EvidenceError("location %r is not a regular file"
                            % (location["file"],))
    try:
        fd = os.open(target, os.O_RDONLY)
    except OSError as exc:
        raise EvidenceError("cannot open %r: %s" % (location["file"], exc))
    st2 = os.fstat(fd)
    if not _stat.S_ISREG(st2.st_mode) or \
            (st2.st_dev, st2.st_ino) != (st.st_dev, st.st_ino):
        os.close(fd)
        raise EvidenceError("location %r changed identity while opening"
                            % (location["file"],))
    offset = location["offset"]
    if offset + declared_length > st2.st_size:
        os.close(fd)
        raise EvidenceError(
            "declared %d bytes at offset %d in %r exceeds file size %d "
            "(truncated)" % (declared_length, offset, location["file"],
                             st2.st_size))
    return fd, offset


class RangeReader(object):
    """Forward reader over a bounded file range that hashes what it reads.

    Memory is O(IO_CHUNK).  :meth:`copy_exact` streams large bodies straight
    to a sink, so a value may be arbitrarily larger than process memory.
    """

    __slots__ = ("fd", "start", "length", "end", "_file_pos", "_buf",
                 "_buf_abs", "_i", "_hasher", "bufsize")

    def __init__(self, fd, offset, length, bufsize=IO_CHUNK):
        self.fd = fd
        self.start = offset
        self.length = length
        self.end = offset + length
        self._file_pos = offset
        self._buf = b""
        self._buf_abs = offset
        self._i = 0
        self._hasher = hashlib.sha256()
        self.bufsize = bufsize

    def _fill(self):
        if self._i < len(self._buf):
            return
        want = self.end - self._file_pos
        if want <= 0:
            return
        n = min(self.bufsize, want)
        data = os.pread(self.fd, n, self._file_pos)
        if len(data) != n:
            raise EvidenceError("short read: wanted %d bytes at offset %d, "
                                "got %d" % (n, self._file_pos, len(data)))
        self._buf = data
        self._buf_abs = self._file_pos
        self._i = 0
        self._file_pos += n
        self._hasher.update(data)

    def remaining(self):
        return (len(self._buf) - self._i) + (self.end - self._file_pos)

    def read_exact(self, n):
        """Read n bytes into memory; callers must bound n."""
        if n < 0 or n > self.remaining():
            raise EvidenceError("read of %d bytes with %d remaining"
                                % (n, self.remaining()))
        if n == 0:
            return b""
        if n <= len(self._buf) - self._i:
            out = self._buf[self._i:self._i + n]
            self._i += n
            return out
        parts = []
        need = n
        while need:
            if self._i >= len(self._buf):
                self._fill()
                if self._i >= len(self._buf):
                    raise EvidenceError("unexpected EOF")
            take = min(need, len(self._buf) - self._i)
            parts.append(self._buf[self._i:self._i + take])
            self._i += take
            need -= take
        return b"".join(parts)

    def copy_exact(self, n, sink, on_chunk=None):
        """Stream n bytes to *sink*; never buffers more than one chunk."""
        if n < 0 or n > self.remaining():
            raise EvidenceError("copy of %d bytes with %d remaining"
                                % (n, self.remaining()))
        need = n
        while need:
            if self._i >= len(self._buf):
                self._fill()
                if self._i >= len(self._buf):
                    raise EvidenceError("unexpected EOF")
            take = min(need, len(self._buf) - self._i)
            chunk = self._buf[self._i:self._i + take]
            self._i += take
            need -= take
            if on_chunk is not None:
                on_chunk(chunk)
            sink(chunk)

    def finish(self):
        if self.remaining() != 0:
            raise EvidenceError("%d undeclared bytes remain in the declared "
                                "range" % self.remaining())
        return self._hasher.hexdigest()


class Utf8Checker(object):
    """Streaming strict UTF-8 validation, memory O(chunk)."""

    __slots__ = ("_decoder",)

    def __init__(self):
        self._decoder = codecs.getincrementaldecoder("utf-8")("strict")

    def feed(self, chunk):
        try:
            self._decoder.decode(chunk, False)
        except UnicodeDecodeError as exc:
            raise DecodeError("string is not valid UTF-8: %s" % exc)

    def finish(self):
        try:
            self._decoder.decode(b"", True)
        except UnicodeDecodeError as exc:
            raise DecodeError("string ends inside a UTF-8 sequence: %s" % exc)


# ---------------------------------------------------------------------------
# Page bodies
# ---------------------------------------------------------------------------


def _check_tag(tag, ctype, where):
    if tag == 0:
        return
    if ctype == TYPE_STRING and tag == 1:
        return
    if ctype == "integer" and tag == 2:
        return
    raise PageRejected("wrong_value_type",
                       "%s has value tag 0x%02x, which cannot occur in a %r "
                       "column" % (where, tag, ctype))


def decode_plain(page, reader, writer):
    rows = page.row_count
    ctype = page.column_type
    for i in range(rows):
        writer.begin_value()
        if reader.remaining() < 1:
            raise PageRejected("page_body_length_mismatch",
                               "plain page row %d of %d is missing"
                               % (i, rows))
        tag = reader.read_exact(1)[0]
        _check_tag(tag, ctype, "plain page row %d" % i)
        if tag == 0:
            writer.write(b"\x00")
        elif tag == 1:
            if reader.remaining() < 4:
                raise PageRejected("page_body_length_mismatch",
                                   "plain page row %d: truncated string length"
                                   % i)
            length = int.from_bytes(reader.read_exact(4), "little")
            if length > reader.remaining():
                raise PageRejected(
                    "page_body_length_mismatch",
                    "plain page row %d: string length %d exceeds the %d "
                    "remaining bytes" % (i, length, reader.remaining()))
            writer.write(b"\x01" + length.to_bytes(4, "little"))
            checker = Utf8Checker()
            reader.copy_exact(length, writer.write, checker.feed)
            checker.finish()
        else:
            if reader.remaining() < 8:
                raise PageRejected("page_body_length_mismatch",
                                   "plain page row %d: truncated integer" % i)
            writer.write(b"\x02" + reader.read_exact(8))
    if reader.remaining() != 0:
        raise PageRejected(
            "page_body_length_mismatch",
            "plain page declares %d rows but has %d extra bytes"
            % (rows, reader.remaining()))


def decode_dict_page(page, reader, writer, dictionary):
    rows = page.row_count
    ctype = page.column_type
    expected = rows * 4
    have = reader.remaining()
    if have != expected:
        raise PageRejected(
            "dict_page_length_mismatch",
            "dictionary page body is %d bytes but row_count %d requires "
            "exactly %d (a wrong page length is not usable evidence)"
            % (have, rows, expected))
    done = 0
    while done < rows:
        block = min(BLOCK_ROWS, rows - done)
        raw = reader.read_exact(block * 4)
        resolved = {}
        for j in range(block):
            off = j * 4
            code = int.from_bytes(raw[off:off + 4], "little")
            row = done + j
            if code >= dictionary.count:
                raise PageRejected(
                    "dict_code_out_of_range",
                    "dictionary page row %d: code %d is outside [0, %d)"
                    % (row, code, dictionary.count))
            entry = resolved.get(code)
            if entry is None:
                entry = dictionary.entry(code)
                _check_tag(entry.tag, ctype,
                           "dictionary entry %d used at row %d" % (code, row))
                resolved[code] = entry
            writer.begin_value()
            writer.write(entry.header())
            if entry.tag == 1:
                for chunk in entry.iter_chunks(dictionary.pool):
                    writer.write(chunk)
        done += block


# ---------------------------------------------------------------------------
# Page decode entry point
# ---------------------------------------------------------------------------


def decode_page(page, workdir, tag_prefix, dict_repo, directory):
    """Verify and decode one page exactly once; return a :class:`Spool`."""
    path = os.path.join(workdir, "%s.page%d-%d.spool"
                        % (tag_prefix, page.page, page.start))
    fd = None
    writer = None
    try:
        fd, abs_off = open_evidence(directory, page.location, page.byte_length)
        reader = RangeReader(fd, abs_off, page.byte_length)
        writer = SpoolWriter(path, page.start)
        if page.encoding == "plain":
            decode_plain(page, reader, writer)
        else:
            try:
                dictionary, _src = dict_repo.resolve(
                    (page.dict_ref.digest, page.dict_ref.byte_length))
            except PageRejected as exc:
                raise PageRejected("dictionary_unavailable", exc.detail)
            decode_dict_page(page, reader, writer, dictionary)
        digest = reader.finish()
        if digest != page.byte_digest:
            raise PageRejected(
                "page_digest_mismatch",
                "declared %s, computed %s (body bytes do not match the "
                "descriptor)" % (page.byte_digest, digest))
        if writer.row_count != page.row_count:
            raise PageRejected("page_body_length_mismatch",
                               "decoded %d values for a %d-row page"
                               % (writer.row_count, page.row_count))
        writer.close()
        return Spool(path, page.start, page.end, writer.offsets)
    except PageRejected:
        if writer is not None:
            writer.abort()
        raise
    except EvidenceError as exc:
        if writer is not None:
            writer.abort()
        text = str(exc)
        reason = "page_truncated" if "truncated" in text or \
            "exceeds file size" in text else "io_error"
        raise PageRejected(reason, text)
    except DecodeError as exc:
        if writer is not None:
            writer.abort()
        raise PageRejected("page_decode_error", str(exc))
    except OSError as exc:
        if writer is not None:
            writer.abort()
        raise PageRejected("io_error", str(exc))
    finally:
        if fd is not None:
            os.close(fd)


# ---------------------------------------------------------------------------
# Dictionaries
# ---------------------------------------------------------------------------


def _parse_dictionary(reader, spill_path, index_path, identity):
    """Parse/verify a dictionary blob in one pass.

    The body must be exactly ``magic || u32 count || count canonical values``;
    the sha256 of the declared range must equal the descriptor's digest, and
    the declared byte length must be exactly the body's length.
    """
    magic = reader.read_exact(8)
    if magic != DICT_MAGIC:
        raise DecodeError("dictionary magic is %r, expected %r"
                          % (magic, DICT_MAGIC))
    count = int.from_bytes(reader.read_exact(4), "little")
    if count > reader.remaining():
        raise DecodeError("dictionary claims %d entries but only %d bytes "
                          "remain" % (count, reader.remaining()))
    entry_writer = SpoolWriter(spill_path, 0)
    index_fh = open(index_path, "wb")
    buf = bytearray()
    try:
        for i in range(count):
            if reader.remaining() < 1:
                raise DecodeError("dictionary truncated at entry %d of %d"
                                  % (i, count))
            entry_writer.begin_value()
            buf += entry_writer.tell().to_bytes(8, "little")
            if len(buf) >= 65536:
                index_fh.write(buf)
                buf = bytearray()
            tag = reader.read_exact(1)[0]
            if tag == 0:
                entry_writer.write(b"\x00")
            elif tag == 1:
                if reader.remaining() < 4:
                    raise DecodeError("dictionary entry %d: truncated length" % i)
                length = int.from_bytes(reader.read_exact(4), "little")
                if length > reader.remaining():
                    raise DecodeError("dictionary entry %d: string length %d "
                                      "exceeds remaining bytes" % (i, length))
                entry_writer.write(b"\x01" + length.to_bytes(4, "little"))
                checker = Utf8Checker()
                reader.copy_exact(length, entry_writer.write, checker.feed)
                checker.finish()
            elif tag == 2:
                if reader.remaining() < 8:
                    raise DecodeError("dictionary entry %d: truncated integer" % i)
                entry_writer.write(b"\x02" + reader.read_exact(8))
            else:
                raise DecodeError("dictionary entry %d: unknown tag 0x%02x"
                                  % (i, tag))
        if buf:
            index_fh.write(buf)
        index_fh.close()
        index_fh = None
        if reader.remaining() != 0:
            raise DecodeError("dictionary has %d extra bytes after %d entries "
                              "(declared length or entry count is wrong)"
                              % (reader.remaining(), count))
        digest = reader.finish()
        if digest != identity[0]:
            raise DecodeError("dictionary digest mismatch: declared %s, "
                              "computed %s" % (identity[0], digest))
        entry_writer.close()
        return DictSpill(identity[0], identity[1], count, spill_path,
                         index_path)
    except BaseException:
        entry_writer.abort()
        if index_fh is not None:
            try:
                index_fh.close()
            except OSError:
                pass
        try:
            os.unlink(index_path)
        except OSError:
            pass
        raise


class DictionaryUnavailable(PageRejected):
    def __init__(self, detail):
        PageRejected.__init__(self, "dictionary_unavailable", detail)


class DictRepository(object):
    """Content-addressed dictionary cache, fed by candidates from any replica."""

    def __init__(self, pool, workdir, anomaly_sink):
        self.pool = pool
        self.workdir = workdir
        self.anomalies = anomaly_sink
        self._candidates = {}
        self._loaded = {}
        self._failed = {}
        self._seq = 0

    def add_candidate(self, identity, replica_id, dict_ref, directory, page):
        key = (replica_id, dict_ref.location["file"], dict_ref.location["offset"])
        cands = self._candidates.setdefault(identity, [])
        for existing in cands:
            if existing[0] == key:
                return
        cands.append((key, replica_id, dict_ref, directory, page))

    def resolve(self, identity):
        if identity in self._loaded:
            return self._loaded[identity]
        if identity in self._failed:
            raise DictionaryUnavailable(self._failed[identity])
        cands = self._candidates.get(identity)
        if not cands:
            msg = ("no replica descriptor declares dictionary digest %s "
                   "(length %d)" % (identity[0], identity[1]))
            self._failed[identity] = msg
            raise DictionaryUnavailable(msg)
        ordered = sorted(cands, key=lambda c: (c[1], c[2].location["file"],
                                               c[2].location["offset"]))
        failures = []
        for _key, replica_id, dref, directory, page in ordered:
            self._seq += 1
            spill_path = os.path.join(self.workdir, "dict%d.spill" % self._seq)
            index_path = os.path.join(self.workdir, "dict%d.idx" % self._seq)
            fd = None
            try:
                fd, abs_off = open_evidence(directory, dref.location,
                                            dref.byte_length)
                reader = RangeReader(fd, abs_off, dref.byte_length)
                spill = _parse_dictionary(reader, spill_path, index_path,
                                          identity)
                spill.pool = self.pool
                provenance = {
                    "replica_id": replica_id,
                    "page": page.page,
                    "dict_id": dref.dict_id,
                    "dict_digest": identity[0],
                    "dict_byte_length": identity[1],
                    "location": dict(dref.location),
                }
                self._loaded[identity] = (spill, provenance)
                return spill, provenance
            except (EvidenceError, DecodeError, OSError) as exc:
                failures.append((replica_id, str(exc)))
                if self.anomalies is not None:
                    self.anomalies.add({
                        "kind": "dictionary_rejected",
                        "replica_id": replica_id,
                        "page": page.page,
                        "dict_id": dref.dict_id,
                        "dict_digest": identity[0],
                        "dict_byte_length": identity[1],
                        "location": dict(dref.location),
                        "reason": _classify_dict(exc),
                        "detail": str(exc),
                    })
            finally:
                if fd is not None:
                    os.close(fd)
                for p in (spill_path, index_path):
                    if os.path.exists(p) and (identity not in self._loaded):
                        try:
                            os.unlink(p)
                        except OSError:
                            pass
        msg = ("no replica supplied a verified copy of dictionary %s "
               "(%d candidate(s) rejected)" % (identity[0], len(failures)))
        self._failed[identity] = msg
        raise DictionaryUnavailable(msg)


def _classify_dict(exc):
    text = str(exc)
    if "escapes" in text or "not a regular file" in text or "cannot " in text:
        return "dictionary_io_error"
    if "truncated" in text or "short read" in text or "exceeds file size" in text:
        return "dictionary_truncated"
    if "digest mismatch" in text:
        return "dictionary_digest_mismatch"
    if "magic" in text:
        return "dictionary_bad_magic"
    if "extra bytes" in text or "claims" in text or "length" in text:
        return "dictionary_length_mismatch"
    return "dictionary_decode_error"
