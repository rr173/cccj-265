"""Deliverable archive: a self-contained, row-addressable restored dataset.

Layout::

    <archive>/
      archive.json              # manifest copy + column/slot descriptors
      columns/c000.canon        # canonical value stream, row order
      columns/c000.idx          # u64le offsets, one per ARCHIVE_BLOCK_ROWS
      checks/index.json         # sub-interval -> original page/dict provenance

The ``.canon`` stream uses exactly the canonical value encoding of
FORMAT.md, so the archive can be re-verified without the manifest, without
the replicas and without this tool's decoder.  ``archive.json`` carries each
column's own slot digests (copied from the trusted manifest), the stream's
length and sha256, and the row count; ``verify_archive`` recomputes
everything from the files alone.

Only a fully confirmed dataset is ever written as an archive.  A partial
result is never labelled "restored": the caller writes diagnostics instead.

Random access by original row number is O(1) blocks: seek the block offset in
the ``.idx`` file, then skip at most ARCHIVE_BLOCK_ROWS values.  No column and
no slot is loaded whole, and individual values are streamed.
"""

import hashlib
import json
import os
import stat
import sys

from .errors import ArchError, DecodeError, EvidenceError
from .format import (
    ALL_TYPES,
    FORMAT_VERSION,
    TYPE_INTEGER,
    TYPE_STRING,
    SlotHasher,
)
from .spill import BLOCK_ROWS, FilePool, Spool

ARCHIVE_FORMAT_NAME = "archrestore-archive"
ARCHIVE_STATUS = "restored"


def _safe_filename(name, index):
    # Column names are user data.  Archive filenames are derived only from the
    # column *index*, never from the name, so no name can escape the directory.
    return "c%03d" % index


def _write_u64_index(path, offsets):
    import array
    arr = array.array("Q", offsets)
    if sys.byteorder != "little":
        arr.byteswap()
    with open(path, "wb") as fh:
        fh.write(arr.tobytes())


def read_u64_index(path):
    import array
    with open(path, "rb") as fh:
        data = fh.read()
    if len(data) % 8:
        raise ArchError("%s: index length %d is not a multiple of 8"
                        % (path, len(data)))
    arr = array.array("Q")
    arr.frombytes(data)
    if sys.byteorder != "little":
        arr.byteswap()
    return arr


def write_archive(out_dir, manifest, manifest_sha256, column_results,
                  dict_provenance, report_meta):
    """Materialise a fully confirmed dataset.

    ``column_results`` maps column name -> ColumnResolution whose
    ``unique_spill`` holds exactly one canonical value per row of that column
    (in row order).  Raises ArchError unless every row of every column is
    confirmed and every slot digest matched.
    """
    for c in manifest.columns:
        res = column_results.get(c.name)
        if res is None or not res.stream_complete:
            raise ArchError("refusing to write archive: column %r is not fully "
                            "confirmed" % (c.name,))
        for slot in res.slots:
            if slot.status != "confirmed":
                raise ArchError("refusing to write archive: column %r slot %d "
                                "is %s" % (c.name, slot.slot.slot, slot.status))

    if os.path.exists(out_dir) and os.listdir(out_dir):
        raise ArchError("output directory %r exists and is not empty" % (out_dir,))
    os.makedirs(out_dir, exist_ok=True)
    col_dir = os.path.join(out_dir, "columns")
    checks_dir = os.path.join(out_dir, "checks")
    os.makedirs(col_dir, exist_ok=True)
    os.makedirs(checks_dir, exist_ok=True)

    columns_meta = []
    for c in manifest.columns:
        res = column_results[c.name]
        fname = _safe_filename(c.name, c.index) + ".canon"
        iname = _safe_filename(c.name, c.index) + ".idx"
        cpath = os.path.join(col_dir, fname)
        ipath = os.path.join(col_dir, iname)
        h = hashlib.sha256()
        length = 0
        # Copy exactly once; unique spill contains no duplicate rows even when
        # replicas overlap.
        with open(res.unique_path, "rb") as src, open(cpath, "wb") as dst:
            while True:
                chunk = src.read(1 << 20)
                if not chunk:
                    break
                dst.write(chunk)
                h.update(chunk)
                length += len(chunk)
        offsets = res.unique_spill.offsets
        if len(offsets) != (manifest.row_count + BLOCK_ROWS - 1) // BLOCK_ROWS:
            raise ArchError("column %r: block index has %d entries, expected %d"
                            % (c.name, len(offsets),
                               (manifest.row_count + BLOCK_ROWS - 1) // BLOCK_ROWS))
        _write_u64_index(ipath, offsets)
        slot_meta = []
        for d in res.slots:
            slot_meta.append({
                "slot": d.slot.slot,
                "start": d.start,
                "end": d.end,
                "digest": d.expected_digest,
            })
        columns_meta.append({
            "name": c.name,
            "type": c.type,
            "file": "columns/" + fname,
            "index_file": "columns/" + iname,
            "block_rows": BLOCK_ROWS,
            "value_count": manifest.row_count,
            "byte_length": length,
            "sha256": h.hexdigest(),
            "slots": slot_meta,
        })

    check_index = build_check_index(manifest, column_results, dict_provenance)
    cpath = os.path.join(checks_dir, "index.json")
    with open(cpath, "w", encoding="utf-8") as fh:
        json.dump(check_index, fh, ensure_ascii=False, sort_keys=True,
                  indent=1)
        fh.write("\n")

    archive = {
        "archrestore_format": {"name": ARCHIVE_FORMAT_NAME,
                               "version": FORMAT_VERSION},
        "status": ARCHIVE_STATUS,
        "row_count": manifest.row_count,
        "manifest_sha256": manifest_sha256,
        "source": report_meta,
        "columns": columns_meta,
        "checks": "checks/index.json",
    }
    apath = os.path.join(out_dir, "archive.json")
    with open(apath, "w", encoding="utf-8") as fh:
        json.dump(archive, fh, ensure_ascii=False, sort_keys=True, indent=1)
        fh.write("\n")
    return archive


def build_check_index(manifest, column_results, dict_provenance):
    """Sub-interval -> original page / dictionary provenance."""
    cols = []
    for c in manifest.columns:
        res = column_results[c.name]
        slots = []
        for d in res.slots:
            slots.append({
                "slot": d.slot.slot,
                "start": d.start,
                "end": d.end,
                "expected_digest": d.expected_digest,
                "computed_digest": d.computed_digest,
                "status": d.status,
                "evidence": d.evidence,
                "witnesses": d.witnesses,
                "dict_digests": d.dict_digests,
            })
        cols.append({
            "column": c.name,
            "type": c.type,
            "archive_file": "columns/%s.canon"
                            % _safe_filename(c.name, c.index),
            "row_range": {"start": 0, "end": manifest.row_count},
            "slots": slots,
        })
    dicts = []
    for (digest, blen), prov in sorted(dict_provenance.items()):
        entry = dict(prov)
        entry["dict_digest"] = digest
        entry["dict_byte_length"] = blen
        dicts.append(entry)
    return {
        "archrestore_format": {"name": "archrestore-check-index",
                               "version": FORMAT_VERSION},
        "row_count": manifest.row_count,
        "columns": cols,
        "dictionaries": dicts,
        "note": "Every archive row number r in a slot's [start,end) is backed "
                "by the listed evidence sub-intervals; replicas that merely "
                "overlap are witnesses, not extra rows.",
    }


# ---------------------------------------------------------------------------
# Independent verification
# ---------------------------------------------------------------------------


class VerifyResult(object):
    def __init__(self):
        self.ok = True
        self.problems = []
        self.columns_checked = 0
        self.slots_checked = 0
        self.rows_checked = 0

    def fail(self, msg):
        self.ok = False
        self.problems.append(msg)

    def to_json(self):
        return {
            "ok": self.ok,
            "problems": self.problems,
            "columns_checked": self.columns_checked,
            "slots_checked": self.slots_checked,
            "rows_checked": self.rows_checked,
        }


def _load_archive_header(out_dir):
    path = os.path.join(out_dir, "archive.json")
    try:
        with open(path, "rb") as fh:
            obj = json.loads(fh.read().decode("utf-8"))
    except (OSError, ValueError) as exc:
        raise ArchError("%s: cannot read archive header: %s" % (path, exc))
    try:
        hdr = obj["archrestore_format"]
        if hdr.get("name") != ARCHIVE_FORMAT_NAME:
            raise ArchError("%s: not an archrestore archive" % path)
        if hdr.get("version") != FORMAT_VERSION or isinstance(
                hdr.get("version"), bool):
            raise ArchError("%s: unsupported archive version" % path)
    except (KeyError, AttributeError, TypeError):
        raise ArchError("%s: malformed archive header" % path)
    return obj


def verify_archive(out_dir, problem_limit=100):
    """Recompute the whole archive from its own bytes.

    Checks: header sanity, per-column stream length and sha256, canonical
    decode of every value against the column type, exact row count, and every
    slot digest (recomputed from archive bytes alone).  Does not consult the
    manifest or any replica.
    """
    res = VerifyResult()
    try:
        archive = _load_archive_header(out_dir)
    except ArchError as exc:
        res.fail(str(exc))
        return res
    if archive.get("status") != ARCHIVE_STATUS:
        res.fail("archive status is %r, not %r"
                 % (archive.get("status"), ARCHIVE_STATUS))
    row_count = archive.get("row_count")
    if not isinstance(row_count, int) or isinstance(row_count, bool) or \
            row_count < 0:
        res.fail("archive row_count is invalid")
        return res
    pool = FilePool(max_open=16)
    try:
        for cmeta in archive.get("columns", []):
            _verify_column(out_dir, cmeta, row_count, pool, res, problem_limit)
    finally:
        pool.close()
    return res


def _verify_column(out_dir, cmeta, row_count, pool, res, problem_limit):
    try:
        name = cmeta["name"]
        ctype = cmeta["type"]
        rel = cmeta["file"]
        idx_rel = cmeta["index_file"]
        declared_len = cmeta["byte_length"]
        declared_sha = cmeta["sha256"]
        declared_count = cmeta["value_count"]
        block_rows = cmeta["block_rows"]
        slots = cmeta["slots"]
    except (KeyError, TypeError) as exc:
        res.fail("column metadata malformed: %r" % (exc,))
        return
    if ctype not in ALL_TYPES:
        res.fail("column %r has unknown type %r" % (name, ctype))
        return
    if declared_count != row_count:
        res.fail("column %r declares %r values, archive row_count is %d"
                 % (name, declared_count, row_count))
    cpath = os.path.realpath(os.path.join(out_dir, rel))
    ipath = os.path.realpath(os.path.join(out_dir, idx_rel))
    root = os.path.realpath(out_dir)
    for p, label in ((cpath, "stream"), (ipath, "index")):
        try:
            if os.path.commonpath([root, p]) != root:
                res.fail("column %r %s escapes archive directory" % (name, label))
                return
            if not stat.S_ISREG(os.stat(p).st_mode):
                res.fail("column %r %s is not a regular file" % (name, label))
                return
        except OSError as exc:
            res.fail("column %r %s missing: %s" % (name, label, exc))
            return
    try:
        offsets = read_u64_index(ipath)
    except ArchError as exc:
        res.fail(str(exc))
        return
    expected_blocks = (row_count + block_rows - 1) // block_rows
    if len(offsets) != expected_blocks:
        res.fail("column %r index has %d blocks, expected %d"
                 % (name, len(offsets), expected_blocks))
    if os.path.getsize(cpath) != declared_len:
        res.fail("column %r length is %d, header says %d"
                 % (name, os.path.getsize(cpath), declared_len))
    spool = Spool(cpath, 0, row_count, offsets)
    reader = spool.reader(pool)
    hashers = {}
    for s in slots:
        hashers[s["slot"]] = SlotHasher(name, ctype, s["start"], s["end"])
    sindex = sorted(slots, key=lambda s: s["start"])
    si = 0
    h = hashlib.sha256()
    filepos = 0
    for row in range(row_count):
        while si < len(sindex) and sindex[si]["end"] <= row:
            si += 1
        try:
            value = reader.next_value()
        except (EvidenceError, DecodeError) as exc:
            res.fail("column %r row %d: archive stream is unreadable: %s"
                     % (name, row, exc))
            return
        # Stream: a single value may be far larger than memory, so it is never
        # joined into one bytes object here.
        header = value.header()
        h.update(header)
        filepos += len(header)
        if value.tag == 1:
            # Validate the string by streaming it through a UTF-8 decoder and
            # recompute its canonical length; the length was already bounded
            # by the spool decoder.
            import codecs
            dec = codecs.getincrementaldecoder("utf-8")("strict")
            seen = 0
            bad_utf8 = False
            for chunk in value.iter_chunks(pool):
                h.update(chunk)
                seen += len(chunk)
                filepos += len(chunk)
                try:
                    dec.decode(chunk, False)
                except UnicodeDecodeError:
                    bad_utf8 = True
            try:
                dec.decode(b"", True)
            except UnicodeDecodeError:
                bad_utf8 = True
            if bad_utf8 or seen != value.length:
                if len(res.problems) < problem_limit:
                    res.fail("column %r row %d: invalid canonical string"
                             % (name, row))
        # For integers the canonical bytes are entirely inside header().
        if ctype == TYPE_STRING and value.tag == 2:
            if len(res.problems) < problem_limit:
                res.fail("column %r row %d: integer value in string column"
                         % (name, row))
        elif ctype == TYPE_INTEGER and value.tag == 1:
            if len(res.problems) < problem_limit:
                res.fail("column %r row %d: string value in integer column"
                         % (name, row))
        if si >= len(sindex):
            res.fail("column %r row %d falls outside all slots" % (name, row))
            break
        slot = sindex[si]
        if not (slot["start"] <= row < slot["end"]):
            res.fail("column %r row %d is not inside its next slot" % (name, row))
            break
        hasher = hashers[slot["slot"]]
        hasher._h.update(header)  # complete bytes for null and integer
        if value.tag == 1:
            for chunk in value.iter_chunks(pool):
                hasher._h.update(chunk)
        hasher.fed_rows += 1
        res.rows_checked += 1
    if filepos != declared_len:
        res.fail("column %r stream ends at %d bytes, header says %d"
                 % (name, filepos, declared_len))
    if h.hexdigest() != declared_sha:
        res.fail("column %r stream sha256 is %s, header says %s"
                 % (name, h.hexdigest(), declared_sha))
    for s in slots:
        computed = hashers[s["slot"]].hexdigest()
        res.slots_checked += 1
        if computed != s["digest"]:
            res.fail("column %r slot %r [%d,%d): digest %s, archive header "
                     "says %s" % (name, s["slot"], s["start"], s["end"],
                                  computed, s["digest"]))
        elif hashers[s["slot"]].fed_rows != s["end"] - s["start"]:
            res.fail("column %r slot %r: %d rows hashed, expected %d"
                     % (name, s["slot"], hashers[s["slot"]].fed_rows,
                        s["end"] - s["start"]))
    if reader.row != row_count:
        res.fail("column %r decoded %d rows, expected %d"
                 % (name, reader.row, row_count))
    res.columns_checked += 1


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


class ArchiveReader(object):
    """Random access to one archive column by original row number."""

    def __init__(self, out_dir, header=None):
        self.out_dir = out_dir
        self.header = header if header is not None else _load_archive_header(out_dir)
        self.pool = FilePool(max_open=8)
        self._spools = {}

    def column(self, name):
        for c in self.header["columns"]:
            if c["name"] == name:
                return c
        raise ArchError("archive has no column %r (have: %s)"
                        % (name, [c["name"] for c in self.header["columns"]]))

    def _spool(self, cmeta):
        key = cmeta["name"]
        if key not in self._spools:
            root = os.path.realpath(self.out_dir)
            cpath = os.path.realpath(os.path.join(self.out_dir, cmeta["file"]))
            ipath = os.path.realpath(os.path.join(self.out_dir,
                                                  cmeta["index_file"]))
            if os.path.commonpath([root, cpath]) != root or \
                    os.path.commonpath([root, ipath]) != root:
                raise ArchError("archive column path escapes archive directory")
            offsets = read_u64_index(ipath)
            self._spools[key] = Spool(cpath, 0, self.header["row_count"],
                                      offsets)
        return self._spools[key]

    def read_row_column(self, name, row):
        cmeta = self.column(name)
        if not (0 <= row < self.header["row_count"]):
            raise ArchError("row %d out of range [0, %d)"
                            % (row, self.header["row_count"]))
        reader = self._spool(cmeta).reader(self.pool)
        reader.seek_row(row)
        return reader.next_value()

    def column_cursor(self, name):
        """A forward :class:`SpoolReader` over one column, row 0 onwards.

        Additive streaming helper used by whole-archive scans (compare); the
        caller drives exactly ``header["row_count"]`` ``next_value()`` calls.
        Reuses the same block index and bounded pool as random access.
        """
        cmeta = self.column(name)
        return self._spool(cmeta).reader(self.pool)

    def close(self):
        self.pool.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def stream_text(raw, pool):
    """Yield the canonical text of a value in bounded chunks.

    Used for exports where a value may exceed memory; strings are decoded
    incrementally so a multibyte UTF-8 sequence split across file chunks is
    handled correctly.
    """
    if raw.tag == 0:
        yield "null"
        return
    if raw.tag == 2:
        yield str(int.from_bytes(raw.inline, "little", signed=True))
        return
    import codecs
    dec = codecs.getincrementaldecoder("utf-8")("strict")
    yield '"'
    for chunk in raw.iter_chunks(pool):
        text = dec.decode(chunk, False)
        if text:
            yield _escape_fragment(text)
    tail = dec.decode(b"", True)
    if tail:
        yield _escape_fragment(tail)
    yield '"'


def _escape_fragment(text):
    out = json.dumps(text, ensure_ascii=False)
    return out[1:-1]
