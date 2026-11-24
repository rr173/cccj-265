"""Format parsing and canonical value encoding.

The wire format is JSON, but the contract is about bytes, not Python objects:
every digest is defined over an explicit little-endian, length-prefixed
canonical encoding, so any tool can recompute it.

See FORMAT.md for the normative specification.  Summary
--------------------------------------------------------

Canonical value encoding (self-delimiting, injective)::

    null        -> 0x00
    string s    -> 0x01 || u32le(len(utf8(s))) || utf8(s)
    integer n   -> 0x02 || i64le(n)

Strings are Unicode scalar sequences encoded as strict UTF-8 (no surrogates);
integers are signed 64-bit.  Thus "" (0x01 00000000), null (0x00) and the
string "1" (0x01 01000000 31) are all distinct, as are string "1" and
integer 1 (0x02 0100000000000000).

Canonical text form (used in reports and the restored archive): JSON scalar
expression -- ``json.dumps(s, ensure_ascii=False)`` for strings, the decimal
form for integers, ``null`` for null.

Slot digest (column c, half-open row range [start, end))::

    H = sha256()
    H.update(b"archrestore/slot/v1\0")
    for part in (c.name, c.type): H.update(u32le(len(utf8(part))) || utf8(part))
    H.update(i64le(start) || i64le(end))
    for r in [start, end): H.update(canonical_value(v_r))
    digest = lowercase hex(H.digest())

Dictionary blob (a sequence of canonical values addressed by u32 index)::

    b"ARCHDICT" || u32le(count) || count * canonical_value
    sha256 over the whole blob is the dictionary content digest.

Page body encodings::

    plain: concatenation of one canonical_value per row.
    dict:  row_count * u32le(code), code indexes the dictionary entry list.

The format is deliberately closed: unknown fields, duplicate JSON keys,
non-finite JSON constants and wrong types are rejected, never ignored.
"""

import hashlib
import json
import re

from .errors import (ArchError, ManifestError, ReplicaError, DecodeError)

TYPE_STRING = "string"
TYPE_INTEGER = "integer"
ALL_TYPES = (TYPE_STRING, TYPE_INTEGER)

FORMAT_NAME = "archrestore"
FORMAT_VERSION = 1

MANIFEST_KIND = "archrestore-manifest"
REPLICA_KIND = "archrestore-replica"

DICT_MAGIC = b"ARCHDICT"
SLOT_DOMAIN = b"archrestore/slot/v1\0"
PAGE_ENCODINGS = ("plain", "dict")

MAX_I64 = (1 << 63) - 1
MIN_I64 = -(1 << 63)

_HEX64 = re.compile(r"\A[0-9a-f]{64}\Z")


# ---------------------------------------------------------------------------
# Canonical values
# ---------------------------------------------------------------------------


def encode_value(value, ctype):
    """Encode one logical value as canonical bytes.

    Raises TypeError if ``value`` does not belong to ``ctype``; this is the
    check that keeps string "1" and integer 1 apart.
    """
    if ctype not in ALL_TYPES:
        raise TypeError("unknown column type %r" % (ctype,))
    if value is None:
        return b"\x00"
    if isinstance(value, bool):
        raise TypeError("boolean is not a supported value type")
    if ctype == TYPE_STRING:
        if not isinstance(value, str):
            raise TypeError("string column got %s" % type(value).__name__)
        try:
            raw = value.encode("utf-8")  # strict: rejects surrogates
        except UnicodeEncodeError as exc:
            raise TypeError("string is not encodable as UTF-8: %s" % exc)
        if len(raw) > 0xFFFFFFFF:
            raise TypeError("string too long for canonical u32 length")
        return b"\x01" + len(raw).to_bytes(4, "little") + raw
    if ctype == TYPE_INTEGER:
        if not isinstance(value, int):
            raise TypeError("integer column got %s" % type(value).__name__)
        if not (MIN_I64 <= value <= MAX_I64):
            raise TypeError("integer %d outside signed 64-bit range" % value)
        return b"\x02" + value.to_bytes(8, "little", signed=True)
    raise AssertionError("unreachable")


def canonical_text(value, ctype):
    """Canonical text form of a logical value (see module docstring)."""
    if value is None:
        return "null"
    if ctype == TYPE_STRING:
        if not isinstance(value, str):
            raise TypeError("string column got %s" % type(value).__name__)
        return json.dumps(value, ensure_ascii=False)
    if ctype == TYPE_INTEGER:
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError("integer column got %s" % type(value).__name__)
        return str(value)
    raise TypeError("unknown column type %r" % (ctype,))


def decode_value_into(raw, ctype):
    """Decode canonical bytes to a logical value, checking the column type.

    Used by the archive verifier so that a value of the wrong type in a
    restored file cannot silently pass.
    """
    if not isinstance(raw, (bytes, bytearray)) or not raw:
        raise DecodeError("empty canonical value")
    tag = raw[0]
    if tag == 0:
        if len(raw) != 1:
            raise DecodeError("trailing bytes after null")
        return None
    if tag == 1:
        if len(raw) < 5:
            raise DecodeError("truncated string header")
        n = int.from_bytes(raw[1:5], "little")
        if n != len(raw) - 5:
            raise DecodeError("string length mismatch")
        try:
            return raw[5:].decode("utf-8")
        except UnicodeDecodeError:
            raise DecodeError("invalid UTF-8 in string")
    if tag == 2:
        if len(raw) != 9:
            raise DecodeError("bad integer width")
        return int.from_bytes(raw[1:9], "little", signed=True)
    raise DecodeError("unknown canonical tag 0x%02x" % tag)


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


class SlotHasher(object):
    """Incremental slot digester; memory is O(1), independent of slot size."""

    __slots__ = ("_h", "_type", "start", "end", "column", "fed_rows")

    def __init__(self, column, ctype, start, end):
        h = hashlib.sha256()
        h.update(SLOT_DOMAIN)
        for part in (column, ctype):
            raw = part.encode("utf-8")
            h.update(len(raw).to_bytes(4, "little"))
            h.update(raw)
        h.update(start.to_bytes(8, "little", signed=True))
        h.update(end.to_bytes(8, "little", signed=True))
        self._h = h
        self._type = ctype
        self.start = start
        self.end = end
        self.column = column
        self.fed_rows = 0

    def update(self, value):
        self._h.update(encode_value(value, self._type))
        self.fed_rows += 1

    def update_raw(self, raw):
        """Feed already-canonical bytes (validated to match the column type)."""
        self._h.update(raw)
        self.fed_rows += 1

    def update_stream(self, header, chunks):
        """Feed one value as header + streamed body chunks.

        Memory stays O(chunk); a single value may be far larger than RAM.
        """
        self._h.update(header)
        for chunk in chunks:
            self._h.update(chunk)
        self.fed_rows += 1

    def hexdigest(self):
        return self._h.hexdigest()


def compute_slot_digest(column, ctype, start, end, values):
    """Reference helper (tests/synthesis only; materialises nothing itself)."""
    h = SlotHasher(column, ctype, start, end)
    for v in values:
        h.update(v)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def is_int(x):
    return isinstance(x, int) and not isinstance(x, bool)


def check_int(v, lo, hi, cls, where):
    if not is_int(v):
        raise cls("%s must be an integer" % where)
    if v < lo or v > hi:
        raise cls("%s out of range [%d, %d]: %d" % (where, lo, hi, v))
    return v


def check_str(v, cls, where, allow_empty=True):
    if not isinstance(v, str):
        raise cls("%s must be a string" % where)
    if not allow_empty and v == "":
        raise cls("%s must not be empty" % where)
    try:
        v.encode("utf-8")
    except UnicodeEncodeError:
        raise cls("%s is not valid Unicode (surrogate?)" % where)
    return v


def check_hex64(v, cls, where):
    if not isinstance(v, str) or not _HEX64.match(v):
        raise cls("%s must be exactly 64 lowercase hex characters" % where)
    return v


# ---------------------------------------------------------------------------
# Strict JSON loading
# ---------------------------------------------------------------------------


def _reject_constant(name):
    raise ValueError("non-finite JSON constant %r is not allowed" % (name,))


def _reject_dup_pairs(pairs):
    out = {}
    for k, v in pairs:
        if k in out:
            raise ValueError("duplicate JSON object key %r" % (k,))
        out[k] = v
    return out


def loads_strict(text):
    """json.loads with duplicate-key rejection and no NaN/Infinity."""
    return json.loads(text, object_pairs_hook=_reject_dup_pairs,
                      parse_constant=_reject_constant)


def load_json(path):
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except OSError as exc:
        raise ArchError("%s: cannot read: %s" % (path, exc))
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ArchError("%s: not valid UTF-8: %s" % (path, exc))
    try:
        return loads_strict(text)
    except ValueError as exc:
        raise ArchError("%s: not valid JSON: %s" % (path, exc))


def _header(obj, expected_kind, where, cls):
    if not isinstance(obj, dict):
        raise cls("%s: top level must be a JSON object" % where)
    hdr = obj.get("archrestore_format")
    if not isinstance(hdr, dict):
        raise cls("%s: missing archrestore_format header" % where)
    if set(hdr) != {"name", "version"}:
        raise cls("%s: header must have exactly name and version" % where)
    if hdr.get("name") != expected_kind:
        raise cls("%s: header name must be %r" % (where, expected_kind))
    if not is_int(hdr.get("version")) or hdr.get("version") != FORMAT_VERSION:
        raise cls("%s: unsupported format version %r"
                  % (where, hdr.get("version")))


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


class Column(object):
    __slots__ = ("name", "type", "index")

    def __init__(self, name, ctype, index):
        self.name = name
        self.type = ctype
        self.index = index

    def __repr__(self):
        return "Column(%r, %r)" % (self.name, self.type)


class Slot(object):
    __slots__ = ("slot", "column", "start", "end", "digest")

    def __init__(self, slot, column, start, end, digest):
        self.slot = slot
        self.column = column
        self.start = start
        self.end = end
        self.digest = digest

    @property
    def row_count(self):
        return self.end - self.start

    def __repr__(self):
        return "Slot(%r#%d, [%d,%d))" % (self.column, self.slot, self.start, self.end)


class Manifest(object):
    __slots__ = ("columns", "row_count", "slots", "slots_by_column", "path")

    def __init__(self, columns, row_count, slots, slots_by_column, path=None):
        self.columns = columns
        self.row_count = row_count
        self.slots = slots
        self.slots_by_column = slots_by_column
        self.path = path


def parse_manifest(obj, path=None):
    """Validate and return a :class:`Manifest`.

    The trusted handover document must tile ``[0, row_count)`` exactly, and
    independently for every column: no gaps, no overlaps, no duplicate slot
    ids, no slots outside the dataset.  Such defects are *bad manifest* --
    they are not missing data and can never be repaired from replicas.
    """
    where = "manifest[%s]" % (path,) if path else "manifest"
    _header(obj, MANIFEST_KIND, where, ManifestError)
    allowed = {"archrestore_format", "row_count", "columns", "slots", "note"}
    unknown = set(obj) - allowed
    if unknown:
        raise ManifestError("%s: unknown top-level fields %r" % (where, sorted(unknown)))
    if "note" in obj:
        check_str(obj["note"], ManifestError, where + ".note")

    cols_obj = obj.get("columns")
    if not isinstance(cols_obj, list) or not cols_obj:
        raise ManifestError("%s: columns must be a non-empty list" % where)
    columns = []
    by_name = {}
    for i, c in enumerate(cols_obj):
        w = "%s.columns[%d]" % (where, i)
        if not isinstance(c, dict):
            raise ManifestError("%s must be an object" % w)
        if set(c) != {"name", "type"}:
            raise ManifestError("%s must have exactly name and type" % w)
        name = check_str(c.get("name"), ManifestError, w + ".name", allow_empty=False)
        ctype = c.get("type")
        if ctype not in ALL_TYPES:
            raise ManifestError("%s.type must be one of %r" % (w, list(ALL_TYPES)))
        if name in by_name:
            raise ManifestError("%s: duplicate column name %r" % (where, name))
        col = Column(name, ctype, i)
        by_name[name] = col
        columns.append(col)

    row_count = check_int(obj.get("row_count"), 0, MAX_I64, ManifestError,
                          where + ".row_count")
    slots_obj = obj.get("slots")
    if not isinstance(slots_obj, list):
        raise ManifestError("%s.slots must be a list" % where)
    slots = []
    by_column = {c.name: [] for c in columns}
    seen_ids = set()
    for i, s in enumerate(slots_obj):
        w = "%s.slots[%d]" % (where, i)
        if not isinstance(s, dict):
            raise ManifestError("%s must be an object" % w)
        if set(s) != {"column", "slot", "start", "end", "digest"}:
            raise ManifestError("%s must have exactly column, slot, start, end, "
                                "digest" % w)
        cname = check_str(s.get("column"), ManifestError, w + ".column",
                          allow_empty=False)
        if cname not in by_name:
            raise ManifestError("%s.column %r is not in the manifest" % (w, cname))
        sid = check_int(s.get("slot"), 0, MAX_I64, ManifestError, w + ".slot")
        if (cname, sid) in seen_ids:
            raise ManifestError("%s: duplicate slot id %r for column %r"
                                % (w, sid, cname))
        seen_ids.add((cname, sid))
        start = check_int(s.get("start"), 0, row_count, ManifestError,
                          w + ".start")
        end = check_int(s.get("end"), 0, row_count, ManifestError, w + ".end")
        if end <= start:
            raise ManifestError("%s: end (%d) must be greater than start (%d)"
                                % (w, end, start))
        digest = check_hex64(s.get("digest"), ManifestError, w + ".digest")
        slot = Slot(sid, cname, start, end, digest)
        slots.append(slot)
        by_column[cname].append(slot)

    if row_count == 0:
        if slots:
            raise ManifestError("%s: zero-row dataset must have no slots" % where)
    else:
        if not slots:
            raise ManifestError("%s: non-empty dataset has no slots" % where)
        for c in columns:
            cslots = sorted(by_column[c.name], key=lambda x: x.start)
            cursor = 0
            for s in cslots:
                if s.start < cursor:
                    raise ManifestError(
                        "%s: column %r slot %d overlaps coverage at row %d "
                        "(overlapping slots are a bad manifest, not a data gap)"
                        % (where, c.name, s.slot, s.start))
                if s.start > cursor:
                    raise ManifestError(
                        "%s: column %r has a manifest gap at rows [%d, %d); "
                        "bad manifest, not missing data"
                        % (where, c.name, cursor, s.start))
                cursor = s.end
            if cursor != row_count:
                raise ManifestError(
                    "%s: column %r slots cover [0, %d), row_count is %d"
                    % (where, c.name, cursor, row_count))
            by_column[c.name] = cslots
    return Manifest(columns, row_count, slots, by_column, path)


# ---------------------------------------------------------------------------
# Replica descriptors
# ---------------------------------------------------------------------------

PAGE_FIELDS_PLAIN = {"page", "column", "start", "end", "encoding",
                     "byte_length", "byte_digest", "location"}
PAGE_FIELDS_DICT = PAGE_FIELDS_PLAIN | {"dict_id", "dict_digest",
                                        "dict_byte_length", "dict_location"}


class DictRef(object):
    __slots__ = ("dict_id", "digest", "byte_length", "location")

    def __init__(self, dict_id, digest, byte_length, location):
        self.dict_id = dict_id
        self.digest = digest
        self.byte_length = byte_length
        self.location = location


class PageDesc(object):
    __slots__ = ("page", "column", "start", "end", "encoding", "byte_length",
                 "byte_digest", "location", "dict_ref", "replica_id",
                 "column_type")

    def __init__(self, page, column, start, end, encoding, byte_length,
                 byte_digest, location, dict_ref):
        self.page = page
        self.column = column
        self.start = start
        self.end = end
        self.encoding = encoding
        self.byte_length = byte_length
        self.byte_digest = byte_digest
        self.location = location
        self.dict_ref = dict_ref
        self.replica_id = None
        self.column_type = None

    @property
    def row_count(self):
        return self.end - self.start

    def __repr__(self):
        return "Page(%r#%r, %r, [%d,%d), %s)" % (
            self.replica_id, self.page, self.column, self.start, self.end,
            self.encoding)


class Replica(object):
    __slots__ = ("replica_id", "pages", "path", "directory", "note")

    def __init__(self, replica_id, pages, path, directory, note=None):
        self.replica_id = replica_id
        self.pages = pages
        self.path = path
        self.directory = directory
        self.note = note
        for p in pages:
            p.replica_id = replica_id


def _parse_location(loc, cls, where):
    """Validate a required page/dict location.  Returns a normalised dict."""
    if not isinstance(loc, dict):
        raise cls("%s must be an object" % where)
    if "file" not in loc or "offset" not in loc:
        raise cls("%s must contain file and offset" % where)
    f = check_str(loc["file"], cls, where + ".file", allow_empty=False)
    if f.startswith("/") or f.startswith("\\") or "\x00" in f:
        raise cls("%s.file must be a relative path inside the replica directory"
                  % where)
    # Reject path traversal lexically; symlink escapes are rejected later by
    # resolving against the replica directory and checking containment.
    parts = re.split(r"[/\\]+", f)
    if any(p in ("..", "") for p in parts):
        raise cls("%s.file must not contain empty or '..' components" % where)
    off = check_int(loc["offset"], 0, (1 << 62), cls, where + ".offset")
    out = dict(loc)
    out["file"] = f
    out["offset"] = off
    return out


def parse_replica(obj, manifest, path=None, directory=None):
    """Validate a replica descriptor.

    Returns ``(replica, page_errors)``.  A replica-level structural problem
    raises :class:`ReplicaError` and the caller skips the whole replica; a
    single bad page is reported in *page_errors* and the rest of the replica
    remains usable evidence, as required for unreliable replicas.
    """
    where = "replica[%s]" % (path,) if path else "replica"
    _header(obj, REPLICA_KIND, where, ReplicaError)
    allowed = {"archrestore_format", "replica_id", "pages", "note"}
    unknown = set(obj) - allowed
    if unknown:
        raise ReplicaError("%s: unknown top-level fields %r"
                           % (where, sorted(unknown)))
    rid = check_str(obj.get("replica_id"), ReplicaError, where + ".replica_id",
                    allow_empty=False)
    note = obj.get("note")
    if note is not None:
        check_str(note, ReplicaError, where + ".note")
    pages_obj = obj.get("pages")
    if not isinstance(pages_obj, list):
        raise ReplicaError("%s.pages must be a list" % where)

    col_by_name = {c.name: c for c in manifest.columns}
    pages = []
    errors = []
    seen_ids = {}
    for i, p in enumerate(pages_obj):
        w = "%s.pages[%d]" % (where, i)
        try:
            if not isinstance(p, dict):
                raise ReplicaError("%s must be an object" % w)
            enc = p.get("encoding")
            if enc not in PAGE_ENCODINGS:
                raise ReplicaError("%s.encoding must be one of %r"
                                   % (w, list(PAGE_ENCODINGS)))
            allowed_fields = PAGE_FIELDS_DICT if enc == "dict" else PAGE_FIELDS_PLAIN
            missing = allowed_fields - set(p)
            extra = set(p) - allowed_fields
            if missing:
                raise ReplicaError("%s missing fields %r" % (w, sorted(missing)))
            if extra:
                raise ReplicaError("%s has fields not valid for encoding %r: %r"
                                   % (w, enc, sorted(extra)))
            pid = check_int(p.get("page"), 0, MAX_I64, ReplicaError, w + ".page")
            if pid in seen_ids:
                raise ReplicaError("%s: duplicate page id %d" % (w, pid))
            seen_ids[pid] = i
            cname = check_str(p.get("column"), ReplicaError, w + ".column",
                              allow_empty=False)
            if cname not in col_by_name:
                raise ReplicaError("%s.column %r is not in the manifest"
                                   % (w, cname))
            start = check_int(p.get("start"), 0, manifest.row_count,
                              ReplicaError, w + ".start")
            end = check_int(p.get("end"), start, manifest.row_count,
                            ReplicaError, w + ".end")
            if end == start:
                raise ReplicaError("%s covers no rows; omit it instead" % w)
            blen = check_int(p.get("byte_length"), 0, (1 << 62), ReplicaError,
                             w + ".byte_length")
            bdig = check_hex64(p.get("byte_digest"), ReplicaError,
                               w + ".byte_digest")
            loc = _parse_location(p.get("location"), ReplicaError,
                                  w + ".location")
            dref = None
            if enc == "dict":
                did = check_str(p.get("dict_id"), ReplicaError, w + ".dict_id",
                                allow_empty=False)
                ddig = check_hex64(p.get("dict_digest"), ReplicaError,
                                   w + ".dict_digest")
                dlen = check_int(p.get("dict_byte_length"), 1, (1 << 62),
                                 ReplicaError, w + ".dict_byte_length")
                dloc = _parse_location(p.get("dict_location"), ReplicaError,
                                       w + ".dict_location")
                dref = DictRef(did, ddig, dlen, dloc)
            page = PageDesc(pid, cname, start, end, enc, blen, bdig, loc, dref)
            page.replica_id = rid
            page.column_type = col_by_name[cname].type
            pages.append(page)
        except ReplicaError as exc:
            errors.append({"page_index": i,
                           "page": p.get("page") if isinstance(p, dict) else None,
                           "column": p.get("column") if isinstance(p, dict) else None,
                           "reason": "malformed_descriptor",
                           "detail": str(exc)})
    return Replica(rid, pages, path, directory, note), errors
