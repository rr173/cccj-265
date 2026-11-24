"""Out-of-core validation of a periodic delivery group (period package).

A periodic delivery bundles several independently produced archive members
(e.g. a device ledger and an inspection-point ledger, each restored on its own
through the normal restore flow).  Even when every member is individually
complete, the receiver still has to prove that, for the period, every
referencing record points at exactly one unique referenced record.

The caller supplies one small JSON **period descriptor** naming the members and
the relations between them::

    {
      "archrestore_format": {"name": "archrestore-period-package", "version": 1},
      "period_id": "2026-W48",
      "members": [
        {"name": "devices", "kind": "archive", "path": "devices-archive",
         "fields": [{"name": "device_id", "type": "integer"}]},
        {"name": "points", "kind": "jsonl", "path": "points.jsonl",
         "fields": [{"name": "device_ref", "type": "integer"}]}
      ],
      "relations": [
        {"name": "points->devices",
         "from": {"member": "points",  "fields": ["device_ref"]},
         "to":   {"member": "devices", "fields": ["device_id"]}}
      ]
    }

Rules (the whole group passes only if every rule holds):

* each relation pairs one or more fields on the referencing side with one or
  more fields on the referenced side; paired fields must have compatible
  (equal) declared types;
* a composite key is the concatenation of the fields' canonical encodings, so
  equality is exact type-and-value equality per field (string ``"1"`` never
  equals integer ``1``);
* referenced-side composite keys must be **non-null and unique**;
* every referencing record must match **exactly one** referenced record
  (a null or otherwise empty key, or a key with no match, fails);
* any malformed member record, duplicate referenced key or unmatched
  reference invalidates the period.

On full success a *period manifest* is written atomically.  It records a
content summary of every member, so a reviewer can independently re-check the
group without this tool.  On any failure no manifest is written and a stable,
fully-ordered diagnosis is returned (and optionally published as JSONL).

Memory boundary
---------------
Members are never loaded whole.  Every member is streamed through bounded
in-memory runs that are spilled and merged with the same cascading k-way
external merge used by :mod:`archrestore.compare`.  Duplicate detection is an
adjacent-key streaming scan and the referential check is a streaming two-way
merge join.  A member with more relations than one is scanned once per
relation; each scan still uses O(run budget) memory regardless of row count.
Diagnostics themselves are externally sorted, so a member with millions of
bad rows cannot grow memory either.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import os
import shutil
import struct
import sys
import tempfile

try:  # pragma: no cover - import-mode branching
    from .archive import ArchiveReader, verify_archive
    from .errors import ArchError, DecodeError, EvidenceError
    from .format import (
        FORMAT_VERSION,
        TYPE_INTEGER,
        TYPE_STRING,
        ALL_TYPES,
        check_str,
        is_int,
        loads_strict,
        load_json,
    )
except ImportError:  # pragma: no cover
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from archrestore.archive import ArchiveReader, verify_archive  # noqa: E402
    from archrestore.errors import ArchError, DecodeError, EvidenceError  # noqa: E402
    from archrestore.format import (  # noqa: E402
        FORMAT_VERSION,
        TYPE_INTEGER,
        TYPE_STRING,
        ALL_TYPES,
        check_str,
        is_int,
        loads_strict,
        load_json,
    )

PERIOD_KIND = "archrestore-period-package"
PERIOD_MANIFEST_KIND = "archrestore-period-manifest"

DEFAULT_RUN_RECORDS = 4096
DEFAULT_RUN_BYTES = 4 * 1024 * 1024
DEFAULT_FAN_IN = 32
DEFAULT_PROBLEM_LIMIT = 100

KIND_ARCHIVE = "archive"
KIND_JSONL = "jsonl"
_KIND_ALIASES = {
    KIND_ARCHIVE: KIND_ARCHIVE,
    "arch": KIND_ARCHIVE,
    "archive_dir": KIND_ARCHIVE,
    "archrestore-archive": KIND_ARCHIVE,
    KIND_JSONL: KIND_JSONL,
    "jsonlines": KIND_JSONL,
    "ndjson": KIND_JSONL,
    "lines": KIND_JSONL,
}

# Problem kinds (stable tokens used in both the JSON report and the JSONL
# problem report).
PROBLEM_MEMBER_ERROR = "member_error"
PROBLEM_INVALID_RECORD = "invalid_record"
PROBLEM_TYPE_MISMATCH = "field_type_mismatch"
PROBLEM_EMPTY_TARGET_KEY = "empty_target_key"
PROBLEM_DUPLICATE_TARGET_KEY = "duplicate_target_key"
PROBLEM_EMPTY_REFERENCE_KEY = "empty_reference_key"
PROBLEM_UNMATCHED_REFERENCE = "unmatched_reference"
PROBLEM_AMBIGUOUS_REFERENCE = "ambiguous_reference"

_JSONL_DIGEST_DOMAIN = b"archrestore/jsonl-content/v1\0"
_PERIOD_DIGEST_DOMAIN = b"archrestore/period-manifest/v1\0"


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class PeriodError(ArchError):
    """Base class for period-package failures."""

    exit_code = 2


class PeriodDescriptorError(PeriodError):
    """The period descriptor itself is missing or malformed.

    This is distinct from data-level validation failures: a bad descriptor
    aborts before any member is read and never produces a period manifest.
    """


class PeriodValidationError(PeriodError):
    """One or more members/relations failed validation.

    ``report`` is a JSON-serialisable diagnosis.  A period manifest is never
    published when this is raised.
    """

    exit_code = 3

    def __init__(self, message, report):
        super().__init__(message)
        self.report = report


# Neighbouring-vocabulary aliases.
PeriodPackageError = PeriodError
PeriodSpecError = PeriodDescriptorError
PeriodFailure = PeriodValidationError
GroupValidationError = PeriodValidationError


class _MemberUnreadable(PeriodError):
    """Internal signal: a member's bytes cannot be decoded during a scan.

    The driver converts this into a structured ``member_error`` diagnosis
    rather than letting a low-level read/decoder exception escape, so an
    unreadable member invalidates the period cleanly like any other failure.
    """

    def __init__(self, message, member_name):
        super().__init__(message)
        self.member_name = member_name


# ---------------------------------------------------------------------------
# Descriptor model
# ---------------------------------------------------------------------------


class FieldSpec(object):
    __slots__ = ("name", "ctype")

    def __init__(self, name, ctype):
        self.name = name
        self.ctype = ctype

    def to_json(self):
        return {"name": self.name, "type": self.ctype}


class MemberSpec(object):
    __slots__ = ("name", "kind", "path", "fields", "raw")

    def __init__(self, name, kind, path, fields, raw=None):
        self.name = name
        self.kind = kind
        self.path = path
        self.fields = fields  # list[FieldSpec]
        self.raw = raw or {}

    def field(self, name):
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def to_json(self):
        return {
            "name": self.name,
            "kind": self.kind,
            "path": self.path,
            "fields": [f.to_json() for f in self.fields],
        }


class RelationEndpoint(object):
    __slots__ = ("member_name", "fields")

    def __init__(self, member_name, fields):
        self.member_name = member_name
        self.fields = list(fields)


class RelationSpec(object):
    __slots__ = ("name", "from_endpoint", "to_endpoint", "pairs", "raw")

    def __init__(self, name, from_endpoint, to_endpoint, pairs, raw=None):
        self.name = name
        self.from_endpoint = from_endpoint
        self.to_endpoint = to_endpoint
        # pairs: list of (from_field, to_field, ctype)
        self.pairs = pairs
        self.raw = raw or {}

    def to_json(self):
        return {
            "name": self.name,
            "from": {"member": self.from_endpoint.member_name,
                     "fields": list(self.from_endpoint.fields)},
            "to": {"member": self.to_endpoint.member_name,
                   "fields": list(self.to_endpoint.fields)},
            "field_pairs": [
                {"from": a, "to": b, "type": t} for a, b, t in self.pairs
            ],
        }


class PeriodDescriptor(object):
    __slots__ = ("period_id", "members", "relations", "raw", "path",
                 "base_dir")

    def __init__(self, members, relations, period_id=None, raw=None,
                 path=None, base_dir=None):
        self.members = members
        self.relations = relations
        self.period_id = period_id
        self.raw = raw or {}
        self.path = path
        self.base_dir = base_dir

    def member(self, name):
        for m in self.members:
            if m.name == name:
                return m
        return None


def _require_object(obj, where):
    if not isinstance(obj, dict):
        raise PeriodDescriptorError("%s must be a JSON object" % where)


def _parse_field_list(obj, where):
    """Parse a member's declared field list into ordered FieldSpec objects."""
    if not isinstance(obj, list) or not obj:
        raise PeriodDescriptorError(
            "%s must be a non-empty list of field declarations" % where)
    fields = []
    seen = set()
    for i, item in enumerate(obj):
        w = "%s[%d]" % (where, i)
        if isinstance(item, str):
            raise PeriodDescriptorError(
                "%s: field entries must be {name, type} objects, got a bare "
                "string %r" % (w, item))
        _require_object(item, w)
        unknown = set(item) - {"name", "type"}
        if unknown:
            raise PeriodDescriptorError(
                "%s: unknown field keys %r" % (w, sorted(unknown)))
        name = item.get("name")
        ctype = item.get("type")
        check_str(name, PeriodDescriptorError, w + ".name", allow_empty=False)
        if ctype not in ALL_TYPES:
            raise PeriodDescriptorError(
                "%s.type must be one of %r" % (w, list(ALL_TYPES)))
        if name in seen:
            raise PeriodDescriptorError(
                "%s: duplicate field name %r" % (where, name))
        seen.add(name)
        fields.append(FieldSpec(name, ctype))
    return fields


def _parse_member(obj, index, where):
    w = "%s.members[%d]" % (where, index)
    _require_object(obj, w)
    allowed = {"name", "kind", "type", "path", "file", "location", "fields",
               "columns"}
    unknown = set(obj) - allowed
    if unknown:
        raise PeriodDescriptorError(
            "%s: unknown member keys %r" % (w, sorted(unknown)))
    name = obj.get("name")
    check_str(name, PeriodDescriptorError, w + ".name", allow_empty=False)
    kind_raw = obj.get("kind", obj.get("type"))
    if not isinstance(kind_raw, str) or kind_raw not in _KIND_ALIASES:
        raise PeriodDescriptorError(
            "%s.kind must be one of %r"
            % (w, sorted(set(_KIND_ALIASES))))
    kind = _KIND_ALIASES[kind_raw]
    path = obj.get("path", obj.get("file", obj.get("location")))
    check_str(path, PeriodDescriptorError, w + ".path", allow_empty=False)
    if "\x00" in path or os.path.isabs(path):
        raise PeriodDescriptorError(
            "%s.path must be a relative path without NUL bytes" % w)
    parts = path.replace("\\", "/").split("/")
    if any(p in ("", "..") for p in parts):
        raise PeriodDescriptorError(
            "%s.path must not contain empty or '..' components" % w)
    fields = _parse_field_list(
        obj.get("fields", obj.get("columns")), w + ".fields")
    return MemberSpec(name, kind, path, fields, raw=dict(obj))


def _parse_endpoint(obj, where, members_by_name):
    _require_object(obj, where)
    unknown = set(obj) - {"member", "fields", "field"}
    if unknown:
        raise PeriodDescriptorError(
            "%s: unknown endpoint keys %r" % (where, sorted(unknown)))
    member_name = obj.get("member")
    check_str(member_name, PeriodDescriptorError, where + ".member",
              allow_empty=False)
    if member_name not in members_by_name:
        raise PeriodDescriptorError(
            "%s.member %r is not a declared member" % (where, member_name))
    raw_fields = obj.get("fields", obj.get("field"))
    if isinstance(raw_fields, str):
        raw_fields = [raw_fields]
    if not isinstance(raw_fields, list) or not raw_fields:
        raise PeriodDescriptorError(
            "%s.fields must be a non-empty list of field names" % where)
    fields = []
    for i, fname in enumerate(raw_fields):
        check_str(fname, PeriodDescriptorError,
                  "%s.fields[%d]" % (where, i), allow_empty=False)
        fields.append(fname)
    return RelationEndpoint(member_name, fields)


def _parse_relation(obj, index, where, members_by_name):
    w = "%s.relations[%d]" % (where, index)
    _require_object(obj, w)
    allowed = {"name", "from", "to", "source", "target", "references",
               "ref", "fields", "field_pairs"}
    unknown = set(obj) - allowed
    if unknown:
        raise PeriodDescriptorError(
            "%s: unknown relation keys %r" % (w, sorted(unknown)))

    name = obj.get("name")
    if name is None:
        name = "relation-%d" % index
    check_str(name, PeriodDescriptorError, w + ".name", allow_empty=False)

    from_obj = obj.get("from", obj.get("source", obj.get("references",
                                                          obj.get("ref"))))
    to_obj = obj.get("to", obj.get("target"))
    if from_obj is None or to_obj is None:
        raise PeriodDescriptorError(
            "%s must declare both 'from' and 'to' endpoints" % w)
    src = _parse_endpoint(from_obj, w + ".from", members_by_name)
    dst = _parse_endpoint(to_obj, w + ".to", members_by_name)
    if len(src.fields) != len(dst.fields):
        raise PeriodDescriptorError(
            "%s: endpoints list %d and %d fields; a relation pairs fields "
            "one-to-one and both sides must list the same number"
            % (w, len(src.fields), len(dst.fields)))

    # Explicit field-pair declarations are optional cross-checks.
    explicit_pairs = obj.get("field_pairs", obj.get("fields"))
    pairs = []
    for i, (sf, tf) in enumerate(zip(src.fields, dst.fields)):
        sfield = members_by_name[src.member_name].field(sf)
        tfield = members_by_name[dst.member_name].field(tf)
        if sfield is None:
            raise PeriodDescriptorError(
                "%s: member %r has no field %r"
                % (w, src.member_name, sf))
        if tfield is None:
            raise PeriodDescriptorError(
                "%s: member %r has no field %r"
                % (w, dst.member_name, tf))
        if sfield.ctype != tfield.ctype:
            raise PeriodDescriptorError(
                "%s: field pair %s.%s (%s) vs %s.%s (%s) has incompatible "
                "types; paired fields must share one type"
                % (w, src.member_name, sf, sfield.ctype,
                   dst.member_name, tf, tfield.ctype))
        pairs.append((sf, tf, sfield.ctype))
    if isinstance(explicit_pairs, list) and explicit_pairs and \
            all(isinstance(p, dict) for p in explicit_pairs):
        if len(explicit_pairs) != len(pairs):
            raise PeriodDescriptorError(
                "%s.field_pairs has %d entries but the endpoints pair %d "
                "fields" % (w, len(explicit_pairs), len(pairs)))
        for i, p in enumerate(explicit_pairs):
            if p.get("from") not in (None, pairs[i][0]) or \
                    p.get("to") not in (None, pairs[i][1]):
                raise PeriodDescriptorError(
                    "%s.field_pairs[%d] does not match the endpoint field "
                    "order" % (w, i))
            if "type" in p and p["type"] != pairs[i][2]:
                raise PeriodDescriptorError(
                    "%s.field_pairs[%d].type disagrees with the declared "
                    "field types" % (w, i))
    return RelationSpec(name, src, dst, pairs, raw=dict(obj))


def parse_period_descriptor(obj, path=None, base_dir=None):
    """Validate the descriptor object and return a :class:`PeriodDescriptor`."""
    where = "period descriptor[%s]" % path if path else "period descriptor"
    _require_object(obj, where)
    hdr = obj.get("archrestore_format")
    if not isinstance(hdr, dict):
        raise PeriodDescriptorError(
            "%s: missing archrestore_format header" % where)
    if set(hdr) != {"name", "version"}:
        raise PeriodDescriptorError(
            "%s: header must have exactly name and version" % where)
    if hdr.get("name") != PERIOD_KIND:
        raise PeriodDescriptorError(
            "%s: header name must be %r" % (where, PERIOD_KIND))
    if not is_int(hdr.get("version")) or hdr.get("version") != FORMAT_VERSION:
        raise PeriodDescriptorError(
            "%s: unsupported format version %r"
            % (where, hdr.get("version")))

    allowed = {"archrestore_format", "period_id", "id", "name", "members",
               "relations", "note"}
    unknown = set(obj) - allowed
    if unknown:
        raise PeriodDescriptorError(
            "%s: unknown top-level keys %r" % (where, sorted(unknown)))
    period_id = obj.get("period_id", obj.get("id", obj.get("name")))
    if period_id is not None:
        check_str(period_id, PeriodDescriptorError, where + ".period_id",
                  allow_empty=False)
    if "note" in obj:
        check_str(obj["note"], PeriodDescriptorError, where + ".note")

    members_obj = obj.get("members")
    if not isinstance(members_obj, list) or not members_obj:
        raise PeriodDescriptorError(
            "%s.members must be a non-empty list" % where)
    members = []
    members_by_name = {}
    for i, mobj in enumerate(members_obj):
        member = _parse_member(mobj, i, where)
        if member.name in members_by_name:
            raise PeriodDescriptorError(
                "%s: duplicate member name %r" % (where, member.name))
        members_by_name[member.name] = member
        members.append(member)

    relations_obj = obj.get("relations", [])
    if not isinstance(relations_obj, list):
        raise PeriodDescriptorError(
            "%s.relations must be a list" % where)
    relations = []
    seen_rel_names = set()
    for i, robj in enumerate(relations_obj):
        rel = _parse_relation(robj, i, where, members_by_name)
        if rel.name in seen_rel_names:
            raise PeriodDescriptorError(
                "%s: duplicate relation name %r" % (where, rel.name))
        seen_rel_names.add(rel.name)
        relations.append(rel)

    return PeriodDescriptor(members, relations, period_id=period_id,
                            raw=obj, path=path, base_dir=base_dir)


def load_period_descriptor(source, base_dir=None):
    """Load a descriptor from a path, or validate an already-parsed dict.

    Member paths are resolved relative to the descriptor's directory unless
    *base_dir* is supplied explicitly.
    """
    if isinstance(source, PeriodDescriptor):
        return source
    if isinstance(source, dict):
        return parse_period_descriptor(
            source, base_dir=base_dir or os.getcwd())
    path = os.fspath(source)
    obj = load_json(path)
    descriptor_dir = base_dir or os.path.dirname(os.path.abspath(path))
    return parse_period_descriptor(obj, path=path, base_dir=descriptor_dir)


# ---------------------------------------------------------------------------
# Canonical key encoding
# ---------------------------------------------------------------------------


def encode_scalar(value, ctype):
    """Encode one JSON scalar to canonical bytes for the declared field type.

    Returns ``None`` for a null/absent value.  Raises :class:`PeriodError`
    when the JSON value cannot belong to *ctype* (booleans are never valid,
    mirroring FORMAT.md §1).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise PeriodError("boolean is not a supported %s value" % ctype)
    if ctype == TYPE_STRING:
        if not isinstance(value, str):
            raise PeriodError(
                "expected a string, got %s" % type(value).__name__)
        raw = value.encode("utf-8")  # strict; rejects surrogates
        if len(raw) > 0xFFFFFFFF:
            raise PeriodError("string too long for canonical u32 length")
        return b"\x01" + len(raw).to_bytes(4, "little") + raw
    if ctype == TYPE_INTEGER:
        if not isinstance(value, int):
            raise PeriodError(
                "expected an integer, got %s" % type(value).__name__)
        if not (-(1 << 63) <= value <= (1 << 63) - 1):
            raise PeriodError("integer %d outside signed 64-bit range" % value)
        return b"\x02" + value.to_bytes(8, "little", signed=True)
    raise PeriodError("unknown field type %r" % (ctype,))


def order_key_for_canonical(canon):
    """Order-preserving binary key for one canonical scalar value.

    Integers become big-endian u64 with the sign bit flipped; strings use an
    order-preserving zero-terminated byte stuffing.  Null sorts before
    everything.  This is the same construction as
    :func:`archrestore.compare.order_key_for_canonical`.
    """
    tag = canon[0]
    if tag == 0:
        return b"\x00"
    if tag == 2:
        n = int.from_bytes(canon[1:9], "little", signed=True)
        return (n + (1 << 63)).to_bytes(8, "big")
    n = int.from_bytes(canon[1:5], "little")
    body = canon[5:5 + n]
    out = bytearray()
    append = out.append
    for b in body:
        if b == 0:
            append(1)
            append(1)
        elif b == 1:
            append(1)
            append(2)
        else:
            append(b)
    append(0)
    return bytes(out)


# ---------------------------------------------------------------------------
# Framed external-sort files
#
# Key frame: u32le(order_len), u32le(canon_len), u64le(row), then the order
# key and the canonical composite key.  Problem frame: u32le(payload_len),
# then the deterministic order key, then one JSON object.
# ---------------------------------------------------------------------------

_KEY_HEADER = struct.Struct("<IIQ")
_PROB_HEADER = struct.Struct("<I")


def _write_key_frame(fh, order_key, canon_key, row):
    fh.write(_KEY_HEADER.pack(len(order_key), len(canon_key), int(row)))
    fh.write(order_key)
    fh.write(canon_key)


class KeyFrameReader(object):
    __slots__ = ("path", "fh")

    def __init__(self, path):
        self.path = path
        self.fh = open(path, "rb")

    def next(self):
        header = self.fh.read(16)
        if header == b"":
            return None
        if len(header) != 16:
            raise PeriodError("key frame file %r is truncated" % self.path)
        lo, lc, row = _KEY_HEADER.unpack(header)
        order_key = self.fh.read(lo)
        canon_key = self.fh.read(lc)
        if len(order_key) != lo or len(canon_key) != lc:
            raise PeriodError("key frame file %r is truncated" % self.path)
        return order_key, canon_key, row

    def close(self):
        try:
            self.fh.close()
        except OSError:  # pragma: no cover
            pass


def _spill_key_run(records, run_path):
    records.sort(key=lambda rec: (rec[0], rec[2]))
    with open(run_path, "wb") as fh:
        for order_key, canon_key, row in records:
            _write_key_frame(fh, order_key, canon_key, row)
    return run_path


def _merge_key_runs(run_paths, out_path, fan_in=DEFAULT_FAN_IN):
    """Cascading bounded-fan-in k-way merge of sorted key runs."""
    if not run_paths:
        open(out_path, "wb").close()
        return out_path
    current = list(run_paths)
    tmp_dir = os.path.dirname(out_path)
    round_no = 0
    while len(current) > 1:
        merged = []
        for batch_start in range(0, len(current), fan_in):
            batch = current[batch_start:batch_start + fan_in]
            if len(batch) == 1:
                merged.append(batch[0])
                continue
            if len(current) <= fan_in and batch_start == 0 and \
                    batch_start + len(batch) >= len(current):
                target = out_path
            else:
                round_no += 1
                target = os.path.join(
                    tmp_dir, "key-merge-r%d-%d.run" % (round_no, batch_start))
            _merge_key_batch(batch, target)
            merged.append(target)
        current = merged
    if current[0] != out_path:
        shutil.move(current[0], out_path)
    return out_path


def _merge_key_batch(paths, out_path):
    readers = [KeyFrameReader(p) for p in paths]
    heap = []
    try:
        for i, fr in enumerate(readers):
            rec = fr.next()
            if rec is not None:
                order_key, canon_key, row = rec
                heapq.heappush(heap, (order_key, row, i, canon_key))
        with open(out_path, "wb") as fh:
            while heap:
                order_key, row, i, canon_key = heapq.heappop(heap)
                _write_key_frame(fh, order_key, canon_key, row)
                rec = readers[i].next()
                if rec is not None:
                    no, nc, nr = rec
                    heapq.heappush(heap, (no, nr, i, nc))
    finally:
        for fr in readers:
            fr.close()


# ---------------------------------------------------------------------------
# Problem log (spilled JSONL, externally sorted into a stable diagnosis)
# ---------------------------------------------------------------------------


class ProblemCollector(object):
    """Bounded-memory sink for validation problems.

    Problems are streamed as JSONL to a spool file; at most *limit* items are
    retained in memory for the JSON/human report.  Final ordering is produced
    by an external sort on a deterministic binary order key, so diagnosis
    order never depends on dict hashing or run boundaries.
    """

    def __init__(self, spool_path, limit=DEFAULT_PROBLEM_LIMIT):
        self.spool_path = spool_path
        self.limit = max(1, int(limit))
        self._fh = open(spool_path, "w", encoding="utf-8")
        self.items = []
        self.total = 0
        self.counts = {}

    def add(self, problem):
        kind = problem["kind"]
        self.total += 1
        self.counts[kind] = self.counts.get(kind, 0) + 1
        self._fh.write(json.dumps(problem, ensure_ascii=False,
                                  sort_keys=True) + "\n")
        if len(self.items) < self.limit:
            self.items.append(dict(problem))

    def close(self):
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def summary(self):
        return {
            "problem_count": self.total,
            "by_kind": dict(sorted(self.counts.items())),
            "problems_shown": len(self.items),
            "problems_truncated": len(self.items) < self.total,
        }


def _problem_order_key(problem):
    """Stable total order for diagnostics.

    member name, relation name ('' for member-level), problem kind, original
    row (jsonl line or archive row; -1 when not row-specific), then the JSON
    rendering as a final tie-breaker.
    """
    parts = [
        str(problem.get("member") or ""),
        str(problem.get("relation") or ""),
        str(problem.get("kind") or ""),
    ]
    out = b""
    for p in parts:
        raw = p.encode("utf-8")
        out += len(raw).to_bytes(4, "little") + raw
    row = problem.get("row")
    row_int = int(row) if isinstance(row, int) and not isinstance(row, bool) \
        else -1
    # Big-endian with the sign bit flipped so byte order matches numeric order
    # (the same construction used for integer canonical order keys); -1 sorts
    # before every non-negative physical row number.
    out += (row_int + (1 << 63)).to_bytes(8, "big")
    out += json.dumps(problem, ensure_ascii=False,
                      sort_keys=True).encode("utf-8")
    return out


def _external_sort_problems(spool_path, out_path, fan_in):
    """Bounded external sort of JSONL problems by the deterministic key."""
    work_dir = os.path.dirname(out_path)
    run_paths = []
    batch = []
    run_index = 0

    def flush():
        nonlocal batch, run_index
        if not batch:
            return
        batch.sort(key=lambda item: item[0])
        path = os.path.join(work_dir, "psort-%05d.run" % run_index)
        run_index += 1
        with open(path, "wb") as fh:
            for okey, payload in batch:
                fh.write(_PROB_HEADER.pack(len(okey)))
                fh.write(okey)
                fh.write(_PROB_HEADER.pack(len(payload)))
                fh.write(payload)
        run_paths.append(path)
        batch = []

    with open(spool_path, "r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            payload = line.strip().encode("utf-8")
            problem = json.loads(line)
            batch.append((_problem_order_key(problem), payload))
            if len(batch) >= DEFAULT_RUN_RECORDS:
                flush()
    flush()

    if not run_paths:
        open(out_path, "w", encoding="utf-8").close()
        return out_path

    all_temp = list(run_paths)
    current = list(run_paths)
    round_no = 0
    while len(current) > 1:
        merged = []
        for start in range(0, len(current), fan_in):
            group = current[start:start + fan_in]
            if len(group) == 1:
                merged.append(group[0])
                continue
            if len(current) <= fan_in and start == 0 and \
                    start + len(group) >= len(current):
                target = out_path + ".bin"
            else:
                round_no += 1
                target = os.path.join(
                    work_dir, "pmerge-r%d-%d.run" % (round_no, start))
                all_temp.append(target)
            _merge_problem_batch(group, target)
            merged.append(target)
        current = merged

    bin_path = current[0]
    # Emit final ordered JSONL.
    with open(bin_path, "rb") as src, open(out_path, "w",
                                           encoding="utf-8") as dst:
        while True:
            hdr = src.read(4)
            if not hdr:
                break
            (klen,) = _PROB_HEADER.unpack(hdr)
            src.read(klen)
            (plen,) = _PROB_HEADER.unpack(src.read(4))
            payload = src.read(plen)
            dst.write(payload.decode("utf-8") + "\n")
    for rp in all_temp:
        if os.path.exists(rp):
            os.unlink(rp)
    return out_path


def _merge_problem_batch(paths, out_path):
    readers = [open(p, "rb") for p in paths]
    heap = []

    def read_record(rf):
        hdr = rf.read(4)
        if not hdr:
            return None
        (klen,) = _PROB_HEADER.unpack(hdr)
        okey = rf.read(klen)
        (plen,) = _PROB_HEADER.unpack(rf.read(4))
        payload = rf.read(plen)
        return okey, payload

    try:
        for i, rf in enumerate(readers):
            rec = read_record(rf)
            if rec is not None:
                heapq.heappush(heap, (rec[0], i, rf, rec[1]))
        with open(out_path, "wb") as out:
            while heap:
                okey, i, rf, payload = heapq.heappop(heap)
                out.write(_PROB_HEADER.pack(len(okey)))
                out.write(okey)
                out.write(_PROB_HEADER.pack(len(payload)))
                out.write(payload)
                rec = read_record(rf)
                if rec is not None:
                    heapq.heappush(heap, (rec[0], i, rf, rec[1]))
    finally:
        for rf in readers:
            rf.close()


# ---------------------------------------------------------------------------
# Member profiling (content summaries + record-level member diagnostics)
# ---------------------------------------------------------------------------


class MemberProfile(object):
    __slots__ = ("member", "abs_path", "record_count", "content_digest",
                 "source_sha256", "column_count", "ok", "error")

    def __init__(self, member, abs_path):
        self.member = member
        self.abs_path = abs_path
        self.record_count = 0
        self.content_digest = None
        self.source_sha256 = None
        self.column_count = len(member.fields)
        self.ok = True
        self.error = None


def _profile_archive(member, abs_path, problems, workdir, verify):
    profile = MemberProfile(member, abs_path)
    if not os.path.isdir(abs_path):
        profile.ok = False
        profile.error = "not a directory"
        problems.add({"kind": PROBLEM_MEMBER_ERROR, "member": member.name,
                      "relation": None, "row": None,
                      "detail": "archive member path %r is not a directory"
                                % member.path})
        return profile, None
    try:
        reader = ArchiveReader(abs_path)
    except ArchError as exc:
        profile.ok = False
        profile.error = str(exc)
        problems.add({"kind": PROBLEM_MEMBER_ERROR, "member": member.name,
                      "relation": None, "row": None,
                      "detail": "cannot open archive: %s" % exc})
        return profile, None
    try:
        header = reader.header
        available = {c["name"]: c.get("type")
                     for c in header.get("columns", [])}
        for field in member.fields:
            actual = available.get(field.name)
            if actual is None:
                profile.ok = False
                problems.add({
                    "kind": PROBLEM_MEMBER_ERROR, "member": member.name,
                    "relation": None, "row": None,
                    "detail": "archive has no declared field %r (have %s)"
                              % (field.name, sorted(available))})
            elif actual != field.ctype:
                profile.ok = False
                problems.add({
                    "kind": PROBLEM_MEMBER_ERROR, "member": member.name,
                    "relation": None, "row": None,
                    "detail": "field %r declared %s but the archive column is "
                              "%s" % (field.name, field.ctype, actual)})
        if verify:
            result = verify_archive(abs_path)
            if not result.ok:
                profile.ok = False
                for msg in result.problems:
                    problems.add({
                        "kind": PROBLEM_MEMBER_ERROR,
                        "member": member.name, "relation": None, "row": None,
                        "detail": "self-verification: %s" % msg})
        if not profile.ok:
            return profile, reader

        row_count = int(header["row_count"])
        profile.record_count = row_count
        # Content digest: reuse the domain-separated archive content digest so
        # the period manifest agrees with `archrestore digest`.
        from .materialize import archive_content_digest
        profile.content_digest = archive_content_digest(abs_path)
        h = hashlib.sha256()
        with open(os.path.join(abs_path, "archive.json"), "rb") as fh:
            while True:
                chunk = fh.read(1 << 20)
                if not chunk:
                    break
                h.update(chunk)
        profile.source_sha256 = h.hexdigest()
        return profile, reader
    finally:
        if not profile.ok:
            reader.close()


def _iter_jsonl_records(path):
    """Yield ``(line_number, raw_line, record_or_none, error_or_none)``.

    The byte iteration is line-buffered; a single JSON object that itself
    exceeds memory is outside the bounded-memory envelope (the same envelope
    compare/materialize assume for one identity tuple) and is reported as an
    invalid record rather than crashing the run.
    """
    with open(path, "rb") as fh:
        line_no = 0
        for raw in fh:
            line_no += 1
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                text = stripped.decode("utf-8")
            except UnicodeDecodeError as exc:
                yield line_no, stripped, None, "not valid UTF-8: %s" % exc
                continue
            try:
                record = loads_strict(text)
            except ValueError as exc:
                yield line_no, stripped, None, "not valid JSON: %s" % exc
                continue
            if not isinstance(record, dict):
                yield line_no, stripped, None, \
                    "record must be a JSON object, got %s" \
                    % type(record).__name__
                continue
            yield line_no, stripped, record, None


def _profile_jsonl(member, abs_path, problems):
    profile = MemberProfile(member, abs_path)
    if not os.path.isfile(abs_path):
        profile.ok = False
        profile.error = "not a regular file"
        problems.add({"kind": PROBLEM_MEMBER_ERROR, "member": member.name,
                      "relation": None, "row": None,
                      "detail": "jsonl member path %r is not a regular file"
                                % member.path})
        return profile
    digest = hashlib.sha256()
    content = hashlib.sha256()
    content.update(_JSONL_DIGEST_DOMAIN)
    record_count = 0
    try:
        for line_no, raw, record, error in _iter_jsonl_records(abs_path):
            digest.update(raw)
            content.update(len(raw).to_bytes(8, "little"))
            content.update(raw)
            content.update(b"\n")
            if error is not None:
                problems.add({"kind": PROBLEM_INVALID_RECORD,
                              "member": member.name, "relation": None,
                              "row": line_no, "detail": error})
                continue
            record_count += 1
            for field in member.fields:
                if field.name not in record:
                    continue  # absence is treated as null at key extraction
                value = record[field.name]
                try:
                    encode_scalar(value, field.ctype)
                except PeriodError as exc:
                    problems.add({
                        "kind": PROBLEM_TYPE_MISMATCH,
                        "member": member.name, "relation": None,
                        "row": line_no, "field": field.name,
                        "declared_type": field.ctype,
                        "detail": str(exc)})
    except OSError as exc:
        profile.ok = False
        profile.error = str(exc)
        problems.add({"kind": PROBLEM_MEMBER_ERROR, "member": member.name,
                      "relation": None, "row": None,
                      "detail": "cannot read %r: %s" % (member.path, exc)})
        return profile
    profile.record_count = record_count
    profile.source_sha256 = digest.hexdigest()
    profile.content_digest = content.hexdigest()
    return profile


# ---------------------------------------------------------------------------
# Key index construction (one sorted key file per member/relation side)
# ---------------------------------------------------------------------------


def _build_archive_key_index(reader, member, field_names, ctypes, side_path,
                             workdir, run_records, run_bytes):
    cursors = [reader.column_cursor(name) for name in field_names]
    row_count = int(reader.header["row_count"])
    run = []
    buffered = 0
    run_paths = []
    run_index = 0

    def flush():
        nonlocal run, buffered, run_index
        if not run:
            return
        path = os.path.join(workdir, "%s-%05d.run" % (side_path, run_index))
        run_index += 1
        _spill_key_run(run, path)
        run_paths.append(path)
        run = []
        buffered = 0

    pool = reader.pool
    try:
        for row in range(row_count):
            canon_parts = []
            order_parts = []
            for cursor in cursors:
                raw = cursor.next_value()
                header = raw.header()
                body = b"".join(raw.iter_chunks(pool))
                canon = header + body
                canon_parts.append(canon)
                order_parts.append(order_key_for_canonical(canon))
            canon_key = b"".join(canon_parts)
            order_key = b"".join(order_parts)
            run.append((order_key, canon_key, row))
            buffered += len(order_key) + len(canon_key)
            if len(run) >= run_records or buffered >= run_bytes:
                flush()
        flush()
        final = os.path.join(workdir, "%s.sorted" % side_path)
        _merge_key_runs(run_paths, final)
        return final
    except (ArchError, OSError) as exc:
        # A corrupt/truncated/unreadable member stream is a member failure,
        # not a crash: clean up partial spills and let the driver diagnose it.
        for rp in run_paths:
            try:
                os.unlink(rp)
            except OSError:
                pass
        raise _MemberUnreadable(
            "member %r cannot be decoded while building key index: %s"
            % (member.name, exc), member.name)


def _build_jsonl_key_index(member, field_names, ctypes, abs_path, side_path,
                           workdir, problems, run_records, run_bytes):
    run = []
    buffered = 0
    run_paths = []
    run_index = 0

    def flush():
        nonlocal run, buffered, run_index
        if not run:
            return
        path = os.path.join(workdir, "%s-%05d.run" % (side_path, run_index))
        run_index += 1
        _spill_key_run(run, path)
        run_paths.append(path)
        run = []
        buffered = 0

    try:
        for line_no, _raw, record, error in _iter_jsonl_records(abs_path):
            if error is not None:
                continue  # already diagnosed during profiling
            canon_parts = []
            order_parts = []
            skip = False
            for fname, ctype in zip(field_names, ctypes):
                value = record.get(fname)
                try:
                    canon = encode_scalar(value, ctype)
                except PeriodError:
                    # Already diagnosed as field_type_mismatch during profiling.
                    skip = True
                    break
                if canon is None:
                    # Null component: encode the canonical null so empty keys
                    # sort first and are detected during the relation scan.
                    canon = b"\x00"
                canon_parts.append(canon)
                order_parts.append(order_key_for_canonical(canon))
            if skip:
                continue
            canon_key = b"".join(canon_parts)
            order_key = b"".join(order_parts)
            run.append((order_key, canon_key, line_no))
            buffered += len(order_key) + len(canon_key)
            if len(run) >= run_records or buffered >= run_bytes:
                flush()
        flush()
        final = os.path.join(workdir, "%s.sorted" % side_path)
        _merge_key_runs(run_paths, final)
        return final
    except OSError as exc:
        for rp in run_paths:
            try:
                os.unlink(rp)
            except OSError:
                pass
        raise _MemberUnreadable(
            "member %r cannot be read while building key index: %s"
            % (member.name, exc), member.name)


def _is_empty_canon_key(canon_key, field_count):
    """A composite key is empty iff every component encodes null."""
    pos = 0
    nulls = 0
    for _ in range(field_count):
        tag = canon_key[pos]
        if tag == 0:
            nulls += 1
            pos += 1
        elif tag == 1:
            n = int.from_bytes(canon_key[pos + 1:pos + 5], "little")
            pos += 5 + n
        else:
            pos += 9
    return nulls == field_count


def decode_composite_key(canon_key, ctypes):
    """Decode a composite canonical key into JSON scalar values in order."""
    values = []
    pos = 0
    for ctype in ctypes:
        tag = canon_key[pos]
        if tag == 0:
            values.append(None)
            pos += 1
        elif tag == 2:
            values.append(int.from_bytes(canon_key[pos + 1:pos + 9],
                                         "little", signed=True))
            pos += 9
        else:
            n = int.from_bytes(canon_key[pos + 1:pos + 5], "little")
            values.append(canon_key[pos + 5:pos + 5 + n].decode("utf-8"))
            pos += 5 + n
    return values


# ---------------------------------------------------------------------------
# Relation scans: target uniqueness + streaming merge-join referential check
# ---------------------------------------------------------------------------


def _scan_target_uniqueness(sorted_path, relation, target_member, ctypes,
                            problems):
    """Stream the referenced side; empty and duplicate keys are fatal."""
    fr = KeyFrameReader(sorted_path)
    field_count = len(ctypes)
    current_order = None
    current_canon = None
    group_rows = None
    try:
        while True:
            rec = fr.next()
            if rec is None:
                break
            order_key, canon_key, row = rec
            if order_key != current_order:
                _finish_target_group(group_rows, current_canon, relation,
                                     target_member, ctypes, field_count,
                                     problems)
                current_order = order_key
                current_canon = canon_key
                group_rows = [row]
            else:
                group_rows.append(row)
        _finish_target_group(group_rows, current_canon, relation,
                             target_member, ctypes, field_count, problems)
    finally:
        fr.close()


def _finish_target_group(rows, canon_key, relation, target_member, ctypes,
                         field_count, problems):
    if not rows:
        return
    if _is_empty_canon_key(canon_key, field_count):
        for row in rows:
            problems.add({
                "kind": PROBLEM_EMPTY_TARGET_KEY,
                "member": target_member.name,
                "relation": relation.name,
                "row": row,
                "fields": list(relation.to_endpoint.fields),
                "key": decode_composite_key(canon_key, ctypes),
            })
        return
    if len(rows) > 1:
        # One problem per participating row so every original row is named,
        # while the duplicate group stays identifiable via key + rows.
        for row in rows:
            problems.add({
                "kind": PROBLEM_DUPLICATE_TARGET_KEY,
                "member": target_member.name,
                "relation": relation.name,
                "row": row,
                "fields": list(relation.to_endpoint.fields),
                "key": decode_composite_key(canon_key, ctypes),
                "duplicate_rows": list(rows),
            })


def _scan_reference_join(ref_path, target_path, relation, ref_member,
                         target_member, ctypes, field_count, problems):
    """Streaming merge join verifying one-and-only-one target per reference.

    Both files are sorted by the same injective order key, so a single
    forward scan over the target side suffices.  The target's current key
    group is buffered in ``group`` (one order key, its rows); because target
    uniqueness is checked separately, a valid group holds exactly one row, so
    the buffer is O(1) for a valid period and only grows for already-fatal
    duplicates.  Repeated reference keys (a normal one-to-many relationship)
    re-read the buffered group and never require rewinding the file cursor.
    """
    fr_ref = KeyFrameReader(ref_path)
    fr_tgt = KeyFrameReader(target_path)
    counts = {"references": 0, "matched": 0, "unmatched": 0,
              "ambiguous": 0, "empty": 0}
    try:
        rref = fr_ref.next()
        next_tgt = fr_tgt.next()  # first target record, or None
        group = None              # (order_key, [rows]) for the current key

        def advance_group():
            """Consume one full equal-key target group from the forward scan."""
            nonlocal next_tgt
            if next_tgt is None:
                return None
            order_key = next_tgt[0]
            rows = []
            while next_tgt is not None and next_tgt[0] == order_key:
                rows.append(next_tgt[2])
                next_tgt = fr_tgt.next()
            return order_key, rows

        group = advance_group()
        while rref is not None:
            ref_order, ref_canon, ref_row = rref
            counts["references"] += 1
            if _is_empty_canon_key(ref_canon, field_count):
                counts["empty"] += 1
                problems.add({
                    "kind": PROBLEM_EMPTY_REFERENCE_KEY,
                    "member": ref_member.name,
                    "relation": relation.name,
                    "row": ref_row,
                    "fields": list(relation.from_endpoint.fields),
                    "key": decode_composite_key(ref_canon, ctypes),
                })
                rref = fr_ref.next()
                continue

            # Discard target groups whose key is behind the reference key;
            # references are sorted so such targets can never match again.
            while group is not None and group[0] < ref_order:
                group = advance_group()

            if group is not None and group[0] == ref_order:
                target_rows = group[1]  # order keys are injective per canon key
                if len(target_rows) > 1:
                    counts["ambiguous"] += 1
                    problems.add({
                        "kind": PROBLEM_AMBIGUOUS_REFERENCE,
                        "member": ref_member.name,
                        "relation": relation.name,
                        "row": ref_row,
                        "fields": list(relation.from_endpoint.fields),
                        "key": decode_composite_key(ref_canon, ctypes),
                        "target_rows": list(target_rows),
                    })
                else:
                    counts["matched"] += 1
            else:
                counts["unmatched"] += 1
                problems.add({
                    "kind": PROBLEM_UNMATCHED_REFERENCE,
                    "member": ref_member.name,
                    "relation": relation.name,
                    "row": ref_row,
                    "fields": list(relation.from_endpoint.fields),
                    "key": decode_composite_key(ref_canon, ctypes),
                })
            rref = fr_ref.next()
    finally:
        fr_ref.close()
        fr_tgt.close()
    return counts


# ---------------------------------------------------------------------------
# Period manifest
# ---------------------------------------------------------------------------


def _member_summary(profile):
    member = profile.member
    summary = {
        "name": member.name,
        "kind": member.kind,
        "path": member.path,
        "absolute_path": os.path.abspath(profile.abs_path),
        "fields": [f.to_json() for f in member.fields],
        "record_count": profile.record_count,
        "column_count": profile.column_count,
        "content_digest": profile.content_digest,
        "source_sha256": profile.source_sha256,
    }
    if member.kind == KIND_ARCHIVE:
        summary["digest_algorithm"] = "archrestore/archive-content/v1"
    else:
        summary["digest_algorithm"] = "archrestore/jsonl-content/v1"
    return summary


def _period_manifest_digest(manifest):
    """Domain-separated digest over the manifest's stable JSON rendering."""
    payload = json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":")).encode("utf-8")
    h = hashlib.sha256()
    h.update(_PERIOD_DIGEST_DOMAIN)
    h.update(payload)
    return h.hexdigest()


def _write_period_manifest(out_path, descriptor, profiles, relation_stats):
    manifest = {
        "archrestore_format": {"name": PERIOD_MANIFEST_KIND,
                               "version": FORMAT_VERSION},
        "status": "valid",
        "period_id": descriptor.period_id,
        "descriptor_sha256": _descriptor_sha256(descriptor),
        "member_count": len(descriptor.members),
        "relation_count": len(descriptor.relations),
        "members": [_member_summary(profiles[m.name])
                    for m in descriptor.members],
        "relations": [],
    }
    for rel in descriptor.relations:
        stats = relation_stats.get(rel.name, {})
        entry = rel.to_json()
        entry["references"] = stats.get("references", 0)
        entry["matched_references"] = stats.get("matched", 0)
        manifest["relations"].append(entry)
    manifest["manifest_digest"] = _period_manifest_digest(manifest)
    dest_dir = os.path.dirname(os.path.abspath(out_path)) or "."
    os.makedirs(dest_dir, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".period-manifest-", suffix=".tmp",
                               dir=dest_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, ensure_ascii=False, sort_keys=True,
                      indent=1)
            fh.write("\n")
        os.replace(tmp, out_path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return manifest


def _descriptor_sha256(descriptor):
    if descriptor.path and os.path.isfile(descriptor.path):
        h = hashlib.sha256()
        with open(descriptor.path, "rb") as fh:
            while True:
                chunk = fh.read(1 << 20)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    # Inline dict descriptors: hash their stable JSON rendering.
    return hashlib.sha256(json.dumps(descriptor.raw, ensure_ascii=False,
                                    sort_keys=True,
                                    separators=(",", ":")).encode("utf-8")
                         ).hexdigest()


# ---------------------------------------------------------------------------
# Top-level driver
# ---------------------------------------------------------------------------


def validate_period_package(descriptor, out=None, *, base_dir=None,
                            verify_archives=False, workdir=None,
                            keep_work=False, problems_out=None,
                            problem_limit=DEFAULT_PROBLEM_LIMIT,
                            run_records=DEFAULT_RUN_RECORDS,
                            run_bytes=DEFAULT_RUN_BYTES,
                            fan_in=DEFAULT_FAN_IN):
    """Validate a whole periodic delivery group.

    On success writes the period manifest to *out* (a path; when omitted a
    temporary file is used and its path returned) and returns a report dict.
    On failure raises :class:`PeriodValidationError` with a stable diagnosis;
    no manifest is created and an existing *out* is left untouched.
    """
    descriptor = load_period_descriptor(descriptor, base_dir=base_dir)
    owns_workdir = workdir is None
    if workdir is None:
        workdir = tempfile.mkdtemp(prefix="archrestore-period-")
    else:
        os.makedirs(workdir, exist_ok=True)
    problems_path = os.path.join(workdir, "period-problems.jsonl")
    problems = ProblemCollector(problems_path, limit=problem_limit)
    readers = []
    try:
        # ---- Phase 1: resolve paths and profile every member. -------------
        profiles = {}
        abs_paths = {}
        for member in descriptor.members:
            abs_path = os.path.join(descriptor.base_dir, member.path)
            abs_paths[member.name] = abs_path
            if member.kind == KIND_ARCHIVE:
                profile, reader = _profile_archive(
                    member, abs_path, problems, workdir, verify_archives)
                if reader is not None:
                    readers.append(reader)
            else:
                profile = _profile_jsonl(member, abs_path, problems)
            profiles[member.name] = profile

        usable_members = {name for name, p in profiles.items() if p.ok}

        # ---- Phase 2: per-relation key indexes and checks. ---------------
        relation_stats = {}
        index_cache = {}

        def key_index(member_name, field_names, ctypes, side_tag):
            cache_key = (member_name, tuple(field_names))
            if cache_key in index_cache:
                return index_cache[cache_key]
            member = descriptor.member(member_name)
            side_path = "keys-%s-%s" % (side_tag, member_name)
            if member.kind == KIND_ARCHIVE:
                reader = next(r for r in readers
                              if r.out_dir == abs_paths[member_name])
                path = _build_archive_key_index(
                    reader, member, field_names, ctypes, side_path, workdir,
                    run_records, run_bytes)
            else:
                path = _build_jsonl_key_index(
                    member, field_names, ctypes, abs_paths[member_name],
                    side_path, workdir, problems, run_records, run_bytes)
            index_cache[cache_key] = path
            return path

        for rel in descriptor.relations:
            src_member = descriptor.member(rel.from_endpoint.member_name)
            dst_member = descriptor.member(rel.to_endpoint.member_name)
            # A relation whose member is already broken cannot be scanned;
            # the member-level diagnosis already invalidates the period.
            if src_member.name not in usable_members or \
                    dst_member.name not in usable_members:
                continue
            src_fields = rel.from_endpoint.fields
            dst_fields = rel.to_endpoint.fields
            ctypes = [pair[2] for pair in rel.pairs]
            try:
                ref_sorted = key_index(src_member.name, src_fields, ctypes,
                                       "ref-%s" % rel.name)
                tgt_sorted = key_index(dst_member.name, dst_fields, ctypes,
                                       "tgt-%s" % rel.name)
            except _MemberUnreadable as exc:
                # Mark the member that failed to build so later relations
                # touching it are skipped rather than re-raising.
                failed = exc.member_name
                usable_members.discard(failed)
                profiles[failed].ok = False
                problems.add({"kind": PROBLEM_MEMBER_ERROR,
                              "member": failed,
                              "relation": None, "row": None,
                              "detail": str(exc)})
                continue
            _scan_target_uniqueness(tgt_sorted, rel, dst_member, ctypes,
                                    problems)
            stats = _scan_reference_join(
                ref_sorted, tgt_sorted, rel, src_member, dst_member,
                ctypes, len(ctypes), problems)
            relation_stats[rel.name] = stats

        problems.close()

        # ---- Phase 3: publish diagnosis or the period manifest. ----------
        if problems.total:
            ordered_path = os.path.join(workdir, "period-problems.sorted.jsonl")
            _external_sort_problems(problems_path, ordered_path, fan_in)
            report = {
                "ok": False,
                "outcome": "period_invalid",
                "status": "failed",
                "period_id": descriptor.period_id,
                "error": ("period invalid: %d validation problem(s); no "
                          "period manifest was written" % problems.total),
                "summary": problems.summary(),
                "problems": problems.items,
                "problems_path": os.path.abspath(ordered_path),
                "member_count": len(descriptor.members),
                "relation_count": len(descriptor.relations),
            }
            if problems_out:
                _publish_problems(ordered_path, problems_out)
                report["problems_path"] = os.path.abspath(
                    os.fspath(problems_out))
            else:
                # Preserve the full report outside the auto-cleaned workdir.
                fd, preserve = tempfile.mkstemp(
                    prefix="archrestore-period-problems-", suffix=".jsonl")
                os.close(fd)
                shutil.copyfile(ordered_path, preserve)
                report["problems_path"] = os.path.abspath(preserve)
            raise PeriodValidationError(report["error"], report)

        if out is None:
            fd, out = tempfile.mkstemp(prefix="archrestore-period-",
                                       suffix=".json")
            os.close(fd)
        manifest = _write_period_manifest(
            os.fspath(out), descriptor, profiles, relation_stats)
        report = {
            "ok": True,
            "outcome": "period_valid",
            "status": "valid",
            "period_id": descriptor.period_id,
            "manifest_path": os.path.abspath(os.fspath(out)),
            "manifest_digest": manifest["manifest_digest"],
            "member_count": len(descriptor.members),
            "relation_count": len(descriptor.relations),
            "members": manifest["members"],
            "relations": manifest["relations"],
            "summary": {
                "members": len(descriptor.members),
                "relations": len(descriptor.relations),
                "records": sum(p.record_count
                                for p in profiles.values()),
            },
        }
        return report
    finally:
        for reader in readers:
            reader.close()
        problems.close()
        if owns_workdir and not keep_work:
            shutil.rmtree(workdir, ignore_errors=True)


def _publish_problems(src, destination):
    destination = os.fspath(destination)
    dest_dir = os.path.dirname(os.path.abspath(destination)) or "."
    os.makedirs(dest_dir, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".period-problems-", suffix=".tmp",
                               dir=dest_dir)
    os.close(fd)
    shutil.copyfile(src, tmp)
    os.replace(tmp, destination)


# Convenience aliases for neighbouring vocabulary.
validate_period = validate_period_package
validate_group = validate_period_package
check_period_package = validate_period_package


def main(argv=None):  # pragma: no cover - thin CLI shim
    try:
        from .cli import build_parser, _cmd_validate_period
    except ImportError:
        from archrestore.cli import build_parser, _cmd_validate_period  # noqa: E402
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in ("validate-period", "period",
                                   "validate-package"):
        argv = ["validate-period"] + argv
    parser = build_parser()
    args = parser.parse_args(argv)
    return _cmd_validate_period(args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
