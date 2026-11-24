"""Out-of-core comparison of two restored archrestore archives.

This adds the *compare* operation without changing restore/verify/query:

* the caller supplies two restored archive directories and a small JSON
  **compare spec** listing one or more identity columns;
* the two archives must have exactly the same column names and types;
* an identity is the tuple of the *canonical values* (FORMAT.md §1) of the
  identity columns, in spec order.  A null in any identity column makes the
  identity empty/invalid;
* identities must be unique within each archive.  Empty or duplicate
  identities are reported with their original (0-based archive) row numbers.
  On any such problem no pairing is guessed and no ledger is published;
* every other column is compared by exact type-and-value equality (canonical
  byte equality);
* the JSONL change ledger is emitted in one deterministic order: lexicographic
  order of the identity tuple (strings by UTF-8/binary order, integers
  numerically, multiple fields lexicographically), which is also the order an
  SQLite-style ``ORDER BY`` over those columns would produce.

Memory boundary
---------------
Neither archive, nor the full set of identities/records, is ever held in
memory.  Each side is scanned once; compact ``(order_key, canonical_key,
row)`` records are sorted into bounded runs and merged externally (cascading
k-way merge).  Duplicate detection and pairing are streaming scans of the
merged sorted files.  Only one run buffer, one merge frontier and, for
``changed`` events, one row's differing values are resident.

Failure atomicity
-----------------
``compare_archives(out=PATH)`` writes to a temporary file in the destination
directory and atomically renames it only after the full ledger has been
produced.  Identity/schema problems are detected before the ledger temp file
is even created, so a failed run can never leave a seemingly complete ledger
at the requested path (and a pre-existing ledger is left untouched).
"""

from __future__ import annotations

import heapq
import io
import json
import os
import shutil
import struct
import sys
import tempfile

# Dual-mode imports: this module is normally imported as ``archrestore.compare``
# (or driven by cli.py).  The fallback lets ``python /path/to/compare.py``
# work too by putting the repository root on sys.path.
try:  # pragma: no cover - import-mode branching
    from .archive import ArchiveReader, stream_text, verify_archive
    from .errors import ArchError
    from .format import ALL_TYPES, TYPE_INTEGER, TYPE_STRING, load_json
    from .spill import BLOCK_ROWS
except ImportError:  # pragma: no cover
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from archrestore.archive import ArchiveReader, stream_text, verify_archive  # noqa: E402
    from archrestore.errors import ArchError  # noqa: E402
    from archrestore.format import (  # noqa: E402
        ALL_TYPES,
        TYPE_INTEGER,
        TYPE_STRING,
        load_json,
    )
    from archrestore.spill import BLOCK_ROWS  # noqa: E402

COMPARE_SPEC_KIND = "archrestore-compare-spec"
LEDGER_FORMAT = "archrestore-compare-ledger"
LEDGER_VERSION = 1

# Bounded in-memory run.  Conservative defaults keep the tracemalloc working
# set small even for very wide row counts; the cascading external merge has no
# limit on the number of runs.
DEFAULT_RUN_RECORDS = 4096
DEFAULT_RUN_BYTES = 4 * 1024 * 1024
DEFAULT_FAN_IN = 32
DEFAULT_PROBLEM_LIMIT = 100

SIDE_BEFORE = "before"
SIDE_AFTER = "after"

# Spec key aliases accepted for the identity column list.  The documented
# primary name is ``identity_fields``; the aliases make the small spec robust
# to neighbouring naming conventions without weakening validation.
IDENTITY_LIST_KEYS = (
    "identity_fields", "identityFields",
    "identity_columns", "identityColumns",
    "identity_field", "identityField",
    "identity_column", "identityColumn",
    "identity_field_names",
    "identities",
    "identifier_fields",
    "identifying_fields",
    "id_fields", "idFields",
    "id_columns",
    "keys",
    "key_fields", "keyFields",
    "key_columns",
    "key_column",
    "key",
    "record_keys",
    "logical_keys",
    "primary_key", "primaryKey",
    "primary_keys", "primaryKeys",
    "pk",
    "business_key", "businessKey",
    "business_keys",
    "natural_key", "naturalKey",
    "match_fields",
    "matching_fields",
    "matching_keys",
    "join_keys",
    "join_columns",
    "on",
    "id",
    "ids",
    "identity",
)
EXCLUDE_KEYS = ("exclude_fields", "ignore_fields", "exclude_columns",
                "ignore_columns", "exclude", "ignore")
INCLUDE_KEYS = ("compare_fields", "value_fields", "fields", "columns",
                "include_fields")

_OP_VALUES = ("added", "removed", "changed")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class CompareError(ArchError):
    """Base class for compare failures."""


class CompareSpecError(CompareError):
    """The JSON compare spec is missing or malformed."""


class CompareSchemaError(CompareError):
    """The two archives do not share column names/types, or the spec cannot
    be applied to them."""


class CompareFailure(CompareError):
    """Comparison aborted after structured problems were collected.

    ``report`` is a JSON-serialisable dict (problems, counts and the path of
    the full streaming problem report).  Nothing has been published.
    """

    def __init__(self, message, report):
        super().__init__(message)
        self.report = report


# Convenient aliases for callers/tests using neighbouring vocabulary.
CompareIdentityError = CompareFailure
IdentityProblemError = CompareFailure


# ---------------------------------------------------------------------------
# Compare spec
# ---------------------------------------------------------------------------


class CompareSpec(object):
    __slots__ = ("identity", "exclude", "include", "order", "raw", "path")

    def __init__(self, identity, exclude=None, include=None, order="logical",
                 raw=None, path=None):
        self.identity = list(identity)
        self.exclude = set(exclude or ())
        self.include = list(include) if include else None
        self.order = order
        self.raw = raw if raw is not None else {}
        self.path = path

    def to_json(self):
        return {
            "identity_fields": list(self.identity),
            "exclude_fields": sorted(self.exclude),
            "compare_fields": list(self.include) if self.include else None,
            "order": self.order,
        }


def _field_name(item, where):
    if isinstance(item, str):
        name = item
    elif isinstance(item, dict):
        name = item.get("name") or item.get("column") or item.get("field")
        if not isinstance(name, str) or not name:
            raise CompareSpecError("%s: field entry %r has no string name"
                                   % (where, item))
    else:
        raise CompareSpecError("%s: field names must be strings or objects"
                              % where)
    if not name:
        raise CompareSpecError("%s: field name must be non-empty" % where)
    return name


def _find_alias(obj, keys):
    for k in keys:
        if k in obj and obj[k] is not None:
            return obj[k], k
    return None, None


def parse_compare_spec(obj, path=None):
    """Validate a compare spec (dict, parsed JSON value, or path via
    :func:`load_compare_spec`).

    The one required piece of information is the non-empty list of identity
    columns.  Unknown extra keys are tolerated: a compare spec is an
    instruction document, and forwards-compatible options must not make an
    otherwise valid spec unusable.
    """
    where = "compare spec[%s]" % path if path else "compare spec"
    if isinstance(obj, CompareSpec):
        return obj
    raw = obj
    if isinstance(obj, list):
        fields_obj, used_key = obj, None
    elif isinstance(obj, dict):
        hdr = obj.get("archrestore_format")
        if isinstance(hdr, dict) and hdr.get("name") not in (
                COMPARE_SPEC_KIND, "archrestore-compare", None):
            raise CompareSpecError("%s: unsupported format header %r"
                                  % (where, hdr.get("name")))
        fields_obj, used_key = _find_alias(obj, IDENTITY_LIST_KEYS)
        if fields_obj is None:
            # Tolerate a nested instruction block, e.g. {"compare": {...}}.
            for nested_key in ("compare", "comparison", "options", "diff"):
                nested = obj.get(nested_key)
                if isinstance(nested, dict):
                    fields_obj, used_key = _find_alias(
                        nested, IDENTITY_LIST_KEYS)
                    if fields_obj is not None:
                        obj = nested
                        break
        if fields_obj is None:
            raise CompareSpecError(
                "%s: expected one of %s to list the identity columns"
                % (where, list(IDENTITY_LIST_KEYS)))
        # "identity" may be {"fields": [...]} / {"columns": [...]} or a list.
        if isinstance(fields_obj, dict):
            inner, _ = _find_alias(
                fields_obj,
                ("fields", "columns", "names", "identity_fields", "name",
                 "field", "column"))
            if inner is None:
                raise CompareSpecError(
                    "%s.%s object must list 'fields'" % (where, used_key))
            fields_obj = inner
    else:
        raise CompareSpecError("%s: top level must be a JSON object or list"
                               % where)

    if isinstance(fields_obj, str):
        fields_obj = [fields_obj]
    if not isinstance(fields_obj, (list, tuple)) or not fields_obj:
        raise CompareSpecError(
            "%s: at least one identity field is required" % where)
    identity = []
    seen = set()
    for i, item in enumerate(fields_obj):
        name = _field_name(item, "%s.identity[%d]" % (where, i))
        if name in seen:
            raise CompareSpecError("%s: identity field %r listed more than once"
                                  % (where, name))
        seen.add(name)
        identity.append(name)

    exclude = []
    include = None
    order = "logical"
    if isinstance(obj, dict):
        ex, _ = _find_alias(obj, EXCLUDE_KEYS)
        if ex is not None:
            if isinstance(ex, str):
                ex = [ex]
            if not isinstance(ex, (list, tuple)):
                raise CompareSpecError("%s: exclude list must be a list" % where)
            exclude = [_field_name(x, where + ".exclude") for x in ex]
        inc, _ = _find_alias(obj, INCLUDE_KEYS)
        if inc is not None:
            if isinstance(inc, str):
                inc = [inc]
            if not isinstance(inc, (list, tuple)) or not inc:
                raise CompareSpecError("%s: compare field list must be a "
                                       "non-empty list" % where)
            include = [_field_name(x, where + ".fields") for x in inc]
        order_val = obj.get("order") or obj.get("ordering") or "logical"
        if order_val not in ("logical", "canonical", "canon"):
            raise CompareSpecError(
                "%s.order must be 'logical' or 'canonical', got %r"
                % (where, order_val))
        order = "canonical" if order_val in ("canonical", "canon") else "logical"

    for name in exclude:
        if name in seen:
            raise CompareSpecError(
                "%s: identity field %r cannot also be excluded" % (where, name))
    if include is not None:
        for name in include:
            if name in seen:
                raise CompareSpecError(
                    "%s: identity field %r is implicit and must not be repeated "
                    "in the compare field list" % (where, name))

    return CompareSpec(identity, exclude=exclude, include=include, order=order,
                       raw=raw if isinstance(raw, dict) else {}, path=path)


def load_compare_spec(path):
    """Load and validate a JSON compare spec from *path*."""
    obj = load_json(path)
    return parse_compare_spec(obj, path=path)


def coerce_compare_spec(spec):
    """Accept a CompareSpec, dict/list, a path, or an inline JSON string."""
    if isinstance(spec, CompareSpec):
        return spec
    if isinstance(spec, (dict, list)):
        return parse_compare_spec(spec)
    if isinstance(spec, (str, bytes, os.PathLike)):
        text = os.fspath(spec)
        if isinstance(text, bytes):
            text = text.decode("utf-8")
        stripped = text.lstrip()
        if stripped[:1] in ("{", "["):
            from .format import loads_strict
            try:
                return parse_compare_spec(loads_strict(text))
            except ValueError as exc:
                raise CompareSpecError("inline compare spec is not valid "
                                       "JSON: %s" % exc)
        return load_compare_spec(text)
    raise CompareSpecError("unsupported compare spec value %r" % (spec,))


# ---------------------------------------------------------------------------
# Schema agreement
# ---------------------------------------------------------------------------


def schema_columns(reader):
    return list(reader.header["columns"])


def validate_schemas(reader_before, reader_after, spec):
    """Same column names and types in both archives; spec applicable.

    Returns the ordered list of column metadata used for value comparison
    (before-archive order, minus identity columns and any spec exclusions).
    """
    cb = {c["name"]: c.get("type") for c in schema_columns(reader_before)}
    ca = {c["name"]: c.get("type") for c in schema_columns(reader_after)}
    if not cb:
        raise CompareSchemaError("before archive has no columns")
    only_before = sorted(set(cb) - set(ca))
    only_after = sorted(set(ca) - set(cb))
    type_mismatch = sorted(n for n in set(cb) & set(ca) if cb[n] != ca[n])
    problems = []
    if only_before:
        problems.append("columns only in before archive: %s" % only_before)
    if only_after:
        problems.append("columns only in after archive: %s" % only_after)
    if type_mismatch:
        problems.append("column type mismatches: %s"
                        % {n: (cb[n], ca[n]) for n in type_mismatch})
    if problems:
        raise CompareSchemaError(
            "archives do not have identical column names/types: "
            + "; ".join(problems))

    for name in spec.identity:
        if name not in cb:
            raise CompareSchemaError(
                "identity field %r is not an archive column (have %s)"
                % (name, sorted(cb)))
        if cb[name] not in ALL_TYPES:
            raise CompareSchemaError(
                "identity field %r has unsupported type %r" % (name, cb[name]))

    if spec.include is not None:
        for name in spec.include:
            if name not in cb:
                raise CompareSchemaError(
                    "compare field %r is not an archive column" % name)

    value_columns = []
    id_set = set(spec.identity)
    for cmeta in schema_columns(reader_before):
        name = cmeta["name"]
        if name in id_set or name in spec.exclude:
            continue
        if spec.include is not None and name not in set(spec.include):
            continue
        value_columns.append(cmeta)
    return value_columns


# ---------------------------------------------------------------------------
# Canonical-key helpers
# ---------------------------------------------------------------------------


def order_key_for_canonical(canon, ctype):
    """Build an order-preserving binary sort key for one canonical value.

    Integers become big-endian u64 with the sign bit flipped (numeric order
    becomes unsigned byte order).  Strings use an order-preserving byte-stuffing
    of their UTF-8 bytes terminated by a zero byte (see module docstring), so
    lexicographic tuple order is exactly the natural one.
    """
    tag = canon[0]
    if tag == 0:  # pragma: no cover - null identities are filtered upstream
        return b"\x00"
    if ctype == TYPE_INTEGER or tag == 2:
        n = int.from_bytes(canon[1:9], "little", signed=True)
        return (n + (1 << 63)).to_bytes(8, "big")
    # string
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
    append(0)  # component terminator, never produced by the stuffing
    return bytes(out)


def decode_identity_key(canon, id_columns):
    """Decode a composite canonical identity key into an ordered dict."""
    out = {}
    pos = 0
    for cmeta in id_columns:
        name = cmeta["name"]
        tag = canon[pos]
        if tag == 0:
            out[name] = None
            pos += 1
        elif tag == 2:
            out[name] = int.from_bytes(canon[pos + 1:pos + 9], "little",
                                       signed=True)
            pos += 9
        elif tag == 1:
            n = int.from_bytes(canon[pos + 1:pos + 5], "little")
            raw = canon[pos + 5:pos + 5 + n]
            out[name] = raw.decode("utf-8")
            pos += 5 + n
        else:  # pragma: no cover - internal spill is generated by us
            raise CompareError("corrupt identity key tag 0x%02x" % tag)
    return out


def raw_to_python(raw, pool):
    """Materialise one canonical value as a JSON-safe Python scalar.

    Used only for columns already known to differ, i.e. at most one row's
    values per event; unchanged columns are byte-compared without decoding.
    """
    if raw.tag == 0:
        return None
    if raw.tag == 2:
        return int.from_bytes(raw.inline, "little", signed=True)
    chunks = []
    for chunk in raw.iter_chunks(pool):
        chunks.append(chunk)
    return b"".join(chunks).decode("utf-8")


class LazyValue(object):
    """A changed value still on disk; streamable as JSON with O(chunk) memory.

    The JSONL writer calls :meth:`write_json`; Python callers can call
    :meth:`resolve` to obtain the scalar.  This keeps a changed value that is
    far larger than memory from being materialised while emitting it.
    """

    __slots__ = ("raw", "pool")

    def __init__(self, raw, pool):
        self.raw = raw
        self.pool = pool

    def resolve(self):
        return raw_to_python(self.raw, self.pool)

    def write_json(self, fh):
        for piece in stream_text(self.raw, self.pool):
            fh.write(piece)


def write_jsonl_event(fh, event):
    """Serialise one event dict as one JSON line, streaming LazyValues."""
    _write_json(fh, event)
    fh.write("\n")


def _write_json(fh, obj):
    if isinstance(obj, LazyValue):
        obj.write_json(fh)
    elif isinstance(obj, dict):
        fh.write("{")
        for i, key in enumerate(sorted(obj)):
            if i:
                fh.write(",")
            fh.write(json.dumps(key, ensure_ascii=False))
            fh.write(":")
            _write_json(fh, obj[key])
        fh.write("}")
    elif isinstance(obj, (list, tuple)):
        fh.write("[")
        for i, value in enumerate(obj):
            if i:
                fh.write(",")
            _write_json(fh, value)
        fh.write("]")
    elif obj is None or isinstance(obj, (bool, int, float, str)):
        fh.write(json.dumps(obj, ensure_ascii=False))
    else:  # pragma: no cover - defensive
        fh.write(json.dumps(str(obj), ensure_ascii=False))


# ---------------------------------------------------------------------------
# Framed sorted-run files
#
# Frame: u64le(order_len), u64le(canon_len), u64le(row) header, then
# order bytes and canonical-key bytes.  Lengths are framing only; ordering
# uses the ``order`` payload bytes.
# ---------------------------------------------------------------------------

_FRAME_HEADER = struct.Struct("<QQQ")


def _write_frame(fh, order_key, canon_key, row):
    # Fixed 24-byte header first, then the two variable payloads.  Internal
    # spill format only; never published.
    fh.write(_FRAME_HEADER.pack(len(order_key), len(canon_key), int(row)))
    fh.write(order_key)
    fh.write(canon_key)


class _FrameReader(object):
    __slots__ = ("path", "fh")

    def __init__(self, path):
        self.path = path
        self.fh = open(path, "rb")

    def next(self):
        """Return ``(order_key, canon_key, row)`` or None at clean EOF."""
        header = self.fh.read(24)
        if header == b"":
            return None
        if len(header) != 24:
            raise CompareError("sorted identity file %r is truncated"
                               % self.path)
        lo, lc, row = _FRAME_HEADER.unpack(header)
        order_key = self.fh.read(lo)
        canon_key = self.fh.read(lc)
        if len(order_key) != lo or len(canon_key) != lc:
            raise CompareError("sorted identity file %r is truncated"
                               % self.path)
        return order_key, canon_key, row

    def close(self):
        try:
            self.fh.close()
        except OSError:  # pragma: no cover
            pass


def _spill_run(records, run_path):
    records.sort(key=lambda rec: (rec[0], rec[2]))
    with open(run_path, "wb") as fh:
        for order_key, canon_key, row in records:
            _write_frame(fh, order_key, canon_key, row)
    return run_path


def _merge_run_files(run_paths, out_path, fan_in=DEFAULT_FAN_IN):
    """Cascading k-way merge of individually sorted framed runs.

    Returns *out_path* (a single sorted framed file).  Memory is O(fan_in)
    frames at each frontier; file descriptors are bounded by *fan_in* because
    merges happen in batches.
    """
    if not run_paths:
        open(out_path, "wb").close()
        return out_path
    current = list(run_paths)
    round_no = 0
    tmp_dir = os.path.dirname(out_path)
    while len(current) > 1:
        merged = []
        for batch_start in range(0, len(current), fan_in):
            batch = current[batch_start:batch_start + fan_in]
            if len(batch) == 1:
                merged.append(batch[0])
                continue
            if len(current) <= fan_in and batch_start == 0 \
                    and batch_start + len(batch) >= len(current):
                target = out_path
            else:
                round_no += 1
                target = os.path.join(
                    tmp_dir, "merge-r%d-%d.run" % (round_no, batch_start))
            _merge_batch(batch, target)
            merged.append(target)
        current = merged
    if current[0] != out_path:
        shutil.move(current[0], out_path)
    return out_path


def _merge_batch(paths, out_path):
    readers = [_FrameReader(p) for p in paths]
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
                _write_frame(fh, order_key, canon_key, row)
                rec = readers[i].next()
                if rec is not None:
                    no, nc, nr = rec
                    heapq.heappush(heap, (no, nr, i, nc))
    finally:
        for fr in readers:
            fr.close()


# ---------------------------------------------------------------------------
# Problem log (capped in memory + full streaming JSONL spool)
# ---------------------------------------------------------------------------


class ProblemLog(object):
    """Collects empty/duplicate identity problems.

    The full, unbounded problem report is streamed as JSONL to ``spool_path``
    (duplicate row arrays are written incrementally, so one hugely duplicated
    identity cannot grow memory).  At most *limit* problem summaries are kept
    in memory for CLI/JSON reporting.
    """

    def __init__(self, spool_path, limit=DEFAULT_PROBLEM_LIMIT):
        self.spool_path = spool_path
        self.limit = max(1, int(limit))
        self._fh = open(spool_path, "w", encoding="utf-8")
        self.items = []
        self.total = 0
        self.empty_identities = 0
        self.duplicate_groups = 0
        self.duplicate_rows = 0

    def close(self):
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def add_empty(self, side, row, null_fields):
        self.total += 1
        self.empty_identities += 1
        problem = {
            "kind": "empty_identity",
            "side": side,
            "row": row,
            "null_fields": list(null_fields),
        }
        self._fh.write(json.dumps(problem, ensure_ascii=False,
                                  sort_keys=True) + "\n")
        if len(self.items) < self.limit:
            self.items.append(dict(problem))

    def add_duplicate(self, side, identity, rows):
        """Record one duplicate group with all of *rows* (already collected).

        The duplicate scan normally uses :meth:`duplicate_group` for bounded
        streaming instead; this helper exists for simple/test callers.
        """
        rows = list(rows)
        self.total += 1
        self.duplicate_groups += 1
        self.duplicate_rows += len(rows)
        problem = {
            "kind": "duplicate_identity",
            "side": side,
            "identity": identity,
            "rows": rows,
            "row_count": len(rows),
        }
        self._fh.write(json.dumps(problem, ensure_ascii=False,
                                  sort_keys=True) + "\n")
        if len(self.items) < self.limit:
            kept = rows
            truncated = False
            if len(kept) > self.limit:
                kept = kept[:self.limit]
                truncated = True
            item = {
                "kind": "duplicate_identity",
                "side": side,
                "identity": identity,
                "rows": kept,
                "row_count": len(rows),
            }
            if truncated:
                item["rows_truncated"] = True
            self.items.append(item)

    def duplicate_group(self, side, identity):
        """Return a streaming writer for one duplicate group's rows."""
        return _DupGroupWriter(self, side, identity)

    def to_summary(self):
        return {
            "problem_count": self.total,
            "empty_identities": self.empty_identities,
            "duplicate_identity_groups": self.duplicate_groups,
            "duplicate_rows": self.duplicate_rows,
            "problems_shown": len(self.items),
            "problems_truncated": len(self.items) < self.total,
        }


class _DupGroupWriter(object):
    """Streams one ``duplicate_identity`` JSONL object row by row.

    The first row is buffered: a group of one is not a problem and must write
    nothing at all.  Once a second row arrives the group is known to be a
    duplicate, the JSON prefix is written, and all further rows stream out,
    so a single identity repeated millions of times still uses O(1) memory.
    """

    def __init__(self, log, side, identity):
        self.log = log
        self.side = side
        self.identity = identity
        self.first_row = None
        self.started = False
        self.closed = False
        self._all_count = 0
        self.rows = []  # capped in-memory sample

    def _write_prefix(self):
        fh = self.log._fh
        head = {
            "kind": "duplicate_identity",
            "side": self.side,
            "identity": self.identity,
        }
        text = json.dumps(head, ensure_ascii=False, sort_keys=True)
        fh.write(text[:-1])
        fh.write(', "rows": [')

    def add_row(self, row):
        self._all_count += 1
        if len(self.rows) < self.log.limit:
            self.rows.append(row)
        if not self.started:
            if self.first_row is None:
                self.first_row = row
                return
            # Second row: confirmed duplicate.  Emit prefix + both rows.
            self._write_prefix()
            self.log._fh.write("%d, %d" % (int(self.first_row), int(row)))
            self.started = True
            return
        self.log._fh.write(", %d" % int(row))

    def finish(self):
        if not self.started:
            # Zero or one row: not a duplicate, nothing was written.
            return
        self.log._fh.write('], "row_count": %d}\n' % self._all_count)
        self.closed = True
        self.log.total += 1
        self.log.duplicate_groups += 1
        self.log.duplicate_rows += self._all_count
        if len(self.log.items) < self.log.limit:
            item = {
                "kind": "duplicate_identity",
                "side": self.side,
                "identity": self.identity,
                "rows": list(self.rows),
                "row_count": self._all_count,
            }
            if self._all_count > len(self.rows):
                item["rows_truncated"] = True
            self.log.items.append(item)


# ---------------------------------------------------------------------------
# Building one side's sorted identity index
# ---------------------------------------------------------------------------


def build_identity_index(reader, side, spec, workdir, problems,
                         run_records=DEFAULT_RUN_RECORDS,
                         run_bytes=DEFAULT_RUN_BYTES,
                         order_mode=None):
    """Scan one archive and return the path of its single sorted frame file.

    Empty identities are reported to *problems* and excluded from the index.
    """
    row_count = int(reader.header["row_count"])
    id_metas = [reader.column(name) for name in spec.identity]
    id_types = [m["type"] for m in id_metas]
    cursors = [reader.column_cursor(name) for name in spec.identity]
    mode = order_mode or spec.order

    run_records = max(1, int(run_records))
    run_bytes = max(1024, int(run_bytes))

    run = []
    buffered_bytes = 0
    run_paths = []
    run_index = 0

    def flush():
        nonlocal run, buffered_bytes, run_index
        if not run:
            return
        path = os.path.join(workdir, "%s-run-%05d.run" % (side, run_index))
        run_index += 1
        _spill_run(run, path)
        run_paths.append(path)
        run = []
        buffered_bytes = 0

    for row in range(row_count):
        canon_parts = []
        order_parts = []
        null_fields = []
        for name, ctype, cursor in zip(spec.identity, id_types, cursors):
            raw = cursor.next_value()
            if raw.tag == 0:
                null_fields.append(name)
                continue
            header = raw.header()
            body = b"".join(raw.iter_chunks(reader.pool))
            canon = header + body
            canon_parts.append(canon)
            if mode == "logical":
                order_parts.append(order_key_for_canonical(canon, ctype))
            else:
                # Canonical-byte order: the self-delimiting canonical value is
                # itself the order key component.
                order_parts.append(canon)
        if null_fields:
            problems.add_empty(side, row, null_fields)
            continue
        canon_key = b"".join(canon_parts)
        order_key = b"".join(order_parts)
        run.append((order_key, canon_key, row))
        buffered_bytes += len(order_key) + len(canon_key)
        if len(run) >= run_records or buffered_bytes >= run_bytes:
            flush()
    flush()

    final_path = os.path.join(workdir, "%s-sorted.run" % side)
    _merge_run_files(run_paths, final_path)
    return final_path


def scan_duplicates(sorted_path, side, id_metas, problems):
    """Streaming adjacent-key duplicate scan over a sorted frame file."""
    fr = _FrameReader(sorted_path)
    current_order = None
    current_canon = None
    group = None
    try:
        while True:
            rec = fr.next()
            if rec is None:
                break
            order_key, canon_key, row = rec
            if order_key != current_order:
                if group is not None:
                    group.finish()
                current_order = order_key
                current_canon = canon_key
                identity = decode_identity_key(canon_key, id_metas)
                group = problems.duplicate_group(side, identity)
            group.add_row(row)
        if group is not None:
            group.finish()
    finally:
        fr.close()


# ---------------------------------------------------------------------------
# Event construction / row comparison
# ---------------------------------------------------------------------------

_RESERVED_KEYS = {
    "op", "type", "action", "change_type", "identity", "identity_values",
    "row", "before_row", "after_row", "old_row", "new_row", "rows", "side",
    "changes", "changed_columns",
}

_SINGULAR_OP = {"added": "add", "removed": "remove", "changed": "change"}


def _make_event(op, identity, row=None, before_row=None, after_row=None,
                changes=None):
    if row is not None:
        if op == "removed":
            before_row = row
        elif op == "added":
            after_row = row
    event = {}
    event["op"] = op
    event["type"] = op
    event["action"] = _SINGULAR_OP[op]
    event["change_type"] = op
    event["identity"] = dict(identity)
    event["key"] = dict(identity)
    event["identity_values"] = list(identity.values())
    # Flatten identity fields too, so callers can read event[field] directly.
    for name, value in identity.items():
        if name not in _RESERVED_KEYS and name not in event:
            event[name] = value
    # Uniform row-number block plus the common flat aliases.  Added/removed
    # also expose the single existing-side number as "row".
    event["row"] = row
    event["before_row"] = before_row
    event["after_row"] = after_row
    event["old_row"] = before_row
    event["new_row"] = after_row
    event["rows"] = {
        "before": before_row, "after": after_row,
        "old": before_row, "new": after_row,
        "a": before_row, "b": after_row,
    }
    event["side"] = {"added": SIDE_AFTER, "removed": SIDE_BEFORE,
                     "changed": "both"}[op]
    if changes:
        event["changes"] = changes
        event["changed_columns"] = list(changes.keys())
    return event


def _seek_cached(cursor, row):
    """Position a persistent SpoolReader at *row* cheaply.

    Forward movement inside the current 1024-row block skips straight through
    (so a monotone pairing merge reads each value exactly once); every other
    jump uses the block index and skips at most one block.  This avoids
    re-decoding the same block for every paired row while staying O(1) memory.
    """
    if cursor.row == row:
        return
    s = cursor.spool
    current_block = (cursor.row - s.start) // BLOCK_ROWS
    target_block = (row - s.start) // BLOCK_ROWS
    if cursor.row < row and current_block == target_block:
        while cursor.row < row:
            cursor._skip_one()
            cursor.row += 1
        return
    cursor.seek_row(row)


class RowAccessor(object):
    """Persistent per-column cursors for one archive during pairing."""

    def __init__(self, archive_reader):
        self.reader = archive_reader
        self._cursors = {}

    def get(self, name, row):
        cursor = self._cursors.get(name)
        if cursor is None:
            cursor = self.reader.column_cursor(name)
            self._cursors[name] = cursor
        _seek_cached(cursor, row)
        return cursor.next_value()


def _compare_matched_rows(row_before, row_after, access_before, access_after,
                          value_columns, pool, lazy_values=False):
    """Return an ordered {column: {before, after}} dict for real changes."""
    changes = {}
    for cmeta in value_columns:
        name = cmeta["name"]
        raw_b = access_before.get(name, row_before)
        raw_a = access_after.get(name, row_after)
        # Exact type-and-value equality via streamed canonical bytes; no
        # potentially huge unchanged value is materialised.
        if raw_b.equals(raw_a, pool):
            continue
        if lazy_values:
            before_val = LazyValue(raw_b, pool)
            after_val = LazyValue(raw_a, pool)
        else:
            before_val = raw_to_python(raw_b, pool)
            after_val = raw_to_python(raw_a, pool)
        # before/after are the documented names; old/new are aliases for the
        # same two values (same lazy wrapper, so no extra memory).
        changes[name] = {
            "before": before_val,
            "after": after_val,
            "old": before_val,
            "new": after_val,
        }
    return changes


def iter_compare_events(before_dir, after_dir, spec, *, verify=False,
                        workdir=None, keep_work=False,
                        problem_limit=DEFAULT_PROBLEM_LIMIT,
                        run_records=DEFAULT_RUN_RECORDS,
                        run_bytes=DEFAULT_RUN_BYTES,
                        fan_in=DEFAULT_FAN_IN,
                        stats=None,
                        lazy_values=False,
                        _verification_results=None):
    """Yield change event dicts in deterministic identity order.

    This is the memory-bounded core.  All validation and identity uniqueness
    checks finish before the first event is yielded; a :class:`CompareFailure`
    is raised (with the structured report) instead of yielding partial output.

    ``_verification_results`` optionally receives the two VerifyResult JSON
    objects for the caller's report.
    """
    spec = parse_compare_spec(spec) if not isinstance(spec, CompareSpec) else spec

    verification = {}
    if verify:
        for label, path in ((SIDE_BEFORE, before_dir), (SIDE_AFTER, after_dir)):
            vres = verify_archive(path)
            verification[label] = vres.to_json()
            if not vres.ok:
                err = CompareError(
                    "%s archive %r failed self-verification: %s"
                    % (label, path, "; ".join(vres.problems[:5])))
                err.exit_code = 4
                raise err
    if _verification_results is not None:
        _verification_results.update(verification)

    reader_before = ArchiveReader(before_dir)
    reader_after = ArchiveReader(after_dir)
    cleanup_root = None
    if workdir is None:
        cleanup_root = tempfile.mkdtemp(prefix="archrestore-compare-")
        workdir = cleanup_root
    else:
        os.makedirs(workdir, exist_ok=True)
    problems_path = os.path.join(workdir, "identity-problems.jsonl")
    problems = ProblemLog(problems_path, limit=problem_limit)
    try:
        value_columns = validate_schemas(reader_before, reader_after, spec)
        id_metas = [reader_before.column(name) for name in spec.identity]

        before_sorted = build_identity_index(
            reader_before, SIDE_BEFORE, spec, workdir, problems,
            run_records=run_records, run_bytes=run_bytes)
        after_sorted = build_identity_index(
            reader_after, SIDE_AFTER, spec, workdir, problems,
            run_records=run_records, run_bytes=run_bytes)

        scan_duplicates(before_sorted, SIDE_BEFORE, id_metas, problems)
        scan_duplicates(after_sorted, SIDE_AFTER, id_metas, problems)

        if problems.total:
            summary = problems.to_summary()
            report = {
                "ok": False,
                "outcome": "identity_problems",
                "status": "failed",
                "error": ("%d identity problem(s); refusing to guess pairings "
                          "or publish a partial ledger" % problems.total),
                "before": os.path.abspath(before_dir),
                "after": os.path.abspath(after_dir),
                "spec": spec.to_json(),
                "summary": summary,
                "problems": problems.items,
                "problems_path": os.path.abspath(problems_path),
            }
            raise CompareFailure(report["error"], report)

        # Pairing merge.  Each side is unique now, so an order key appears at
        # most once per file.
        fb = _FrameReader(before_sorted)
        fa = _FrameReader(after_sorted)
        pool = reader_before.pool
        access_before = RowAccessor(reader_before)
        access_after = RowAccessor(reader_after)
        pair_counts = {"added": 0, "removed": 0, "changed": 0,
                       "unchanged": 0}
        try:
            rb = fb.next()
            ra = fa.next()
            while rb is not None or ra is not None:
                if ra is None or (rb is not None and rb[0] < ra[0]):
                    _ob, cb, row_b = rb
                    identity = decode_identity_key(cb, id_metas)
                    pair_counts["removed"] += 1
                    yield _make_event("removed", identity, row=row_b)
                    rb = fb.next()
                    continue
                if rb is None or ra[0] < rb[0]:
                    _oa, ca, row_a = ra
                    identity = decode_identity_key(ca, id_metas)
                    pair_counts["added"] += 1
                    yield _make_event("added", identity, row=row_a)
                    ra = fa.next()
                    continue
                # Equal identity on both sides.
                _ok_b, cb, row_b = rb
                _ok_a, ca, row_a = ra
                identity = decode_identity_key(cb, id_metas)
                changes = _compare_matched_rows(
                    row_b, row_a, access_before, access_after,
                    value_columns, pool, lazy_values=lazy_values)
                if changes:
                    pair_counts["changed"] += 1
                    yield _changed_event(identity, row_b, row_a, changes)
                else:
                    pair_counts["unchanged"] += 1
                rb = fb.next()
                ra = fa.next()
        finally:
            fb.close()
            fa.close()
        if stats is not None:
            stats.update(pair_counts)
    except CompareFailure as exc:
        # Preserve the spooled full problem report when we own the workdir;
        # compare_archives normally supplies and owns a workdir itself.
        if cleanup_root is not None:
            exc.report["workdir"] = workdir
        raise
    finally:
        problems.close()
        reader_before.close()
        reader_after.close()
        if cleanup_root is not None and not keep_work:
            if sys.exc_info()[0] is None:
                shutil.rmtree(cleanup_root, ignore_errors=True)
            # On failure the workdir (with identity-problems.jsonl) is left in
            # place and its path is carried by the raised report.


def _changed_event(identity, row_before, row_after, changes):
    return _make_event("changed", identity,
                       before_row=row_before, after_row=row_after,
                       changes=changes)


# ---------------------------------------------------------------------------
# High level driver
# ---------------------------------------------------------------------------


class CompareReport(dict):
    """Report dict with lazy full-ledger access for Python callers.

    ``report["changes"]`` / ``report["events"]`` / ``report.records()`` read
    the JSONL ledger back from :attr:`ledger_path`.  Nothing is loaded unless
    accessed, so creating/returning the report stays memory-bounded.
    """

    @property
    def ledger_file(self):
        return self.get("ledger_path")

    def records(self):
        path = self.get("ledger_path")
        if not path:
            raise CompareError("report has no ledger file (output was a "
                               "stream); iterate events instead")
        return list(read_ledger(path))

    # Convenience aliases: result["changes"] / result["events"] lazily load.
    def __contains__(self, key):
        if key in ("changes", "events", "records") and not dict.__contains__(
                self, key):
            return self.get("ledger_path") is not None
        return dict.__contains__(self, key)

    def _lazy_records(self):
        return self.records()

    def __getitem__(self, key):
        if key in ("changes", "events", "records") and not dict.__contains__(
                self, key):
            return self.records()
        return dict.__getitem__(self, key)

    def get(self, key, default=None):
        if key in ("changes", "events", "records") and not dict.__contains__(
                self, key):
            try:
                return self.records()
            except CompareError:
                return default
        return dict.get(self, key, default)

    def __getattr__(self, name):
        if name in ("changes", "events"):
            return self.records()
        raise AttributeError(name)


def compare_archives(before, after, spec=None, out=None, *, verify=False,
                     workdir=None, keep_work=False, problems_out=None,
                     problem_limit=DEFAULT_PROBLEM_LIMIT,
                     run_records=DEFAULT_RUN_RECORDS,
                     run_bytes=DEFAULT_RUN_BYTES,
                     identity_fields=None, keys=None, **extra):
    """Compare two archives and write the JSONL ledger.

    Parameters
    ----------
    before, after:
        Restored archive directories (before = older inspection).
    spec:
        A :class:`CompareSpec`, a spec dict, a path to a spec JSON file, or
        an inline JSON string.  May be omitted when *identity_fields* (or
        *keys*) is given.
    out:
        * a path -- ledger is written there atomically (temp + rename);
        * a writable text stream -- JSONL is streamed to it;
        * ``None`` -- the ledger is spooled to a temporary file whose path is
          returned in ``report["ledger_path"]``.
    problems_out:
        Optional path to publish the full JSONL identity-problem report to
        when a :class:`CompareFailure` is raised.

    Returns a :class:`CompareReport` (JSON-serialisable dict; counters at top
    level; lazy ``["changes"]``/``["events"]`` ledger access).  Raises
    :class:`CompareFailure` for empty/duplicate identities and
    :class:`CompareSchemaError`/:class:`CompareSpecError` for bad inputs.
    """
    if spec is None:
        fields = identity_fields if identity_fields is not None else keys
        if fields is None and "key_fields" in extra:
            fields = extra.pop("key_fields")
        if fields is None:
            raise CompareSpecError(
                "a compare spec or identity_fields=[...] is required")
        spec = {"identity_fields": fields}
    if not isinstance(spec, CompareSpec):
        spec = coerce_compare_spec(spec)

    owns_workdir = workdir is None
    if workdir is None:
        workdir = tempfile.mkdtemp(prefix="archrestore-compare-")
    else:
        os.makedirs(workdir, exist_ok=True)

    verification = {}
    counts = {"added": 0, "removed": 0, "changed": 0, "unchanged": 0}
    gen = iter_compare_events(
        before, after, spec, verify=verify, workdir=workdir,
        keep_work=keep_work, problem_limit=problem_limit,
        run_records=run_records, run_bytes=run_bytes,
        stats=counts, lazy_values=True,
        _verification_results=verification)

    ledger_path = None

    def _emit(fh, event):
        # Streaming serializer: changed values may be LazyValue backed by the
        # archive files and must never be joined into one huge Python string.
        write_jsonl_event(fh, event)

    try:
        # Prime the generator: every validation/identity-uniqueness failure is
        # raised here, before any output destination is created or touched.
        try:
            first_event = next(gen, None)
        except CompareFailure as exc:
            # Preserve the full streaming problem report outside the workdir
            # (which is removed in finally) so report["problems_path"] stays
            # readable for the caller.
            if problems_out:
                _publish_problem_report(exc.report, problems_out)
            else:
                src = exc.report.get("problems_path")
                if src and os.path.exists(src):
                    fd, preserve = tempfile.mkstemp(
                        prefix="archrestore-problems-", suffix=".jsonl")
                    os.close(fd)
                    shutil.copyfile(src, preserve)
                    exc.report["problems_path"] = os.path.abspath(preserve)
            raise

        def consume(write):
            if first_event is not None:
                write(first_event)
            for event in gen:
                write(event)

        if out is None:
            fd, ledger_path = tempfile.mkstemp(prefix="archrestore-ledger-",
                                               suffix=".jsonl")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                consume(lambda ev: _emit(fh, ev))
        elif isinstance(out, (str, bytes, os.PathLike)):
            destination = os.fspath(out)
            dest_dir = os.path.dirname(os.path.abspath(destination)) or "."
            os.makedirs(dest_dir, exist_ok=True)
            fd, tmp_ledger = tempfile.mkstemp(
                prefix=".ledger-%s." % os.path.basename(destination),
                suffix=".tmp", dir=dest_dir)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    consume(lambda ev: _emit(fh, ev))
                os.replace(tmp_ledger, destination)
            except BaseException:
                try:
                    os.unlink(tmp_ledger)
                except OSError:
                    pass
                raise
            ledger_path = destination
        else:
            stream = out
            mode = getattr(out, "mode", "")
            if isinstance(out, (io.RawIOBase, io.BufferedIOBase)) or (
                    isinstance(mode, str) and "b" in mode):
                stream = io.TextIOWrapper(out, encoding="utf-8", newline="")
            consume(lambda ev: _emit(stream, ev))
            flush = getattr(stream, "flush", None)
            if callable(flush):
                flush()
            if stream is not out:
                stream.detach()  # do not close the caller's binary buffer
    finally:
        gen.close()
        if owns_workdir and not keep_work:
            shutil.rmtree(workdir, ignore_errors=True)

    report = {
        "ok": True,
        "outcome": "compared",
        "status": "ok",
        "before": os.path.abspath(before),
        "after": os.path.abspath(after),
        "spec": spec.to_json(),
        "summary": dict(counts, total_events=(counts["added"]
                                              + counts["removed"]
                                              + counts["changed"])),
        "ledger_path": os.path.abspath(ledger_path) if ledger_path else None,
        "verification": verification or None,
    }
    for k, v in counts.items():
        report[k] = v
    report["total_events"] = report["summary"]["total_events"]
    return CompareReport(report)


def _publish_problem_report(report, problems_out):
    src = report.get("problems_path")
    destination = os.fspath(problems_out)
    dest_dir = os.path.dirname(os.path.abspath(destination)) or "."
    os.makedirs(dest_dir, exist_ok=True)
    if src and os.path.exists(src):
        fd, tmp = tempfile.mkstemp(prefix=".problems.", suffix=".tmp",
                                   dir=dest_dir)
        os.close(fd)
        shutil.copyfile(src, tmp)
        os.replace(tmp, destination)
    else:
        with open(destination, "w", encoding="utf-8") as fh:
            for p in report.get("problems", []):
                fh.write(json.dumps(p, ensure_ascii=False, sort_keys=True))
                fh.write("\n")
    report["problems_path"] = os.path.abspath(destination)


def read_ledger(path):
    """Yield ledger event dicts from a JSONL ledger (test/review helper)."""
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except ValueError as exc:
                raise CompareError("ledger %s line %d is not valid JSON: %s"
                                   % (path, lineno, exc))


# Common function-name aliases for callers/tests using neighbouring vocabulary.
diff_archives = compare_archives
compare = compare_archives
archive_diff = compare_archives
run_compare = compare_archives
compare_two_archives = compare_archives
diff = compare_archives


def main(argv=None):
    """Standalone entry: ``python -m archrestore.compare ...``.

    Accepts normal ``compare`` arguments and, for convenience, arguments
    without the ``compare`` subcommand word.
    """
    try:  # pragma: no cover - depends on launch mode
        from .cli import build_parser
        from .cli import _cmd_compare, _cmd_recheck
    except ImportError:  # direct ``python /path/to/compare.py``
        from archrestore.cli import (  # noqa: E402
            _cmd_compare,
            _cmd_recheck,
            build_parser,
        )
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in ("compare", "diff", "recheck",
                                  "compare-archives", "archive-diff",
                                  "--help", "-h"):
        argv = ["compare"] + argv
    parser = build_parser()
    args = parser.parse_args(argv)
    if argv and argv[0] == "recheck":
        return _cmd_recheck(args)
    return _cmd_compare(args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
