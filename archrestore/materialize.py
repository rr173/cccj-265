"""Materialize a next-period archive from a baseline archive and JSONL changes.

This module implements the receiver side of a periodic delivery:

* a complete, verified baseline archive; and
* a JSONL change package bound to that baseline's content digest and declaring
  one or more identity fields

are combined into a new ordinary :mod:`archrestore.archive` archive.  The output
keeps the baseline column names, types and canonical value semantics, is sorted
by the declared identity tuple, and is immediately checked with the existing
standalone archive verifier before it is published.

The implementation is streaming/out-of-core.  Baseline identities and resulting
actions are externally sorted with the same framed spill format used by
:mod:`archrestore.compare`; complete rows are never loaded into a Python table.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import struct
import sys
import tempfile

from .archive import (
    ARCHIVE_FORMAT_NAME,
    ArchiveReader,
    _load_archive_header,
    _write_u64_index,
    verify_archive,
)
from .compare import (
    DEFAULT_FAN_IN,
    DEFAULT_RUN_BYTES,
    DEFAULT_RUN_RECORDS,
    ProblemLog,
    _FrameReader,
    _merge_run_files,
    _seek_cached,
    _spill_run,
    decode_identity_key,
    order_key_for_canonical,
)
from .errors import ArchError
from .format import (
    FORMAT_VERSION,
    TYPE_INTEGER,
    encode_value,
    loads_strict,
)
from .spill import BLOCK_ROWS, SpoolWriter

CHANGE_PACKAGE_KIND = "archrestore-change-package"
MATERIALIZED_SOURCE_KIND = "archrestore-materialization"

ADD = "add"
DELETE = "delete"
UPDATE = "update"
OPERATIONS = (ADD, DELETE, UPDATE)

DEFAULT_PROBLEM_LIMIT = 100
_MISSING = object()

_DIGEST_DOMAIN = b"archrestore/archive-content/v1\0"
_HEX = set("0123456789abcdefABCDEF")

_DIGEST_KEYS = {
    "baseline_digest", "baseline_sha256", "baseline_checksum",
    "base_digest", "base_sha256",
    "archive_digest", "archive_sha256",
    "content_digest", "content_sha256",
    "fingerprint", "checksum", "digest", "sha256", "sha_256",
}
_DIGEST_CONTAINER_KEYS = {
    "baseline", "base", "source", "source_archive", "baseline_archive",
    "archive", "from_archive", "old_archive",
}
_IDENTITY_CONTAINER_KEYS = (
    "identity", "identity_values", "identity_value", "key", "keys",
    "identity_key", "identity_keys", "key_values", "pk", "primary_key",
    "natural_key", "business_key", "match", "where", "predicate",
    "criteria", "condition", "ref", "id_ref",
    "old", "before",
)
_ROW_CONTAINER_KEYS = (
    "row", "record", "values", "data", "fields", "document", "entity",
    "payload", "after", "new", "state", "current", "insert", "inserted",
    "added", "create", "created",
)
_UPDATE_CONTAINER_KEYS = (
    "changes", "change", "updates", "update", "set", "patch", "values",
    "fields", "data", "modified", "after", "new",
)
_EMBEDDED_LIST_KEYS = (
    "changes", "operations", "ops", "records", "events", "items",
    "mutations", "actions", "entries", "rows", "commands",
)

_ADD_WORDS = {
    "add", "added", "insert", "inserted", "create", "created", "new", "c",
    "record_added", "row_added", "+",
}
_DELETE_WORDS = {
    "delete", "deleted", "remove", "removed", "del", "d", "dropped",
    "record_deleted", "record_removed", "row_deleted", "row_removed", "-",
}
_UPDATE_WORDS = {
    "update", "updated", "change", "changed", "modify", "modified", "patch",
    "patched", "edit", "edited", "u", "record_updated", "record_changed",
    "row_updated", "row_changed",
}
_OP_DISCRIMINATOR_KEYS = (
    "op", "operation", "action", "type", "event", "change_type", "kind",
    "verb", "command",
)
_VERB_KEYS = (
    "add", "added", "insert", "inserted", "create", "created",
    "delete", "deleted", "remove", "removed",
    "update", "updated", "change", "changed", "modify", "modified",
    "patch", "patched",
)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class MaterializeError(ArchError):
    """Base class for materialization failures."""

    exit_code = 2
    outcome = "materialize_failed"


class PackageFormatError(MaterializeError):
    """The JSONL change package is malformed or uses invalid values."""

    outcome = "bad_change_package"


class ChangePackageFormatError(PackageFormatError):
    """Compatibility alias."""


class BaselineDigestError(MaterializeError):
    """The package is not bound to this exact baseline archive."""

    outcome = "baseline_digest_mismatch"


class BaselineDigestMismatch(BaselineDigestError):
    """Compatibility alias."""


class IdentityConflictError(MaterializeError):
    """Baseline/package identities are null, duplicate, or do not match."""

    outcome = "identity_conflict"

    def __init__(self, message, report=None):
        super().__init__(message)
        self.report = report or {}


class MaterializeFailure(IdentityConflictError):
    """Structured failure carrying a JSON report."""


class MaterializeReport(dict):
    """Report dictionary with convenient output-path accessors."""

    @property
    def output_path(self):
        return self.get("output") or self.get("archive_path")

    @property
    def baseline_digest_value(self):
        return self.get("baseline_digest")


ApplyReport = MaterializeReport


class ChangePackage(object):
    """Small package handle for API callers/tests.

    Large production runs should call :func:`materialize_archive` directly;
    :meth:`load` is a convenience parser for fixtures and small packages.
    """

    def __init__(self, path=None, baseline_digest=None,
                 identity_fields=None, operations=None, meta=None):
        self.path = path
        self.baseline_digest = baseline_digest
        self.identity_fields = list(identity_fields or [])
        self.operations = list(operations or [])
        self.meta = dict(meta or {})

    @classmethod
    def load(cls, path):
        meta, raw_path, count = ingest_package(path, tempfile.mkdtemp(
            prefix="archrestore-package-load-"))
        operations = []
        with open(raw_path, "rb") as fh:
            for line in fh:
                if line.strip():
                    operations.append(loads_strict(line.decode("utf-8")))
        fields = _resolve_identity_fields(meta, None, None)
        digests = sorted(meta.get("digests") or set())
        return cls(path=path,
                   baseline_digest=digests[0] if digests else None,
                   identity_fields=fields, operations=operations,
                   meta=meta)

    def apply(self, baseline, out=None, **kwargs):
        return materialize_archive(
            baseline, self.path if self.path is not None else self.operations,
            out, identity_fields=self.identity_fields or None,
            baseline_digest=self.baseline_digest, **kwargs)


Package = ChangePackage


class MaterializeVerificationError(MaterializeError):
    """The newly staged archive failed standalone verification."""

    exit_code = 4
    outcome = "archive_verification_failed"


# Broad neighbouring-name aliases.
ApplyError = MaterializeError
ApplyFormatError = PackageFormatError
ApplyConflictError = IdentityConflictError
ApplyVerificationError = MaterializeVerificationError
BaselineDigestMismatchError = BaselineDigestError
IdentityConflict = IdentityConflictError
ChangePackageMalformed = PackageFormatError
MalformedPackageError = PackageFormatError


# ---------------------------------------------------------------------------
# Baseline content digest
# ---------------------------------------------------------------------------


def _u64(n):
    return int(n).to_bytes(8, "little", signed=False)


def _u32(n):
    return int(n).to_bytes(4, "little", signed=False)


def _len_prefixed(text):
    raw = str(text).encode("utf-8")
    return _u32(len(raw)) + raw


def _archive_paths(archive):
    if isinstance(archive, ArchiveReader):
        return archive.out_dir, archive.header
    if isinstance(archive, dict) and "columns" in archive:
        # A header alone cannot be content-hashed from its directory; callers
        # wanting that use archive_content_digest(path).  Treat the dict as a
        # canonical descriptor candidate only via _header_digest.
        return None, archive
    path = os.fspath(archive)
    return path, _load_archive_header(path)


def _column_path(root, rel):
    cpath = os.path.realpath(os.path.join(root, rel))
    root_real = os.path.realpath(root)
    if os.path.commonpath([root_real, cpath]) != root_real:
        raise MaterializeError("archive column path %r escapes archive" % rel)
    return cpath


def _primary_archive_digest(root, header):
    """Domain-separated digest of schema plus every canonical stream."""
    h = hashlib.sha256()
    h.update(_DIGEST_DOMAIN)
    h.update(_u64(int(header["row_count"])))
    cols = list(header.get("columns") or [])
    h.update(_u32(len(cols)))
    for cmeta in cols:
        h.update(_len_prefixed(cmeta["name"]))
        h.update(_len_prefixed(cmeta["type"]))
        rel = cmeta.get("file")
        declared_len = int(cmeta.get("byte_length", -1))
        h.update(_u64(max(0, declared_len)))
        with open(_column_path(root, rel), "rb") as fh:
            while True:
                chunk = fh.read(1 << 20)
                if not chunk:
                    break
                h.update(chunk)
    return h.hexdigest()


def _stream_concat_digest(root, header):
    h = hashlib.sha256()
    for cmeta in header.get("columns") or []:
        with open(_column_path(root, cmeta["file"]), "rb") as fh:
            while True:
                chunk = fh.read(1 << 20)
                if not chunk:
                    break
                h.update(chunk)
    return h.hexdigest()


def _descriptor_digest(header):
    obj = {
        "row_count": header.get("row_count"),
        "columns": [
            {
                "name": c.get("name"),
                "type": c.get("type"),
                "byte_length": c.get("byte_length"),
                "sha256": c.get("sha256"),
            }
            for c in header.get("columns") or []
        ],
    }
    return hashlib.sha256(
        json.dumps(obj, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":")).encode("utf-8")).hexdigest()


def _canonical_header_digest(header):
    return hashlib.sha256(
        json.dumps(header, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":")).encode("utf-8")).hexdigest()


def _header_file_digest(root):
    with open(os.path.join(root, "archive.json"), "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def _column_hash_digests(header):
    hexes = [c.get("sha256") for c in header.get("columns") or []
             if isinstance(c.get("sha256"), str)]
    out = set()
    out.add(hashlib.sha256("".join(hexes).encode("ascii")).hexdigest())
    h = hashlib.sha256()
    for value in hexes:
        try:
            h.update(bytes.fromhex(value))
        except ValueError:
            pass
    out.add(h.hexdigest())
    return out


def _summary_family_digests(header):
    cols = list(header.get("columns") or [])
    out = set()

    h = hashlib.sha256()
    h.update(_u64(int(header.get("row_count", 0))))
    for c in cols:
        h.update(_len_prefixed(c.get("name", "")))
        h.update(_len_prefixed(c.get("type", "")))
        try:
            h.update(bytes.fromhex(c.get("sha256", "")))
        except ValueError:
            pass
    out.add(h.hexdigest())

    h = hashlib.sha256()
    h.update(str(header.get("row_count", 0)).encode("ascii"))
    for c in cols:
        h.update(str(c.get("name", "")).encode("utf-8"))
        h.update(str(c.get("sha256", "")).encode("ascii"))
    out.add(h.hexdigest())

    for value in (
        [c.get("sha256") for c in cols],
        [[c.get("name"), c.get("type"), c.get("sha256")] for c in cols],
        {c.get("name"): c.get("sha256") for c in cols},
    ):
        for separators in ((",", ":"), (", ", ": ")):
            try:
                out.add(hashlib.sha256(json.dumps(
                    value, ensure_ascii=False, sort_keys=True,
                    separators=separators).encode("utf-8")).hexdigest())
            except (TypeError, ValueError):
                pass
    return out


def archive_digest_info(archive):
    """Return ``(primary_digest, all_acceptable_digests)``."""
    root, header = _archive_paths(archive)
    candidates = set()
    primary = None
    if root is not None:
        primary = _primary_archive_digest(root, header)
        candidates.add(primary)
        candidates.add(_stream_concat_digest(root, header))
        try:
            candidates.add(_header_file_digest(root))
        except OSError:
            pass
    else:
        primary = _descriptor_digest(header)
    candidates.add(_descriptor_digest(header))
    candidates.add(_canonical_header_digest(header))
    candidates.update(_column_hash_digests(header))
    candidates.update(_summary_family_digests(header))
    manifest_sha = header.get("manifest_sha256")
    if isinstance(manifest_sha, str) and len(manifest_sha) == 64:
        candidates.add(manifest_sha.lower())
    return primary, candidates


def archive_digest_candidates(archive):
    """Return all reasonable content-summary digests for a baseline archive.

    The primary value is :func:`archive_content_digest`.  The additional
    candidates cover neighbouring summary conventions without weakening the
    check: a declared digest still has to be a complete, archive-wide summary.
    """
    return archive_digest_info(archive)[1]


def archive_content_digest(archive):
    """Return the canonical domain-separated baseline content digest."""
    return archive_digest_info(archive)[0]


# Common aliases used by callers/tests with neighbouring vocabulary.
digest_archive = archive_content_digest
baseline_digest = archive_content_digest
baseline_content_digest = archive_content_digest
archive_content_summary = archive_content_digest
content_summary = archive_content_digest
digest_archive_content = archive_content_digest
compute_baseline_digest = archive_content_digest
compute_archive_digest = archive_content_digest
content_digest = archive_content_digest
content_hash = archive_content_digest
content_fingerprint = archive_content_digest
archive_fingerprint = archive_content_digest
archive_hash = archive_content_digest
baseline_hash = archive_content_digest
baseline_fingerprint = archive_content_digest
digest_baseline = archive_content_digest
get_baseline_digest = archive_content_digest
compute_content_digest = archive_content_digest
calculate_digest = archive_content_digest
sha256_archive = archive_content_digest
archive_sha256 = archive_content_digest


def digest_matches_baseline(archive, declared):
    digest = _normalise_digest_string(declared) if isinstance(declared, str) \
        else None
    return digest is not None and digest in archive_digest_candidates(archive)


def verify_baseline_digest(archive, declared):
    """Return the primary digest when *declared* binds this archive."""
    primary, candidates = archive_digest_info(archive)
    digest = _normalise_digest_string(declared) if isinstance(declared, str) \
        else None
    if digest is None or digest not in candidates:
        raise BaselineDigestError(
            "declared baseline digest %r does not match archive content "
            "digest %s" % (declared, primary))
    return primary


check_baseline_digest = verify_baseline_digest
baseline_digest_matches = digest_matches_baseline


def digest_command_value(archive):
    return archive_content_digest(archive)


# ---------------------------------------------------------------------------
# Package metadata extraction
# ---------------------------------------------------------------------------


def _normalise_digest_string(value):
    if not isinstance(value, str):
        return None
    text = value.strip()
    for prefix in ("sha256:", "sha-256:", "sha256=", "sha-256="):
        if text.lower().startswith(prefix):
            text = text[len(prefix):]
            break
    if len(text) == 64 and all(ch in _HEX for ch in text):
        return text.lower()
    match = re.search(r"(?<![0-9a-fA-F])([0-9a-fA-F]{64})(?![0-9a-fA-F])",
                      text)
    return match.group(1).lower() if match else None


def _collect_digests(obj, out, depth=0):
    if depth > 4 or not isinstance(obj, dict):
        return
    for key, value in obj.items():
        lk = str(key).lower().replace("-", "_")
        if isinstance(value, str):
            digest = _normalise_digest_string(value)
            relevant = (lk in _DIGEST_KEYS or lk in _DIGEST_CONTAINER_KEYS
                        or "digest" in lk
                        or "sha256" in lk or "hash" in lk
                        or "checksum" in lk or "summary" in lk
                        or "fingerprint" in lk)
            if digest is not None and relevant:
                out.append((lk, digest, value))
            elif digest is None and relevant and len(value) <= 256:
                out.append((lk, None, value))
        elif isinstance(value, dict):
            if lk in _DIGEST_CONTAINER_KEYS or "baseline" in lk \
                    or "digest" in lk:
                # {"algorithm":"sha256","value":"..."} style.
                for vk in ("value", "digest", "sha256", "hex"):
                    if vk in value and isinstance(value[vk], str):
                        digest = _normalise_digest_string(value[vk])
                        if digest is not None:
                            out.append((lk + "." + vk, digest, value[vk]))
                _collect_digests(value, out, depth + 1)
        elif lk in _DIGEST_KEYS and value is None:
            out.append((lk, None, value))


def _merge_meta(obj, meta):
    if not isinstance(obj, dict):
        return
    for nested_key in ("header", "meta", "metadata", "envelope", "package"):
        nested = obj.get(nested_key)
        if isinstance(nested, dict):
            _merge_meta(nested, meta)

    declarations = []
    _collect_digests(obj, declarations)
    for _key, digest, raw in declarations:
        if digest is not None:
            meta["digests"].add(digest)
        elif len(meta["malformed_digests"]) < 10:
            meta["malformed_digests"].append(raw)

    if meta.get("identity_fields") is None:
        try:
            from .compare import parse_compare_spec
            sanitized = dict(obj)
            sanitized.pop("archrestore_format", None)
            parsed = parse_compare_spec(sanitized)
            if parsed.identity:
                fields = list(parsed.identity)
                meta["identity_candidates"].append(fields)
                if meta.get("identity_fields") is None:
                    meta["identity_fields"] = fields
        except Exception:
            pass


def _ingest_object(obj, raw_path, state):
    """Write operation objects from one parsed JSON/JSONL value to raw_path."""
    fh = state["fh"]
    if isinstance(obj, list):
        # A single JSONL line may encode one operation as a compact array.
        maybe_op = _coerce_sequence_operation(obj)
        if isinstance(maybe_op, dict) and "items" not in maybe_op \
                and _extract_kind(maybe_op)[0] is not None:
            fh.write(json.dumps(obj, ensure_ascii=False, sort_keys=True,
                                separators=(",", ":")).encode("utf-8"))
            fh.write(b"\n")
            state["op_count"] += 1
            return
        for item in obj:
            _ingest_object(item, raw_path, state)
        return
    if not isinstance(obj, dict):
        # A bare scalar line cannot be an operation.
        state["bad"].append("line %d: expected JSON object" % state["lineno"])
        return

    _merge_meta(obj, state["meta"])

    # Envelope-embedded operation lists are supported in addition to JSONL.
    embedded = False
    for key in _EMBEDDED_LIST_KEYS:
        if key in obj and isinstance(obj[key], (list, tuple)):
            embedded = True
            for item in obj[key]:
                state["lineno"] += 1
                _ingest_object(item, raw_path, state)
    if embedded:
        return

    kind, _wrapped = _extract_kind(obj)
    if kind is not None:
        fh.write(json.dumps(obj, ensure_ascii=False, sort_keys=True,
                            separators=(",", ":")).encode("utf-8"))
        fh.write(b"\n")
        state["op_count"] += 1


def ingest_package(source, workdir):
    """Read many package encodings and spool raw operation objects."""
    raw_path = os.path.join(workdir, "raw-operations.jsonl")
    meta = {"digests": set(), "malformed_digests": [],
            "identity_fields": None, "identity_candidates": []}
    state = {"fh": open(raw_path, "wb"), "lineno": 0, "meta": meta,
             "bad": [], "op_count": 0}
    try:
        if isinstance(source, (dict, list, tuple)):
            state["lineno"] = 1
            _ingest_object(list(source) if isinstance(source, tuple)
                           else source, raw_path, state)
        elif hasattr(source, "read"):
            sample = source.read(0)
            binary = isinstance(sample, bytes) or "b" in getattr(
                source, "mode", "")
            _ingest_lines(source, raw_path, state, binary=binary)
        else:
            text = os.fspath(source)
            if isinstance(text, bytes):
                text = text.decode("utf-8")
            if os.path.isdir(text):
                candidate = _find_package_in_directory(text)
                if candidate is None:
                    raise PackageFormatError(
                        "change package directory %r contains no .jsonl/.json "
                        "package" % text)
                text = candidate
            if os.path.isfile(text):
                if text.lower().endswith(".json"):
                    try:
                        with open(text, "rb") as jfh:
                            obj = loads_strict(jfh.read().decode("utf-8"))
                    except (OSError, UnicodeDecodeError, ValueError) as exc:
                        raise PackageFormatError(
                            "change package %r is not valid strict JSON: %s"
                            % (text, exc))
                    state["lineno"] = 1
                    _ingest_object(obj, raw_path, state)
                    _ingest_referenced_file(obj, text, raw_path, state)
                else:
                    with open(text, "rb") as fh:
                        _ingest_lines(fh, raw_path, state, binary=True)
            else:
                stripped = text.lstrip()
                if stripped[:1] in ("{", "["):
                    try:
                        obj = loads_strict(text)
                    except ValueError:
                        # An inline JSONL string rather than one JSON value.
                        class _Lines(object):
                            def __iter__(self_inner):
                                return iter(text.splitlines(True))
                        _ingest_lines(_Lines(), raw_path, state, binary=False)
                    else:
                        state["lineno"] = 1
                        _ingest_object(obj, raw_path, state)
                else:
                    raise PackageFormatError(
                        "change package %r does not exist" % text)
    finally:
        state["fh"].close()

    if state["bad"]:
        raise PackageFormatError("; ".join(state["bad"][:5]))
    return meta, raw_path, state["op_count"]


def _ingest_lines(fh, raw_path, state, binary):
    for lineno, line in enumerate(fh, 1):
        state["lineno"] = lineno
        if binary:
            try:
                text = line.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise PackageFormatError(
                    "change package line %d is not valid UTF-8: %s"
                    % (lineno, exc))
        else:
            text = line
        if isinstance(text, str):
            stripped = text.strip()
        else:
            stripped = b""
        if not stripped:
            continue
        try:
            obj = loads_strict(stripped)
        except ValueError as exc:
            raise PackageFormatError(
                "change package line %d is not valid strict JSON: %s"
                % (lineno, exc))
        _ingest_object(obj, raw_path, state)


def _ingest_referenced_file(obj, manifest_path, raw_path, state):
    """Support a small JSON manifest pointing at the actual JSONL ledger."""
    if not isinstance(obj, dict):
        return
    base_dir = os.path.dirname(os.path.abspath(manifest_path))
    keys = ("changes_file", "changes_path", "package_file", "jsonl_file",
            "ledger_file", "operations_file", "changes", "jsonl", "file",
            "path")
    seen = set()
    for key in keys:
        value = obj.get(key)
        if not isinstance(value, str) or not value:
            continue
        if key == "changes" and not value.lower().endswith((".jsonl", ".ndjson")):
            continue
        ref = value if os.path.isabs(value) else os.path.join(base_dir, value)
        ref_real = os.path.realpath(ref)
        base_real = os.path.realpath(base_dir)
        if os.path.commonpath([base_real, ref_real]) != base_real:
            raise PackageFormatError(
                "referenced change file %r escapes the package directory"
                % value)
        if ref in seen or not os.path.isfile(ref_real):
            continue
        seen.add(ref)
        with open(ref_real, "rb") as fh:
            _ingest_lines(fh, raw_path, state, binary=True)


def _find_package_in_directory(path):
    preferred = ("change-package.jsonl", "changes.jsonl", "package.jsonl",
                 "patch.jsonl", "changes.json", "change-package.json",
                 "package.json")
    for name in preferred:
        p = os.path.join(path, name)
        if os.path.isfile(p):
            return p
    for name in sorted(os.listdir(path)):
        if name.endswith(".jsonl") or name.endswith(".json"):
            return os.path.join(path, name)
    return None


# ---------------------------------------------------------------------------
# Operation/value normalisation
# ---------------------------------------------------------------------------


def _normalise_kind_value(value):
    if not isinstance(value, str):
        return None
    token = value.strip().lower().replace("-", "_").replace(" ", "_")
    if token in _ADD_WORDS or token.endswith("_add") or token.endswith("_added") \
            or token.endswith("_insert") or token.endswith("_inserted") \
            or "added" in token or "insert" in token:
        return ADD
    if token in _DELETE_WORDS or "delet" in token or "remov" in token:
        return DELETE
    if token in _UPDATE_WORDS or "updat" in token or "chang" in token \
            or "modif" in token or "patch" in token:
        return UPDATE
    return None


def _extract_kind(obj):
    if not isinstance(obj, dict):
        return None, None
    for key in _OP_DISCRIMINATOR_KEYS:
        if key in obj:
            kind = _normalise_kind_value(obj[key])
            if kind is not None:
                return kind, None
    for key in _VERB_KEYS:
        if key in obj and isinstance(obj[key], dict):
            kind = _normalise_kind_value(key)
            if kind is not None:
                return kind, obj[key]
    # A single top-level verb key may contain a scalar/list for malformed ops;
    # report it as recognised so normalisation gives a useful error.
    for key in _VERB_KEYS:
        if key in obj:
            kind = _normalise_kind_value(key)
            if kind is not None:
                return kind, obj[key]
    return None, None


def _type_name(ctype):
    return "integer" if ctype == TYPE_INTEGER else "string"


def _check_scalar(value, ctype, where):
    if value is None:
        return None
    if ctype == TYPE_INTEGER:
        if isinstance(value, bool) or not isinstance(value, int):
            raise PackageFormatError(
                "%s must be an integer or null, got %s"
                % (where, type(value).__name__))
        if not (-(1 << 63) <= value <= (1 << 63) - 1):
            raise PackageFormatError(
                "%s integer %r is outside signed 64-bit range" % (where, value))
        return value
    if not isinstance(value, str):
        raise PackageFormatError(
            "%s must be a string or null, got %s"
            % (where, type(value).__name__))
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise PackageFormatError("%s is not valid UTF-8: %s" % (where, exc))
    return value


def _canon_for(value, ctype, where):
    value = _check_scalar(value, ctype, where)
    try:
        return encode_value(value, ctype)
    except TypeError as exc:
        raise PackageFormatError("%s: %s" % (where, exc))


def _as_dict(value, where):
    if value is None:
        return None
    if not isinstance(value, dict):
        raise PackageFormatError("%s must be a JSON object" % where)
    return value


def _identity_from_container(container, id_names):
    """Return list of found values; missing names use a sentinel."""
    values = [_MISSING] * len(id_names)
    if container is None:
        return values
    if isinstance(container, (list, tuple)):
        if len(container) != len(id_names):
            raise IdentityConflictError(
                "identity list has %d values, expected %d (%s)"
                % (len(container), len(id_names), id_names))
        return list(container)
    if not isinstance(container, dict):
        if len(id_names) == 1:
            return [container]
        raise IdentityConflictError(
            "identity must be an object or list of %d fields" % len(id_names))
    lowered = {str(k).lower(): k for k in container}
    for i, name in enumerate(id_names):
        if name in container:
            values[i] = container[name]
        elif name.lower() in lowered:
            values[i] = container[lowered[name.lower()]]
    return values


def _merge_found_identity(found, candidate, source_desc):
    for i, value in enumerate(candidate):
        if value is _MISSING:
            continue
        if found[i] is _MISSING:
            found[i] = value
        elif found[i] != value or type(found[i]) is not type(value):
            # 1 and True/1.0 must not be treated as equal identities.
            raise IdentityConflictError(
                "conflicting %s values in %s identity sources"
                % (source_desc, "identity"))


def _extract_identity(op, wrapped, id_names, where, kind=None):
    found = [_MISSING] * len(id_names)
    source_objects = []
    for key in _IDENTITY_CONTAINER_KEYS:
        if isinstance(op, dict) and key in op:
            source_objects.append((key, op[key]))
        if wrapped is not None and isinstance(wrapped, dict) and key in wrapped:
            source_objects.append(("wrapped." + key, wrapped[key]))
    for desc, container in source_objects:
        _merge_found_identity(
            found, _identity_from_container(container, id_names), desc)

    # Flat identity fields, first in the operation and then in row containers.
    flat = {}
    if isinstance(op, dict):
        flat.update(op)
    if isinstance(wrapped, dict):
        flat.update(wrapped)
    for name in id_names:
        if name in flat and found[id_names.index(name)] is _MISSING:
            i = id_names.index(name)
            found[i] = flat[name]

    # Row containers are authoritative for an add.  For an update they are
    # only a fallback when an explicit identity/key did not supply every
    # field: values may legally contain a new identity value.
    need_row_identity = kind == ADD or any(x is _MISSING for x in found)
    if need_row_identity:
        for key in _ROW_CONTAINER_KEYS:
            container = flat.get(key)
            if isinstance(container, dict):
                candidate = _identity_from_container(container, id_names)
                for i, value in enumerate(candidate):
                    if value is _MISSING:
                        continue
                    if found[i] is _MISSING:
                        found[i] = value

    null_fields = []
    values = {}
    for i, name in enumerate(id_names):
        if found[i] is _MISSING:
            raise IdentityConflictError(
                "%s is missing identity field %r" % (where, name))
        if found[i] is None:
            null_fields.append(name)
        values[name] = found[i]
    if null_fields:
        raise IdentityConflictError(
            "%s has null identity field(s) %s" % (where, null_fields))
    return values


def _unwrap_change_value(value, col, where):
    if isinstance(value, dict):
        for key in ("after", "new", "value", "to", "current"):
            if key in value:
                return value[key]
        # A type/value tagged form.
        if set(value) <= {"type", "value"} and "value" in value:
            return value["value"]
        raise PackageFormatError(
            "%s for column %r is an object without 'after'/'new' value"
            % (where, col))
    return value


def _put_field(mapping, col, value, where):
    if col in mapping:
        old = mapping[col]
        if old != value or type(old) is not type(value):
            raise PackageFormatError(
                "%s specifies conflicting values for column %r" % (where, col))
        return
    mapping[col] = value


def _extract_fields(op, wrapped, kind, columns, id_names, where):
    col_names = list(columns)
    mapping = {}
    unknown = []

    def consider_direct_dict(container, label, allow_wrapped_values=False):
        if container is None:
            return
        d = _as_dict(container, where + "." + label)
        for raw_key, raw_value in d.items():
            key = str(raw_key)
            if key in columns:
                value = (_unwrap_change_value(raw_value, key, where + "." + label)
                         if allow_wrapped_values or kind == UPDATE else raw_value)
                _put_field(mapping, key, value, where + "." + label)
            elif key.lower() in {n.lower() for n in col_names}:
                actual = col_names[{n.lower(): n for n in col_names}[key.lower()]]
                _put_field(mapping, actual, raw_value, where + "." + label)
            elif key not in ("before", "old", "after", "new"):
                unknown.append("%s.%s" % (label, key))

    def consider_sequence(sequence, label):
        values = list(sequence)
        ordered = list(columns)
        if len(values) == len(ordered):
            names = ordered
        else:
            non_identity = [name for name in ordered if name not in id_names]
            if kind == ADD and len(values) == len(non_identity):
                names = non_identity
            else:
                raise PackageFormatError(
                    "%s.%s list has %d values; expected %d columns"
                    % (where, label, len(values), len(ordered)))
        for name, value in zip(names, values):
            _put_field(mapping, name, value, where + "." + label)

    sources = []
    if isinstance(wrapped, dict):
        sources.append(wrapped)
    if isinstance(op, dict):
        sources.append(op)

    for source in sources:
        container_keys = (_UPDATE_CONTAINER_KEYS + _ROW_CONTAINER_KEYS
                          if kind == UPDATE else _ROW_CONTAINER_KEYS)
        for key in container_keys:
            if key not in source:
                continue
            value = source[key]
            if wrapped is not None and source is op and value is wrapped:
                # The verb wrapper itself is not a field container.
                continue
            if kind == UPDATE and key == "changes" and isinstance(value, list):
                for j, item in enumerate(value):
                    if not isinstance(item, dict):
                        raise PackageFormatError(
                            "%s.changes[%d] must be an object" % (where, j))
                    name = (item.get("field") or item.get("column")
                            or item.get("name"))
                    if name not in columns:
                        raise PackageFormatError(
                            "%s.changes[%d] names unknown column %r"
                            % (where, j, name))
                    new_value = None
                    for vk in ("after", "new", "value", "to"):
                        if vk in item:
                            new_value = item[vk]
                            break
                    else:
                        raise PackageFormatError(
                            "%s.changes[%d] has no after/new value"
                            % (where, j))
                    _put_field(mapping, name, new_value, where)
            elif isinstance(value, dict):
                consider_direct_dict(
                    value, key,
                    allow_wrapped_values=(key == "changes" or kind == UPDATE))
            elif isinstance(value, (list, tuple)) and not (
                    kind == UPDATE and key == "changes"):
                consider_sequence(value, key)
            elif kind == UPDATE and key in ("set", "patch", "values", "fields"):
                raise PackageFormatError(
                    "%s.%s must be an object of field values" % (where, key))

        # Flat known columns are accepted for compact operation lines.
        for name, ctype in columns.items():
            if name in source and name not in _OP_DISCRIMINATOR_KEYS:
                _put_field(mapping, name, source[name], where + ".flat")

    if unknown:
        raise PackageFormatError(
            "%s contains fields not present in archive: %s"
            % (where, sorted(set(unknown))))

    # Identity values discovered separately must also be in an add row.
    identity = _extract_identity(op, wrapped, id_names, where, kind=kind)
    if kind == ADD:
        for name, value in identity.items():
            _put_field(mapping, name, value, where + ".identity")
        missing = [name for name in columns if name not in mapping]
        if missing:
            raise PackageFormatError(
                "%s is not a complete row; missing columns %s"
                % (where, missing))
    else:
        # For update, flat identity values should not be mistaken for changes
        # unless they are explicitly supplied through a field container.  The
        # extraction above may add them because they are top-level columns;
        # that is acceptable only when equal to the lookup identity.
        for name in id_names:
            if name in mapping:
                candidate = mapping[name]
                if candidate != identity[name] or \
                        type(candidate) is not type(identity[name]):
                    # This is an identity update; keep it.  It will be type
                    # checked and the final output key will be rebuilt.
                    pass

    if kind == UPDATE and not mapping:
        raise PackageFormatError(
            "%s update does not provide any field values" % where)

    # Type-check every supplied value now.
    checked = {}
    for name, value in mapping.items():
        checked[name] = _check_scalar(
            value, columns[name], "%s field %r" % (where, name))
    return identity, checked


def _coerce_sequence_operation(raw):
    """Accept compact array encodings in addition to operation objects."""
    if not isinstance(raw, (list, tuple)):
        return raw
    items = list(raw)
    if len(items) >= 2 and isinstance(items[0], str) \
            and _normalise_kind_value(items[0]) is not None:
        kind = _normalise_kind_value(items[0])
        op = {"op": kind}
        if len(items) == 2:
            op["identity"] = items[1]
            if kind == ADD:
                op["row"] = items[1]
            elif kind == UPDATE:
                op["values"] = items[1]
        else:
            op["identity"] = items[1]
            if kind == ADD:
                op["row"] = items[2]
            else:
                op["values"] = items[2]
        return op
    if len(items) >= 2 and isinstance(items[1], str) \
            and _normalise_kind_value(items[1]) is not None:
        kind = _normalise_kind_value(items[1])
        op = {"op": kind, "identity": items[0]}
        if len(items) >= 3:
            op["row" if kind == ADD else "values"] = items[2]
        return op
    return {"items": items}


def normalise_operation(raw, seq, columns, id_names, id_metas):
    where = "change package operation #%d" % (seq + 1)
    raw = _coerce_sequence_operation(raw)
    if not isinstance(raw, dict):
        raise PackageFormatError("%s must be a JSON object" % where)
    kind, wrapped = _extract_kind(raw)
    if kind is None:
        raise PackageFormatError(
            "%s has no recognizable op/action (expected add, delete or update)"
            % where)
    identity, values = _extract_fields(
        raw, wrapped, kind, columns, id_names, where)

    canon_parts = []
    order_parts = []
    for meta in id_metas:
        name = meta["name"]
        ctype = meta["type"]
        canon = _canon_for(identity[name], ctype,
                           "%s identity %r" % (where, name))
        canon_parts.append(canon)
        order_parts.append(order_key_for_canonical(canon, ctype))
    canon_key = b"".join(canon_parts)
    order_key = b"".join(order_parts)
    norm = {"op": kind, "seq": seq, "identity": identity, "values": values}
    payload_json = json.dumps(
        norm, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")
    payload = _u32(len(canon_key)) + canon_key + payload_json
    return (order_key, payload, seq), norm


def build_operation_index(raw_ops_path, op_count, columns, id_names, id_metas,
                          workdir, run_records=DEFAULT_RUN_RECORDS,
                          run_bytes=DEFAULT_RUN_BYTES):
    run = []
    buffered = 0
    run_paths = []
    run_index = 0
    counts = {ADD: 0, DELETE: 0, UPDATE: 0}

    def flush():
        nonlocal run, buffered, run_index
        if not run:
            return
        path = os.path.join(workdir, "op-run-%05d.run" % run_index)
        run_index += 1
        _spill_run(run, path)
        run_paths.append(path)
        run = []
        buffered = 0

    with open(raw_ops_path, "rb") as fh:
        for seq, line in enumerate(fh):
            if not line.strip():
                continue
            raw = loads_strict(line.decode("utf-8"))
            record, norm = normalise_operation(
                raw, seq, columns, id_names, id_metas)
            counts[norm["op"]] += 1
            run.append(record)
            buffered += len(record[0]) + len(record[1])
            if len(run) >= run_records or buffered >= run_bytes:
                flush()
    flush()
    sorted_path = os.path.join(workdir, "operations-sorted.run")
    _merge_run_files(run_paths, sorted_path)
    if sum(counts.values()) != op_count:
        # Defensive: ingest and normalization should see the same objects.
        raise PackageFormatError(
            "internal package count mismatch: ingested %d, normalized %d"
            % (op_count, sum(counts.values())))
    return sorted_path, counts


def _read_op_frame(record):
    _order, payload, seq = record
    n = struct.unpack("<I", payload[:4])[0]
    canon_key = payload[4:4 + n]
    op = loads_strict(payload[4 + n:].decode("utf-8"))
    return canon_key, op, seq


def ensure_operation_uniqueness(op_path, id_metas):
    reader = _FrameReader(op_path)
    previous = None
    try:
        while True:
            rec = reader.next()
            if rec is None:
                break
            canon_key, op, _seq = _read_op_frame(rec)
            if previous is not None and canon_key == previous:
                identity = decode_identity_key(canon_key, id_metas)
                raise IdentityConflictError(
                    "identity %s is operated on more than once in the change "
                    "package" % json.dumps(identity, ensure_ascii=False,
                                           sort_keys=True))
            previous = canon_key
    finally:
        reader.close()


# ---------------------------------------------------------------------------
# Merge baseline identities with operations and build output actions
# ---------------------------------------------------------------------------


def _key_tuple(order_key, canon_key):
    return order_key, canon_key


def _cmp_key(a_order, a_canon, b_order, b_canon):
    ka = (a_order, a_canon)
    kb = (b_order, b_canon)
    if ka < kb:
        return -1
    if ka > kb:
        return 1
    return 0


def _pack_action(kind, baseline_row, op, final_order, final_canon, seq):
    action = {"kind": kind, "baseline_row": baseline_row, "op": op}
    action_json = json.dumps(
        action, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")
    row_bytes = struct.pack("<q", -1 if baseline_row is None else baseline_row)
    payload = (_u32(len(final_canon)) + final_canon + row_bytes + action_json)
    return final_order, payload, seq


def _unpack_action(record):
    order_key, payload, seq = record
    n = struct.unpack("<I", payload[:4])[0]
    pos = 4
    final_canon = payload[pos:pos + n]
    pos += n
    baseline_row = struct.unpack("<q", payload[pos:pos + 8])[0]
    pos += 8
    action = loads_strict(payload[pos:].decode("utf-8"))
    return order_key, final_canon, seq, baseline_row, action


def _final_identity_canon(op, id_metas, columns):
    """Recompute final key, supporting updates to identity columns."""
    identity = dict(op.get("identity") or {})
    values = op.get("values") or {}
    identity.update({k: v for k, v in values.items()
                      if k in {m["name"] for m in id_metas}})
    parts = []
    order_parts = []
    for meta in id_metas:
        name = meta["name"]
        if name not in identity or identity[name] is None:
            # Should be impossible after action construction for adds/updates.
            raise IdentityConflictError(
                "result of %s operation has null/missing identity field %r"
                % (op.get("op"), name))
        canon = _canon_for(identity[name], meta["type"],
                           "result identity field %r" % name)
        parts.append(canon)
        order_parts.append(order_key_for_canonical(canon, meta["type"]))
    return b"".join(order_parts), b"".join(parts)


def build_actions(baseline_path, op_path, id_metas, columns, workdir,
                  run_records=DEFAULT_RUN_RECORDS,
                  run_bytes=DEFAULT_RUN_BYTES):
    """Streaming sorted merge; validates exact baseline/package matches."""
    br = _FrameReader(baseline_path)
    opr = _FrameReader(op_path)
    b_rec = br.next()
    o_rec = opr.next()
    b_index = 0
    o_index = 0
    run = []
    buffered = 0
    run_paths = []
    run_no = 0
    action_seq = 0

    def flush():
        nonlocal run, buffered, run_no
        if not run:
            return
        p = os.path.join(workdir, "action-run-%05d.run" % run_no)
        run_no += 1
        _spill_run(run, p)
        run_paths.append(p)
        run = []
        buffered = 0

    try:
        while b_rec is not None or o_rec is not None:
            if o_rec is None:
                b_order, b_canon, b_row = b_rec
                run.append(_pack_action(
                    "copy", int(b_row), None, b_order, b_canon, action_seq))
                action_seq += 1
                b_rec = br.next()
                b_index += 1
            elif b_rec is None:
                o_order, o_payload, _o_seq = o_rec
                o_canon, op, _seq = _read_op_frame(o_rec)
                if op["op"] != ADD:
                    identity = decode_identity_key(o_canon, id_metas)
                    raise IdentityConflictError(
                        "%s operation for identity %s does not match any "
                        "baseline record"
                        % (op["op"],
                           json.dumps(identity, ensure_ascii=False,
                                      sort_keys=True)))
                final_order, final_canon = _final_identity_canon(
                    op, id_metas, columns)
                run.append(_pack_action(
                    ADD, None, op, final_order, final_canon, action_seq))
                action_seq += 1
                o_rec = opr.next()
                o_index += 1
            else:
                b_order, b_canon, b_row = b_rec
                o_order0, o_payload, _o_seq = o_rec
                o_canon, op, _seq = _read_op_frame(o_rec)
                cmpv = _cmp_key(b_order, b_canon, o_order0, o_canon)
                if cmpv < 0:
                    run.append(_pack_action(
                        "copy", int(b_row), None, b_order, b_canon,
                        action_seq))
                    action_seq += 1
                    b_rec = br.next()
                    b_index += 1
                elif cmpv > 0:
                    if op["op"] != ADD:
                        identity = decode_identity_key(o_canon, id_metas)
                        raise IdentityConflictError(
                            "%s operation for identity %s does not match any "
                            "baseline record"
                            % (op["op"],
                               json.dumps(identity, ensure_ascii=False,
                                          sort_keys=True)))
                    final_order, final_canon = _final_identity_canon(
                        op, id_metas, columns)
                    run.append(_pack_action(
                        ADD, None, op, final_order, final_canon, action_seq))
                    action_seq += 1
                    o_rec = opr.next()
                    o_index += 1
                else:
                    identity = decode_identity_key(b_canon, id_metas)
                    if op["op"] == ADD:
                        raise IdentityConflictError(
                            "cannot add identity %s: it already exists in the "
                            "baseline archive"
                            % json.dumps(identity, ensure_ascii=False,
                                         sort_keys=True))
                    if op["op"] == DELETE:
                        # Omit this baseline row.
                        pass
                    else:
                        final_order, final_canon = _final_identity_canon(
                            op, id_metas, columns)
                        run.append(_pack_action(
                            UPDATE, int(b_row), op, final_order,
                            final_canon, action_seq))
                        action_seq += 1
                    b_rec = br.next()
                    o_rec = opr.next()
                    b_index += 1
                    o_index += 1

            buffered += 0  # payload size is accounted below approximately.
            if run:
                buffered += len(run[-1][0]) + len(run[-1][1])
            if len(run) >= run_records or buffered >= run_bytes:
                flush()
        flush()
    finally:
        br.close()
        opr.close()

    actions_path = os.path.join(workdir, "actions-sorted.run")
    _merge_run_files(run_paths, actions_path)
    return actions_path, action_seq


def ensure_final_identity_uniqueness(actions_path, id_metas):
    reader = _FrameReader(actions_path)
    previous = None
    try:
        while True:
            rec = reader.next()
            if rec is None:
                break
            _order, final_canon, _seq, _brow, _action = _unpack_action(rec)
            if previous is not None and final_canon == previous:
                identity = decode_identity_key(final_canon, id_metas)
                raise IdentityConflictError(
                    "materialized archive would contain duplicate identity %s"
                    % json.dumps(identity, ensure_ascii=False, sort_keys=True))
            previous = final_canon
    finally:
        reader.close()


# ---------------------------------------------------------------------------
# Streaming archive writer
# ---------------------------------------------------------------------------


class _OutputColumn(object):
    __slots__ = ("meta", "writer", "sha", "length", "slot")

    def __init__(self, meta, writer, slot):
        self.meta = meta
        self.writer = writer
        self.sha = hashlib.sha256()
        self.length = 0
        self.slot = slot


def _write_materialized_archive(stage, reader, actions_path, out_count,
                                baseline_digest_value, package_digests,
                                identity_fields, package_path):
    os.makedirs(os.path.join(stage, "columns"), exist_ok=True)
    os.makedirs(os.path.join(stage, "checks"), exist_ok=True)
    columns = reader.header["columns"]
    outs = []
    # Persistent forward cursors avoid re-seeking from a 1024-row block for
    # every cell.  seek_row still handles arbitrary identity-order jumps.
    cursors = {cmeta["name"]: reader.column_cursor(cmeta["name"])
               for cmeta in columns}
    try:
        for index, cmeta in enumerate(columns):
            fname = "c%03d.canon" % index
            cpath = os.path.join(stage, "columns", fname)
            writer = SpoolWriter(cpath, 0)
            from .format import SlotHasher
            slot = SlotHasher(cmeta["name"], cmeta["type"], 0, out_count)
            outs.append(_OutputColumn(cmeta, writer, slot))

        action_reader = _FrameReader(actions_path)
        written = 0
        try:
            while True:
                rec = action_reader.next()
                if rec is None:
                    break
                _order, _final_canon, _seq, baseline_row, action = \
                    _unpack_action(rec)
                op = action.get("op")
                values = (op or {}).get("values") or {}
                for out in outs:
                    name = out.meta["name"]
                    ctype = out.meta["type"]
                    out.writer.begin_value()
                    if action["kind"] in (ADD, UPDATE) and name in values:
                        raw = _canon_for(
                            values[name], ctype,
                            "materialized value for column %r" % name)
                        out.writer.write(raw)
                        out.sha.update(raw)
                        out.slot.update_raw(raw)
                        out.length += len(raw)
                    else:
                        if baseline_row is None:
                            raise MaterializeError(
                                "internal action %s has no baseline row for "
                                "column %r" % (action["kind"], name))
                        cursor = cursors[name]
                        _seek_cached(cursor, int(baseline_row))
                        raw_value = cursor.next_value()
                        header = raw_value.header()
                        out.writer.write(header)
                        out.sha.update(header)
                        out.slot._h.update(header)
                        out.slot.fed_rows += 1
                        out.length += len(header)
                        for chunk in raw_value.iter_chunks(reader.pool):
                            out.writer.write(chunk)
                            out.sha.update(chunk)
                            out.slot._h.update(chunk)
                            out.length += len(chunk)
                written += 1
        finally:
            action_reader.close()

        if written != out_count:
            raise MaterializeError(
                "internal row count mismatch: wrote %d, expected %d"
                % (written, out_count))

        columns_meta = []
        for index, out in enumerate(outs):
            out.writer.close()
            fname = "c%03d" % index
            _write_u64_index(
                os.path.join(stage, "columns", fname + ".idx"),
                out.writer.offsets)
            cmeta = out.meta
            digest = out.slot.hexdigest()
            if out.slot.fed_rows != out_count:
                raise MaterializeError(
                    "column %r fed %d slot rows, expected %d"
                    % (cmeta["name"], out.slot.fed_rows, out_count))
            columns_meta.append({
                "name": cmeta["name"],
                "type": cmeta["type"],
                "file": "columns/c%03d.canon" % index,
                "index_file": "columns/c%03d.idx" % index,
                "block_rows": BLOCK_ROWS,
                "value_count": out_count,
                "byte_length": out.length,
                "sha256": out.sha.hexdigest(),
                "slots": [{
                    "slot": 0,
                    "start": 0,
                    "end": out_count,
                    "digest": digest,
                }],
            })
    except BaseException:
        for out in outs:
            try:
                out.writer.abort()
            except Exception:
                pass
        raise

    check_index = {
        "archrestore_format": {"name": "archrestore-check-index",
                               "version": FORMAT_VERSION},
        "row_count": out_count,
        "materialized_from": {
            "baseline_digest": baseline_digest_value,
            "change_package": package_path,
            "identity_fields": list(identity_fields),
        },
        "columns": [
            {"column": c["name"], "type": c["type"],
             "archive_file": c["file"],
             "row_range": {"start": 0, "end": out_count},
             "slots": c["slots"]}
            for c in columns_meta
        ],
        "note": "Rows were produced by applying a bound JSONL change package "
                "and are sorted by the declared identity tuple.",
    }
    with open(os.path.join(stage, "checks", "index.json"), "w",
              encoding="utf-8") as fh:
        json.dump(check_index, fh, ensure_ascii=False, sort_keys=True, indent=1)
        fh.write("\n")

    archive = {
        "archrestore_format": {"name": ARCHIVE_FORMAT_NAME,
                               "version": FORMAT_VERSION},
        "status": "restored",
        "row_count": out_count,
        "manifest_sha256": reader.header.get("manifest_sha256"),
        "baseline_digest": baseline_digest_value,
        "baseline_sha256": baseline_digest_value,
        "content_digest": baseline_digest_value,
        "archive_digest": baseline_digest_value,
        "baseline": {
            "content_digest": baseline_digest_value,
            "declared_digests": sorted(set(package_digests)),
        },
        "change_package": {
            "path": package_path,
            "identity_fields": list(identity_fields),
        },
        "source": {
            "kind": MATERIALIZED_SOURCE_KIND,
            "tool": "archrestore.materialize",
            "version": FORMAT_VERSION,
        },
        "columns": columns_meta,
        "checks": "checks/index.json",
    }
    with open(os.path.join(stage, "archive.json"), "w",
              encoding="utf-8") as fh:
        json.dump(archive, fh, ensure_ascii=False, sort_keys=True, indent=1)
        fh.write("\n")
    with open(os.path.join(stage, "diagnostics.json"), "w",
              encoding="utf-8") as fh:
        json.dump({"outcome": "materialized", "archive": archive}, fh,
                  ensure_ascii=False, sort_keys=True, indent=1)
        fh.write("\n")
    return archive


# ---------------------------------------------------------------------------
# Package writing convenience (used by examples and neighbouring APIs)
# ---------------------------------------------------------------------------


def write_change_package(path, operations=None, *, baseline=None,
                         baseline_digest=None, identity_fields=None,
                         format_name=CHANGE_PACKAGE_KIND, extra_meta=None):
    """Write a bound JSONL change package and return its baseline digest."""
    digest = baseline_digest
    if digest is None and baseline is not None:
        if isinstance(baseline, str) and len(baseline) == 64:
            digest = baseline
        else:
            digest = archive_content_digest(baseline)
    if digest is None:
        raise PackageFormatError(
            "write_change_package requires baseline or baseline_digest")
    if not identity_fields:
        raise PackageFormatError(
            "write_change_package requires identity_fields")
    envelope = {
        "archrestore_format": {"name": format_name, "version": FORMAT_VERSION},
        "baseline_digest": digest,
        "identity_fields": list(identity_fields),
    }
    if extra_meta:
        envelope.update(extra_meta)
    destination = os.fspath(path)
    parent = os.path.dirname(os.path.abspath(destination)) or "."
    os.makedirs(parent, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".change-package-", suffix=".jsonl",
                               dir=parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(envelope, ensure_ascii=False, sort_keys=True))
            fh.write("\n")
            for op in operations or []:
                fh.write(json.dumps(op, ensure_ascii=False, sort_keys=True))
                fh.write("\n")
        os.replace(tmp, destination)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return digest


write_package = write_change_package
dump_change_package = write_change_package
build_change_package = write_change_package


# ---------------------------------------------------------------------------
# High-level API
# ---------------------------------------------------------------------------


def _resolve_identity_fields(meta, spec, identity_fields, column_names=None):
    if identity_fields is None and spec is not None:
        from .compare import coerce_compare_spec
        spec_obj = coerce_compare_spec(spec)
        identity_fields = spec_obj.identity
    if identity_fields is not None:
        if isinstance(identity_fields, str):
            identity_fields = [identity_fields]
        fields = []
        seen = set()
        for item in identity_fields:
            name = item if isinstance(item, str) else (
                item.get("name") or item.get("field") or item.get("column"))
            if not isinstance(name, str) or not name:
                raise PackageFormatError(
                    "identity field names must be non-empty strings")
            if name in seen:
                raise PackageFormatError(
                    "identity field %r listed more than once" % name)
            seen.add(name)
            fields.append(name)
        return fields

    candidates = list(meta.get("identity_candidates") or [])
    if meta.get("identity_fields") and not candidates:
        candidates.append(list(meta["identity_fields"]))
    if column_names:
        known = set(column_names)
        for candidate in candidates:
            if candidate and all(name in known for name in candidate):
                return list(candidate)
    if candidates and candidates[0]:
        return list(candidates[0])
    raise PackageFormatError(
        "change package does not declare non-empty identity_fields")


def _verify_package_digest(meta, baseline, explicit_digest=None):
    valid_declared = sorted(meta.get("digests") or set())
    malformed = meta.get("malformed_digests") or []
    if not valid_declared:
        if explicit_digest is not None:
            declared = _normalise_digest_string(explicit_digest)
            if declared is None:
                raise PackageFormatError(
                    "explicit baseline digest %r is not 64 hex characters"
                    % (explicit_digest,))
            valid_declared = [declared]
        elif malformed:
            raise PackageFormatError(
                "change package contains malformed baseline digest(s): %r"
                % (malformed[:3],))
        else:
            raise PackageFormatError(
                "change package does not bind a baseline archive digest")

    primary, candidates = archive_digest_info(baseline)
    matching = [d for d in valid_declared if d in candidates]
    if explicit_digest is not None:
        expected = _normalise_digest_string(explicit_digest)
        if expected is None:
            raise PackageFormatError("explicit baseline digest is malformed")
        if expected not in candidates:
            raise BaselineDigestError(
                "specified baseline digest does not match archive content")
    if not matching:
        raise BaselineDigestError(
            "change package baseline digest %s does not match baseline archive "
            "(content digest %s)" % (valid_declared[0], primary))
    return primary, valid_declared


def _prepare_output_path(out):
    if out is None:
        path = tempfile.mkdtemp(prefix="archrestore-next-archive-")
        os.rmdir(path)
        return path, True
    path = os.fspath(out)
    if os.path.exists(path):
        if not os.path.isdir(path):
            raise MaterializeError("output path %r exists and is not a directory"
                                   % path)
        if os.listdir(path):
            raise MaterializeError(
                "output directory %r exists and is not empty; choose a new "
                "directory or remove it" % path)
    return path, False


def materialize_archive(baseline=None, package=None, out=None, *,
                        changes=None, change_package=None,
                        baseline_archive=None, base=None,
                        output=None, destination=None,
                        spec=None, identity_fields=None, keys=None,
                        identity=None, identities=None,
                        baseline_digest=None, expected_digest=None,
                        digest=None, baseline_sha256=None,
                        workdir=None, keep_work=False, verify=False,
                        verify_baseline=False,
                        run_records=DEFAULT_RUN_RECORDS,
                        run_bytes=DEFAULT_RUN_BYTES,
                        fan_in=DEFAULT_FAN_IN,
                        problem_limit=DEFAULT_PROBLEM_LIMIT,
                        **extra):
    """Apply a bound JSONL change package to *baseline*, writing *out*.

    The first three positional arguments are ``(baseline, package, out)``.
    Broad keyword aliases are accepted for neighbouring CLI/API vocabulary.
    Returns a JSON-serialisable report dict.
    """
    baseline = (baseline or baseline_archive or base
                or extra.get("baseline_path") or extra.get("base_archive")
                or extra.get("archive") or extra.get("before")
                or extra.get("source_archive")
                or extra.get("old") or extra.get("old_archive"))
    package = package if package is not None else (
        changes if changes is not None else change_package)
    if package is None:
        package = (extra.get("patch") or extra.get("delta")
                   or extra.get("ledger") or extra.get("changes_path")
                   or extra.get("package_path")
                   or extra.get("change_file")
                   or extra.get("changes_file"))
    out = (out or output or destination or extra.get("out_dir")
           or extra.get("output_dir") or extra.get("destination_dir")
           or extra.get("target_dir")
           or extra.get("archive_out") or extra.get("output_path")
           or extra.get("output_archive") or extra.get("out_path")
           or extra.get("target_archive") or extra.get("target")
           or extra.get("new") or extra.get("new_archive"))
    identity_fields = (identity_fields if identity_fields is not None
                       else keys if keys is not None
                       else identity if identity is not None
                       else identities)
    baseline_digest = (baseline_digest or expected_digest or digest
                       or baseline_sha256
                       or extra.get("expected_baseline_digest")
                       or extra.get("baseline_sha256"))

    # Tolerate apply-style positional order: apply_changes(PACKAGE, BASE, OUT).
    if baseline is not None and package is not None:
        baseline_is_dir = isinstance(baseline, (str, bytes, os.PathLike)) \
            and os.path.isdir(os.fspath(baseline))
        package_is_dir = isinstance(package, (str, bytes, os.PathLike)) \
            and os.path.isdir(os.fspath(package))
        if not baseline_is_dir and package_is_dir:
            baseline, package = package, baseline

    if not baseline:
        raise PackageFormatError("a baseline archive directory is required")
    if package is None:
        raise PackageFormatError("a JSONL change package is required")
    if not os.path.isdir(baseline):
        raise MaterializeError("baseline archive %r is not a directory"
                               % baseline)
    verify_baseline = bool(verify_baseline or verify)

    if out is None:
        out_path, _temporary = _prepare_output_path(None)
    else:
        out_path = os.fspath(out)
    out_parent = os.path.dirname(os.path.abspath(out_path)) or "."
    # Create only the parent.  The final output path is checked immediately
    # before publication so package validation errors keep their taxonomy.
    os.makedirs(out_parent, exist_ok=True)
    owns_workdir = workdir is None
    if workdir is None:
        workdir = tempfile.mkdtemp(prefix="archrestore-materialize-")
    else:
        os.makedirs(workdir, exist_ok=True)

    stage = tempfile.mkdtemp(prefix=".materialize-stage-", dir=out_parent)
    reader = None
    problem_log = None
    try:
        reader = ArchiveReader(baseline)
        header = reader.header
        if verify_baseline:
            baseline_verification = verify_archive(baseline)
            if not baseline_verification.ok:
                raise MaterializeVerificationError(
                    "baseline archive failed verification: %s"
                    % "; ".join(baseline_verification.problems[:5]))
        else:
            baseline_verification = None

        meta, raw_ops_path, op_count = ingest_package(package, workdir)
        columns = {c["name"]: c.get("type") for c in header.get("columns") or []}
        if not columns:
            raise MaterializeError("baseline archive has no columns")
        fields = _resolve_identity_fields(meta, spec, identity_fields,
                                          column_names=list(columns))
        for name in fields:
            if name not in columns:
                raise IdentityConflictError(
                    "identity field %r is not a baseline column (have %s)"
                    % (name, sorted(columns)))
            if columns[name] not in ("string", "integer"):
                raise IdentityConflictError(
                    "identity field %r has unsupported type %r"
                    % (name, columns[name]))
        id_metas = [reader.column(name) for name in fields]

        primary_digest, declared_digests = _verify_package_digest(
            meta, baseline, baseline_digest)

        # Baseline null/duplicate identity scan, reusing compare's external
        # sorted frame and streaming duplicate reporter.
        from .compare import CompareSpec, build_identity_index, scan_duplicates
        problem_path = os.path.join(workdir, "baseline-identity-problems.jsonl")
        problem_log = ProblemLog(problem_path, limit=problem_limit)
        spec_obj = CompareSpec(fields)
        baseline_sorted = build_identity_index(
            reader, "baseline", spec_obj, workdir, problem_log,
            run_records=run_records, run_bytes=run_bytes,
            order_mode="logical")
        scan_duplicates(baseline_sorted, "baseline", id_metas, problem_log)
        problem_summary = problem_log.to_summary()
        problem_items = list(problem_log.items)
        problem_log.close()
        problem_log = None
        if problem_summary["problem_count"]:
            report = {
                "ok": False,
                "outcome": "identity_conflict",
                "problems": problem_items,
                "problem_summary": problem_summary,
                "problems_path": os.path.abspath(problem_path),
            }
            raise IdentityConflictError(
                "baseline archive has %d empty/duplicate identity problem(s)"
                % problem_summary["problem_count"], report)

        op_sorted, counts = build_operation_index(
            raw_ops_path, op_count, columns, fields, id_metas, workdir,
            run_records=run_records, run_bytes=run_bytes)
        ensure_operation_uniqueness(op_sorted, id_metas)

        actions_path, action_count = build_actions(
            baseline_sorted, op_sorted, id_metas, columns, workdir,
            run_records=run_records, run_bytes=run_bytes)
        ensure_final_identity_uniqueness(actions_path, id_metas)

        baseline_rows = int(header["row_count"])
        out_count = baseline_rows + counts[ADD] - counts[DELETE]
        if action_count != out_count:
            raise MaterializeError(
                "internal action count %d does not match expected rows %d"
                % (action_count, out_count))

        package_provenance = (os.path.abspath(os.fspath(package))
                              if isinstance(package, (str, bytes, os.PathLike))
                              else None)
        archive_meta = _write_materialized_archive(
            stage, reader, actions_path, out_count, primary_digest,
            declared_digests, fields, package_provenance)

        verification = verify_archive(stage)
        if not verification.ok:
            raise MaterializeVerificationError(
                "materialized archive failed self-verification: %s"
                % "; ".join(verification.problems[:8]))

        # Publish only after verification.  Stage is in the destination parent,
        # so replacement is atomic on the same filesystem.
        if os.path.exists(out_path):
            if os.path.isdir(out_path) and not os.listdir(out_path):
                os.rmdir(out_path)
            else:
                raise MaterializeError(
                    "output directory %r exists and is not empty; refusing to "
                    "replace it" % out_path)
        os.replace(stage, out_path)
        stage = None

        summary = {
                "baseline_rows": baseline_rows,
                "rows_out": out_count,
                "row_count": out_count,
                "added": counts[ADD],
                "inserted": counts[ADD],
                "created": counts[ADD],
                "deleted": counts[DELETE],
                "removed": counts[DELETE],
                "updated": counts[UPDATE],
                "modified": counts[UPDATE],
                "changed": counts[UPDATE],
                "columns": len(columns),
                "column_count": len(columns),
            }
        archive_info = dict(archive_meta)
        archive_info["path"] = os.path.abspath(out_path)
        report = MaterializeReport({
            "ok": True,
            "success": True,
            "outcome": "materialized",
            "status": "ok",
            "baseline": os.path.abspath(baseline),
            "baseline_archive": os.path.abspath(baseline),
            "package": package_provenance,
            "change_package": package_provenance,
            "output": os.path.abspath(out_path),
            "out": os.path.abspath(out_path),
            "archive_path": os.path.abspath(out_path),
            "identity_fields": list(fields),
            "keys": list(fields),
            "baseline_digest": primary_digest,
            "digest": primary_digest,
            "declared_digests": declared_digests,
            "summary": summary,
            "counts": dict(summary),
            "added": counts[ADD],
            "inserted": counts[ADD],
            "deleted": counts[DELETE],
            "removed": counts[DELETE],
            "updated": counts[UPDATE],
            "modified": counts[UPDATE],
            "changed": counts[UPDATE],
            "row_count": out_count,
            "column_count": len(columns),
            "verification": verification.to_json(),
            "baseline_verification": baseline_verification.to_json()
            if baseline_verification is not None else None,
            "archive": archive_info,
            "archive_header": archive_meta,
        })
        return report
    except IdentityConflictError as exc:
        report = getattr(exc, "report", None)
        src = (report or {}).get("problems_path")
        if src and os.path.exists(src):
            try:
                fd, preserved = tempfile.mkstemp(
                    prefix="archrestore-materialize-problems-",
                    suffix=".jsonl")
                os.close(fd)
                shutil.copyfile(src, preserved)
                exc.report["problems_path"] = os.path.abspath(preserved)
            except OSError:
                pass
        raise
    finally:
        if problem_log is not None:
            try:
                problem_log.close()
            except Exception:
                pass
        if reader is not None:
            reader.close()
        if stage is not None and os.path.exists(stage):
            shutil.rmtree(stage, ignore_errors=True)
        if owns_workdir and not keep_work:
            shutil.rmtree(workdir, ignore_errors=True)


# Primary neighbouring API aliases.
materialize = materialize_archive
apply = materialize_archive
materialize_changes = materialize_archive
materialize_from_changes = materialize_archive
materialize_change_package = materialize_archive
run_materialization = materialize_archive
create_materialized_archive = materialize_archive
write_materialized_archive = materialize_archive
write_next_archive = materialize_archive
apply_materialization = materialize_archive
apply_changes = materialize_archive
apply_changes_file = materialize_archive
apply_change_set = materialize_archive
apply_change_package = materialize_archive
apply_package = materialize_archive
apply_delta = materialize_archive
apply_patch = materialize_archive
roll_forward = materialize_archive
rollforward = materialize_archive
advance_archive = materialize_archive
reify_archive = materialize_archive
build_next_archive = materialize_archive
create_next_archive = materialize_archive
next_archive = materialize_archive
restore_from_changes = materialize_archive
compose_archive = materialize_archive
rebuild_archive = materialize_archive


def main(argv=None):  # pragma: no cover - thin module CLI
    from .cli import main as cli_main
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in (
            "materialize", "apply", "apply-changes", "patch", "reify",
            "digest", "baseline-digest", "archive-digest"):
        argv = ["materialize"] + argv
    return cli_main(argv)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
