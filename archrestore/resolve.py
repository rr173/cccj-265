"""Evidence resolution and per-slot confirmation.

Contract (see docs/CONTRACT.md)
------------------------------
* Evidence is keyed by *logical row coordinate*, never by page boundary.
  A manifest slot may be satisfied by one page, by a sub-range of a page, or
  by several decoded sub-ranges stitched across replicas.
* A row covered by evidence with two or more distinct decoded values is
  **disputed**.  We do not search for a "correct" subset: there is no
  per-row authority in this format, so a disputed row blocks its slot.
* A row with no usable covering evidence is **missing**.
* Only rows that are neither missing nor disputed are **confirmed**.
* A slot is:
    - ``digest_mismatch`` if every row is confirmed but the recomputed slot
      digest differs from the manifest;
    - ``disputed`` if it has any disputed row (missing rows, if any, are
      reported too);
    - ``missing`` if it has missing rows and no disputed rows;
    - ``confirmed`` only if every row is confirmed and the digest matches.
  Digest mismatch is reported separately from missing/disputed.
* Cross-page overlap is duplicate evidence only: the restored archive holds
  exactly one row per absolute row index.

Memory: the sweep keeps O(active pages) deltas and O(1) hasher state; values
are streamed.  Each page was decoded once into a block-indexed spill; a
reader seeks by block, never re-opening or re-decoding the source page.
"""

import hashlib
import json
from bisect import bisect_right

from .format import SlotHasher

STATUS_CONFIRMED = "confirmed"
STATUS_MISSING = "missing"
STATUS_DISPUTED = "disputed"
STATUS_DIGEST_MISMATCH = "digest_mismatch"

MAX_DISPUTE_SAMPLES = 8
MAX_WITNESSES = 8
MAX_EVIDENCE_SPANS = 64
RENDER_LIMIT = 256


class Piece(object):
    """One decoded page as evidence over logical rows [start, end)."""

    __slots__ = ("spool", "page", "replica_id")

    def __init__(self, spool, page):
        self.spool = spool
        self.page = page
        self.replica_id = page.replica_id

    @property
    def start(self):
        return self.page.start

    @property
    def end(self):
        return self.page.end

    def describe(self):
        p = self.page
        d = {
            "replica_id": self.replica_id,
            "page": p.page,
            "encoding": p.encoding,
            "start": p.start,
            "end": p.end,
            "page_byte_length": p.byte_length,
            "page_byte_digest": p.byte_digest,
            "location": p.location,
        }
        if p.dict_ref is not None:
            d["dict"] = {
                "dict_id": p.dict_ref.dict_id,
                "dict_digest": p.dict_ref.digest,
                "dict_byte_length": p.dict_ref.byte_length,
                "dict_location": p.dict_ref.location,
            }
        return d


class SlotDiagnostics(object):
    __slots__ = ("slot", "column", "start", "end", "status",
                 "missing_rows", "disputed_rows", "missing_ranges",
                 "disputes", "expected_digest", "computed_digest",
                 "evidence", "witnesses", "evidence_truncated", "dict_digests")

    def __init__(self, slot):
        self.slot = slot
        self.column = slot.column
        self.start = slot.start
        self.end = slot.end
        self.status = None
        self.missing_rows = 0
        self.disputed_rows = 0
        self.missing_ranges = []
        self.disputes = []
        self.expected_digest = slot.digest
        self.computed_digest = None
        self.evidence = []
        self.witnesses = []
        self.evidence_truncated = False
        self.dict_digests = []

    @property
    def row_count(self):
        return self.end - self.start

    @property
    def confirmed(self):
        return self.status == STATUS_CONFIRMED

    def to_json(self):
        return {
            "column": self.column,
            "slot": self.slot.slot,
            "start": self.start,
            "end": self.end,
            "row_count": self.row_count,
            "status": self.status,
            "missing_rows": self.missing_rows,
            "disputed_rows": self.disputed_rows,
            "missing_ranges": [{"start": a, "end": b}
                               for a, b in self.missing_ranges],
            "disputes": self.disputes,
            "expected_digest": self.expected_digest,
            "computed_digest": self.computed_digest,
            "witnesses": self.witnesses,
            "evidence": self.evidence,
            "evidence_truncated": self.evidence_truncated,
            "dict_digests": self.dict_digests,
        }


class ColumnResolution(object):
    __slots__ = ("column", "slots", "pieces", "coverage_gaps",
                 "rows_confirmed", "rows_missing", "rows_disputed",
                 "stream_complete", "unique_spill", "unique_path")

    def __init__(self, column):
        self.column = column
        self.slots = []
        self.pieces = []
        self.coverage_gaps = []
        self.rows_confirmed = 0
        self.rows_missing = 0
        self.rows_disputed = 0
        self.stream_complete = False
        self.unique_spill = None
        self.unique_path = None

    @property
    def all_confirmed(self):
        # Vacuously true for a zero-row column, which the manifest permits
        # only when the whole dataset has row_count 0.
        return all(s.confirmed for s in self.slots)

    def to_json(self):
        return {
            "column": self.column.name,
            "type": self.column.type,
            "all_slots_confirmed": self.all_confirmed,
            "rows_confirmed": self.rows_confirmed,
            "rows_missing": self.rows_missing,
            "rows_disputed": self.rows_disputed,
            "coverage_gaps_from_replicas": [
                {"start": a, "end": b} for a, b in self.coverage_gaps],
            "slots": [s.to_json() for s in self.slots],
        }


def merge_intervals(intervals):
    if not intervals:
        return []
    intervals = sorted(intervals)
    out = [list(intervals[0])]
    for a, b in intervals[1:]:
        if a <= out[-1][1]:
            if b > out[-1][1]:
                out[-1][1] = b
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


def complement(merged, start, end):
    out = []
    cursor = start
    for a, b in merged:
        if b <= cursor:
            continue
        if a >= end:
            break
        if a > cursor:
            out.append((cursor, a))
        cursor = max(cursor, b)
        if cursor >= end:
            return out
    if cursor < end:
        out.append((cursor, end))
    return out


def _render_raw(raw, pool):
    """Bounded canonical text for diagnostics.  Never loads a huge value."""
    if raw.tag == 0:
        return "null"
    if raw.tag == 2:
        return str(int.from_bytes(raw.inline, "little", signed=True))
    h = hashlib.sha256()
    preview = bytearray()
    total = 0
    for chunk in raw.iter_chunks(pool):
        h.update(chunk)
        total += len(chunk)
        if len(preview) < RENDER_LIMIT:
            preview += chunk[:RENDER_LIMIT - len(preview)]
    if total <= RENDER_LIMIT:
        return json.dumps(preview.decode("utf-8"), ensure_ascii=False)
    return json.dumps("<%d-byte string sha256:%s preview=%s>"
                      % (total, h.hexdigest()[:16],
                         preview.decode("utf-8", "ignore")),
                      ensure_ascii=False)


def _values_equal(a, b, pool):
    return a.equals(b, pool)


def resolve_column(column, pieces, slots, pool, unique_writer):
    """Resolve one column.

    ``pieces`` are decoded pages (each already decoded exactly once).
    ``slots`` tile [0, manifest.row_count) for this column.
    ``unique_writer`` receives exactly one canonical value per resolved row,
    in ascending row order, and is only trusted when the whole column is
    confirmed.
    """
    result = ColumnResolution(column)
    result.pieces = sorted(pieces, key=lambda p: (p.start, p.end,
                                                  p.replica_id, p.page.page))
    diags = [SlotDiagnostics(s) for s in slots]
    result.slots = diags

    merged_cov = merge_intervals([(p.start, p.end) for p in result.pieces])
    row_count = slots[-1].end if slots else 0
    result.coverage_gaps = complement(merged_cov, 0, row_count)

    if not result.pieces:
        for d in diags:
            d.status = STATUS_MISSING
            d.missing_rows = d.row_count
            d.missing_ranges = [(d.start, d.end)]
        result.rows_missing = row_count
        result.stream_complete = (row_count == 0)
        return result

    starts_at = {}
    ends_at = {}
    for p in result.pieces:
        starts_at.setdefault(p.start, []).append(p)
        ends_at.setdefault(p.end, []).append(p)
    bounds = sorted({0, row_count} |
                    set(starts_at) | set(ends_at) |
                    {s.start for s in slots} | {s.end for s in slots})
    slot_starts = [s.start for s in slots]
    hashers = [None] * len(diags)
    readers = {}  # piece -> SpoolReader, reused across intervals

    active = {}
    for bi in range(len(bounds) - 1):
        a = bounds[bi]
        b = bounds[bi + 1]
        if a >= row_count:
            break
        # Refresh active set for coordinate a.
        for p in list(active):
            if p.end <= a:
                del active[p]
        for p in starts_at.get(a, ()):
            if p.end > a:
                active[p] = True
        if not active:
            continue
        si = bisect_right(slot_starts, a) - 1
        if si < 0 or si >= len(diags):
            continue
        diag = diags[si]
        slot = slots[si]
        seg_start = max(a, slot.start)
        seg_end = min(b, slot.end)
        if seg_start >= seg_end:
            continue
        active_pieces = sorted(active, key=lambda p: (p.replica_id, p.page.page))
        # Acquire / position readers.
        piece_readers = []
        for p in active_pieces:
            r = readers.get(p)
            if r is None:
                r = p.spool.reader(pool)
                readers[p] = r
            if r.row != seg_start:
                r.seek_row(seg_start)
            piece_readers.append((p, r))

        if hashers[si] is None:
            hashers[si] = SlotHasher(column.name, column.type,
                                     slot.start, slot.end)

        ev_start = seg_start
        ev_sources = [p.describe() for p in active_pieces]
        for row in range(seg_start, seg_end):
            values = [r.next_value() for _p, r in piece_readers]
            agreed = _agree(values, pool)
            if agreed is None:
                diag.disputed_rows += 1
                if len(diag.disputes) < MAX_DISPUTE_SAMPLES:
                    diag.disputes.append(
                        _describe_dispute(row, values, piece_readers, pool))
                continue
            hashers[si].update_stream(agreed.header(),
                                      agreed.iter_chunks(pool))
            unique_writer.begin_value()
            agreed.copy_to(unique_writer, pool)
        _merge_evidence(diag, ev_start, seg_end, ev_sources)
        for p in active_pieces:
            d = p.describe()
            if d not in diag.witnesses and len(diag.witnesses) < MAX_WITNESSES:
                diag.witnesses.append(d)
            if p.page.dict_ref is not None:
                dd = p.page.dict_ref.digest
                if dd not in diag.dict_digests:
                    diag.dict_digests.append(dd)

    # Missing rows and final slot statuses.
    for si, d in enumerate(diags):
        gaps = complement(merged_cov, d.start, d.end)
        d.missing_ranges = gaps
        d.missing_rows = sum(b - a for a, b in gaps)
        h = hashers[si]
        if d.disputed_rows:
            # Dispute dominates the status, but missing rows stay reported.
            d.status = STATUS_DISPUTED
            d.computed_digest = None
        elif d.missing_rows:
            d.status = STATUS_MISSING
            d.computed_digest = None
        elif h is None:  # pragma: no cover - defensive
            d.status = STATUS_MISSING
            d.missing_rows = d.row_count
            d.missing_ranges = [(d.start, d.end)]
        else:
            computed = h.hexdigest()
            d.computed_digest = computed
            d.status = (STATUS_CONFIRMED if computed == d.expected_digest
                        else STATUS_DIGEST_MISMATCH)

    result.rows_confirmed = sum(d.row_count - d.missing_rows - d.disputed_rows
                                for d in diags)
    result.rows_missing = sum(d.missing_rows for d in diags)
    result.rows_disputed = sum(d.disputed_rows for d in diags)
    result.stream_complete = (result.rows_missing == 0 and
                              result.rows_disputed == 0 and
                              result.rows_confirmed == row_count)
    return result


def _agree(values, pool):
    first = values[0]
    for v in values[1:]:
        if not _values_equal(first, v, pool):
            return None
    return first


def _describe_dispute(row, values, piece_readers, pool):
    # Group by *actual value equality*, not by bounded rendering, so two
    # distinct huge strings can never be folded into one variant.
    variants = []  # list of (representative RawValue, entry dict)
    for (piece, _reader), value in zip(piece_readers, values):
        source = {"replica_id": piece.replica_id, "page": piece.page.page,
                  "start": piece.start, "end": piece.end,
                  "encoding": piece.page.encoding}
        for representative, entry in variants:
            if _values_equal(representative, value, pool):
                entry["sources"].append(source)
                break
        else:
            entry = {"value": _render_raw(value, pool), "sources": [source]}
            variants.append((value, entry))
    return {"row": row, "variants": [entry for _v, entry in variants]}


def _merge_evidence(diag, start, end, sources):
    if diag.evidence and diag.evidence[-1]["end"] == start and \
            diag.evidence[-1]["sources"] == sources:
        diag.evidence[-1]["end"] = end
        return
    if len(diag.evidence) < MAX_EVIDENCE_SPANS:
        diag.evidence.append({"start": start, "end": end, "sources": sources})
    else:
        diag.evidence_truncated = True
        if diag.evidence:
            diag.evidence[-1]["end"] = max(diag.evidence[-1]["end"], end)
