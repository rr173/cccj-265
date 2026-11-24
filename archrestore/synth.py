"""Deterministic synthetic datasets for offline re-enactment and tests.

Everything here is *test scaffolding*: it builds a logical dataset, writes
it out as several physical replicas with different page boundaries,
encodings and dictionaries, then damages them in controlled ways.  It never
participates in repair.

Far larger than memory: ``--rows`` is bounded only by disk, because the
logical values are generated row by row and every structure is streamed.
"""

import hashlib
import json
import os
import random

from .format import (
    DICT_MAGIC,
    MANIFEST_KIND,
    REPLICA_KIND,
    TYPE_INTEGER,
    encode_value,
)


def generate_value(rng, ctype, row, pattern, big):
    """Deterministic logical value for (column type, row)."""
    if pattern == "nulls" and row % 7 == 3:
        return None
    if ctype == TYPE_INTEGER:
        return rng.randrange(-(1 << 40), 1 << 40)
    if pattern == "empty":
        if row % 11 == 0:
            return ""
        if row % 13 == 0:
            return None
    if pattern == "big" and row % 97 == 0:
        return "x" * big
    if pattern == "unicode":
        return "行-%d-é世界" % row
    if pattern == "unique":
        return "value-%09d" % row
    return "s%d" % rng.randrange(500)


class LogicalDataset(object):
    """A dataset that can regenerate any row range without materialising all."""

    def __init__(self, columns, row_count, seed=0, patterns=None, big=1 << 20):
        self.columns = list(columns)  # [(name, type)]
        self.row_count = row_count
        self.seed = seed
        self.patterns = patterns or {}
        self.big = big

    def value(self, col_index, row):
        name, ctype = self.columns[col_index]
        # Derive a per-row RNG so random access is deterministic and cheap.
        rng = random.Random("%d:%d:%d" % (self.seed, col_index, row))
        return generate_value(rng, ctype, row, self.patterns.get(name), self.big)

    def iter_rows(self, start, end):
        for row in range(start, end):
            yield row, [self.value(i, row) for i in range(len(self.columns))]


def build_manifest(dataset, slot_sizes):
    """Manifest with per-column slots of the given sizes (may differ)."""
    slots = []
    sid = 0
    for ci, (name, ctype) in enumerate(dataset.columns):
        if isinstance(slot_sizes, dict):
            sizes = slot_sizes[name]
        else:
            sizes = slot_sizes
        if isinstance(sizes, int):
            sizes = [sizes] * ((dataset.row_count + sizes - 1) // sizes)
        sizes = list(sizes)
        # A repeating pattern is repeated until it covers the dataset.
        if sizes and sum(sizes) < dataset.row_count:
            cycle = sum(sizes)
            sizes = sizes * ((dataset.row_count + cycle - 1) // cycle)
        start = 0
        for size in sizes:
            end = min(start + size, dataset.row_count)
            if end <= start:
                break
            digest = _digest_rows(dataset, ci, start, end)
            slots.append({"column": name, "slot": sid, "start": start,
                          "end": end, "digest": digest})
            sid += 1
            start = end
        if start != dataset.row_count:
            raise ValueError("slot sizes for %r do not cover row_count" % name)
    return {
        "archrestore_format": {"name": MANIFEST_KIND, "version": 1},
        "row_count": dataset.row_count,
        "columns": [{"name": n, "type": t} for n, t in dataset.columns],
        "slots": slots,
    }


def _digest_rows(dataset, ci, start, end):
    name, ctype = dataset.columns[ci]
    from .format import SlotHasher
    h = SlotHasher(name, ctype, start, end)
    for row in range(start, end):
        h.update(dataset.value(ci, row))
    return h.hexdigest()


def build_dictionary(dataset, ci, codes_needed):
    """Build a dictionary containing the needed canonical values, shuffled.

    ``codes_needed`` is a collection of *logical values* (not row numbers).
    """
    name, ctype = dataset.columns[ci]
    # Sort by canonical bytes so the blob is deterministic for a given set.
    values = sorted(set(codes_needed), key=lambda v: encode_value(v, ctype))
    # The dictionary is a *set* of values; code assignment is arbitrary and is
    # deliberately scrambled so a tool cannot assume code == row derived.
    order = list(range(len(values)))
    random.Random(1234 + ci).shuffle(order)
    entries = [values[i] for i in order]
    code_of = {}
    for code, i in enumerate(order):
        code_of[values[i]] = code
    blob = bytearray(DICT_MAGIC)
    blob += len(entries).to_bytes(4, "little")
    for v in entries:
        blob += encode_value(v, ctype)
    return bytes(blob), code_of


def build_replica(directory, dataset, descriptor, page_sizes, encodings,
                  rng, dict_mode="per-page", dict_shared=False):
    """Write one replica directory: descriptor + data blobs.

    ``descriptor`` provides the page ranges per column, so different replicas
    can deliberately use different boundaries, gaps and overlaps.
    """
    os.makedirs(directory, exist_ok=True)
    data_path = os.path.join(directory, "data.bin")
    dict_path = os.path.join(directory, "dict.bin")
    page_id = 0
    pages = []
    shared_dict = None
    shared_ref = None
    with open(data_path, "wb") as data_fh, open(dict_path, "wb") as dict_fh:
        offset = 0
        for ci, (name, ctype) in enumerate(dataset.columns):
            for (start, end) in descriptor[name]:
                if not (0 <= start < end <= dataset.row_count):
                    raise ValueError("bad page plan for %r" % name)
                enc = encodings[(page_id + ci) % len(encodings)]
                if enc == "dict" and dict_mode == "per-page":
                    needed = [dataset.value(ci, r) for r in range(start, end)]
                    blob, code_of = build_dictionary(dataset, ci, needed)
                    dict_off = dict_fh.tell()
                    dict_fh.write(blob)
                    dict_fh.flush()
                    body = bytearray()
                    for r in range(start, end):
                        body += code_of[dataset.value(ci, r)].to_bytes(4, "little")
                    dict_desc = {
                        "dict_id": "d%d" % page_id,
                        "dict_digest": hashlib.sha256(blob).hexdigest(),
                        "dict_byte_length": len(blob),
                        "dict_location": {"file": "dict.bin",
                                          "offset": dict_off},
                    }
                elif enc == "dict":
                    if shared_dict is None or not dict_shared:
                        needed = []
                        # Shared dictionary for the whole column: it may be
                        # much larger than one page needs, which is allowed.
                        for r in range(dataset.row_count):
                            needed.append(dataset.value(ci, r))
                        shared_dict = dict(
                            zip(("blob", "code_of"),
                                build_dictionary(dataset, ci, needed)))
                        dict_off = dict_fh.tell()
                        dict_fh.write(shared_dict["blob"])
                        dict_fh.flush()
                        shared_ref = {
                            "dict_id": "shared-%s" % name,
                            "dict_digest": hashlib.sha256(
                                shared_dict["blob"]).hexdigest(),
                            "dict_byte_length": len(shared_dict["blob"]),
                            "dict_location": {"file": "dict.bin",
                                              "offset": dict_off},
                        }
                    body = bytearray()
                    for r in range(start, end):
                        body += shared_dict["code_of"][
                            dataset.value(ci, r)].to_bytes(4, "little")
                    dict_desc = dict(shared_ref)
                else:
                    body = bytearray()
                    for r in range(start, end):
                        body += encode_value(dataset.value(ci, r), ctype)
                    dict_desc = None
                page_off = offset
                data_fh.write(body)
                data_fh.flush()
                offset += len(body)
                page = {
                    "page": page_id, "column": name, "start": start,
                    "end": end, "encoding": enc, "byte_length": len(body),
                    "byte_digest": hashlib.sha256(bytes(body)).hexdigest(),
                    "location": {"file": "data.bin", "offset": page_off},
                }
                if dict_desc is not None:
                    page.update(dict_desc)
                pages.append(page)
                page_id += 1
    replica = {
        "archrestore_format": {"name": REPLICA_KIND, "version": 1},
        "replica_id": os.path.basename(directory),
        "pages": pages,
    }
    with open(os.path.join(directory, "replica.json"), "w",
              encoding="utf-8") as fh:
        json.dump(replica, fh, ensure_ascii=False, sort_keys=True, indent=1)
        fh.write("\n")
    return replica


def corrupt_page_bytes(directory, page, mode="flip", position=None):
    """Damage a page body in place (test scaffolding)."""
    path = os.path.join(directory, page["location"]["file"])
    off = page["location"]["offset"]
    with open(path, "r+b") as fh:
        if mode == "flip":
            pos = off + (position if position is not None else 0)
            fh.seek(pos)
            b = fh.read(1)
            fh.seek(pos)
            fh.write(bytes([b[0] ^ 0xFF]))
        elif mode == "truncate":
            # Shrink the file so the declared range no longer fits.
            size = os.path.getsize(path)
            fh.truncate(max(0, off + page["byte_length"] // 2))
        elif mode == "extend":
            fh.seek(off + page["byte_length"])
            fh.write(b"\x00\x00")


def reconstruct_truth(dataset, columns=None):
    """Reference dump of logical rows for tests (small datasets only)."""
    cols = columns or [n for n, _t in dataset.columns]
    idx = {n: i for i, (n, _t) in enumerate(dataset.columns)}
    rows = []
    for row in range(dataset.row_count):
        rows.append({n: dataset.value(idx[n], row) for n in cols})
    return rows
