"""Resource-contract tests: values far larger than the in-process working set,
and operations whose memory must not scale with column or slot size.

We measure with ``tracemalloc`` (Python allocation peak) rather than RSS,
because the contract is about the tool's own working set.  A 24 MiB string is
used: large enough that materialising it (in canonical form, base64, etc.)
would visibly exceed the bound, small enough to keep the suite quick.
"""

import json
import os
import sys
import tracemalloc
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from helpers import Workspace, make_replica, run, write_manifest  # noqa: E402

from archrestore import synth  # noqa: E402
from archrestore.archive import ArchiveReader, stream_text, verify_archive  # noqa: E402
from archrestore.pipeline import run_repair  # noqa: E402

HUGE = 24 * 1024 * 1024


class HugeValueTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()

    def tearDown(self):
        self.ws.cleanup()

    def _dataset(self, rows=12):
        return synth.LogicalDataset(
            [("id", "integer"), ("blob", "string")],
            row_count=rows, seed=3,
            patterns={"blob": "big"}, big=HUGE)

    def test_huge_values_roundtrip_with_bounded_peak(self):
        ds = self._dataset()
        _m, mpath = write_manifest(ds, {"id": [6, 6], "blob": [4, 8]},
                                   self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 12)], "blob": [(0, 12)]},
            encodings=("plain",), page_sizes=(5,))
        out = os.path.join(self.ws.root, "out")
        tracemalloc.start()
        try:
            res = run_repair(mpath, [r1], out)
            _cur, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(res["outcome"], "restored", res.get("error"))
        # Far below the size of a single huge value.
        self.assertLess(peak, 8 * 1024 * 1024,
                        "peak allocation %d exceeded bound" % peak)
        with ArchiveReader(out) as reader:
            # Stream a huge value out and hash it; must not materialise it.
            tracemalloc.start()
            try:
                raw = reader.read_row_column("blob", 0)
                import hashlib
                h = hashlib.sha256()
                total = 0
                for piece in stream_text(raw, reader.pool):
                    h.update(piece.encode("utf-8"))
                    total += len(piece)
                _cur, peak2 = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
        self.assertGreater(total, HUGE)
        self.assertLess(peak2, 4 * 1024 * 1024)
        # The value is exact: 24 MiB of 'x'.
        self.assertEqual(h.hexdigest(),
                         hashlib.sha256(
                             ('"' + "x" * HUGE + '"').encode("utf-8")).hexdigest())

    def test_archive_verification_of_huge_values_is_bounded(self):
        ds = self._dataset(rows=4)
        _m, mpath = write_manifest(ds, {"id": [4], "blob": [4]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 4)], "blob": [(0, 4)]}, encodings=("plain",),
            page_sizes=(3,))
        out = os.path.join(self.ws.root, "out")
        res = run_repair(mpath, [r1], out)
        self.assertEqual(res["outcome"], "restored", res.get("error"))
        tracemalloc.start()
        try:
            v = verify_archive(out)
            _cur, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertTrue(v.ok, v.problems)
        self.assertLess(peak, 8 * 1024 * 1024, peak)

    def test_many_rows_do_not_accumulate(self):
        # 20k rows across a slot; per-row state must stay O(1) apart from the
        # compact block index (one u64 per 1024 rows).
        rows = 20000
        ds = synth.LogicalDataset([("id", "integer")], row_count=rows,
                                  seed=5)
        _m, mpath = write_manifest(ds, {"id": [rows]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {"id": [(0, rows)]},
                          encodings=("plain",), page_sizes=(5000,))
        out = os.path.join(self.ws.root, "out")
        tracemalloc.start()
        try:
            res = run_repair(mpath, [r1], out)
            _cur, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(res["outcome"], "restored", res.get("error"))
        self.assertLess(peak, 8 * 1024 * 1024, peak)


class ScaleTest(unittest.TestCase):
    """A dataset deliberately split at odd boundaries with many small pages."""

    def setUp(self):
        self.ws = Workspace()

    def tearDown(self):
        self.ws.cleanup()

    def test_many_small_pages_many_slots(self):
        rows = 3000
        ds = synth.LogicalDataset([("a", "integer"), ("b", "string")],
                                  row_count=rows, seed=9,
                                  patterns={"b": "unicode"})
        sizes = {"a": [7] * ((rows + 6) // 7), "b": [3, 5, 11, 2]}
        _m, mpath = write_manifest(ds, sizes, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "a": [(0, rows)], "b": [(0, 1000), (1000, rows)]},
            encodings=("plain", "dict"), page_sizes=(13, 7))
        out = os.path.join(self.ws.root, "out")
        res = run_repair(mpath, [r1], out)
        self.assertEqual(res["outcome"], "restored", res.get("error"))
        self.assertGreater(res["summary"]["slot_count"], 400)
        with ArchiveReader(out) as reader:
            for row in (0, 1, 1500, rows - 1):
                import json as _json
                got_a = _json.loads("".join(stream_text(
                    reader.read_row_column("a", row), reader.pool)))
                got_b = _json.loads("".join(stream_text(
                    reader.read_row_column("b", row), reader.pool)))
                self.assertEqual(got_a, ds.value(0, row))
                self.assertEqual(got_b, ds.value(1, row))


if __name__ == "__main__":
    unittest.main()
