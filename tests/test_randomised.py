"""Randomised differential tests.

For random datasets, replica layouts, encodings, gaps and overlaps, the
resolver's verdict is compared against ground truth computed directly from
the synthetic generator.  This exercises segment boundaries, stitching,
page-boundary/slot-boundary mismatches, and dictionary sharing at scale.
"""

import json
import os
import random
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from helpers import Workspace, make_replica, run, write_manifest  # noqa: E402

from archrestore import synth  # noqa: E402
from archrestore.archive import ArchiveReader, stream_text  # noqa: E402


def random_plan(rng, rows, max_pages, require_full=False):
    """A random set of disjoint pages, sometimes leaving gaps.

    With ``require_full`` the gaps are removed, so the plan tiles [0, rows).
    Overlaps between *replicas* are still possible (and common).
    """
    cuts = sorted(rng.sample(range(0, rows + 1), min(max_pages + 1, rows + 1)))
    pages = []
    for a, b in zip(cuts, cuts[1:]):
        if b > a and (require_full or rng.random() < 0.85):
            pages.append((a, b))
    return pages


class RandomisedTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()

    def tearDown(self):
        self.ws.cleanup()

    def test_random_layouts_are_either_restored_exactly_or_diagnosed(self):
        rng = random.Random(2024)
        for trial in range(6):
            rows = rng.choice([40, 75, 130])
            cols = [("a", "integer"), ("b", "string")]
            ds = synth.LogicalDataset(
                cols, row_count=rows, seed=1000 + trial,
                patterns={"b": rng.choice(["unique", "unicode", "empty"])})
            slot_sizes = {
                "a": [rng.randint(1, rows) for _ in range(rng.randint(1, 5))],
                "b": [rng.randint(1, rows) for _ in range(rng.randint(1, 4))],
            }
            _m, mpath = write_manifest(ds, slot_sizes, self.ws.root,
                                        name="m%d.json" % trial)
            replicas = []
            nrep = rng.randint(1, 3)
            # Half the trials are guaranteed fully covered, so the restore
            # path (and exact value comparison) is exercised too.
            full = trial % 2 == 1
            for i in range(nrep):
                plan = {c: random_plan(rng, rows, rng.randint(2, 7),
                                       require_full=full)
                        for c, _t in cols}
                encs = rng.choice([("plain",), ("dict",),
                                   ("plain", "dict"), ("dict", "plain")])
                replicas.append(make_replica(
                    self.ws.root, ds, "t%d_r%d" % (trial, i), plan,
                    encodings=encs,
                    page_sizes=(rng.choice([3, 9, 17, 40]),),
                    seed=rng.randrange(10000),
                    dict_mode=rng.choice(["per-page", "shared"])))
            out = os.path.join(self.ws.root, "out%d" % trial)
            res = run(self.ws.root, mpath, replicas, outname="out%d" % trial)
            summary = res["summary"]

            # Ground truth coverage: union of page ranges per column.
            covered = {c: [] for c, _t in cols}
            for name, directory in replicas:
                with open(os.path.join(directory, "replica.json")) as fh:
                    rep = json.load(fh)
                for p in rep["pages"]:
                    covered[p["column"]].append((p["start"], p["end"]))
            truth_missing = {}
            for c, _t in cols:
                merged = []
                for a, b in sorted(covered[c]):
                    if merged and a <= merged[-1][1]:
                        merged[-1][1] = max(merged[-1][1], b)
                    else:
                        merged.append([a, b])
                missing = 0
                cur = 0
                for a, b in merged:
                    if a > cur:
                        missing += a - cur
                    cur = max(cur, b)
                missing += max(0, rows - cur)
                truth_missing[c] = missing

            expected_missing = sum(truth_missing.values())
            self.assertEqual(summary["cells_missing"], expected_missing,
                             "trial %d: missing rows mismatch" % trial)
            self.assertEqual(summary["cells_disputed"], 0,
                             "trial %d: no conflicting evidence was planted"
                             % trial)

            if expected_missing == 0:
                self.assertEqual(res["outcome"], "restored",
                                 "trial %d: %s" % (trial, res.get("error")))
                got = []
                with ArchiveReader(out) as reader:
                    for row in range(rows):
                        rec = {}
                        for c, _t in cols:
                            raw = reader.read_row_column(c, row)
                            rec[c] = json.loads(
                                "".join(stream_text(raw, reader.pool)))
                        got.append(rec)
                self.assertEqual(got, synth.reconstruct_truth(ds))
            else:
                self.assertEqual(res["outcome"], "incomplete",
                                 "trial %d" % trial)
                self.assertFalse(os.path.exists(
                    os.path.join(out, "archive.json")))


if __name__ == "__main__":
    unittest.main()
