"""Tests for the out-of-core archive comparison (archrestore compare).

Covers: added/removed/changed semantics and original row numbers, composite
and typed identities, empty/duplicate identity rejection with row numbers and
no published ledger, schema agreement, spec validation, deterministic ordering
and output, external-sort threshold invariance, CLI exit codes, ledger
re-check, and bounded memory.

Run with::

    python3 -I -m unittest tests.test_compare -v
"""

import json
import os
import random
import subprocess
import sys
import tempfile
import tracemalloc
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

from helpers import Workspace  # noqa: E402

from archrestore import synth  # noqa: E402
from archrestore.archive import ArchiveReader, stream_text  # noqa: E402
from archrestore.compare import (  # noqa: E402
    CompareFailure,
    CompareSchemaError,
    CompareSpecError,
    compare_archives,
    read_ledger,
)
from archrestore.pipeline import run_repair  # noqa: E402


LAUNCHER = (
    "import sys; sys.path.insert(0, %r); "
    "from archrestore.cli import main; sys.exit(main(sys.argv[1:]))" % ROOT
)


def cli(*args):
    # -I isolation, exactly as in tests/test_cli.py.
    env = dict(os.environ)
    env.pop("PYTHONSTARTUP", None)
    env.pop("PYTHONPATH", None)
    return subprocess.run(
        [sys.executable, "-I", "-c", LAUNCHER] + list(args),
        capture_output=True, text=True, env=env, cwd=ROOT)


class TableDataset(object):
    """Minimal synth-compatible dataset backed by an explicit row table."""

    def __init__(self, columns, rows):
        self.columns = list(columns)
        self._rows = [tuple(r) for r in rows]
        self.row_count = len(self._rows)

    def value(self, col_index, row):
        return self._rows[row][col_index]


def restore_table(ws, name, columns, rows, slot_size=None):
    ds = TableDataset(columns, rows)
    sizes = [slot_size] if slot_size else [max(1, ds.row_count)]
    manifest = synth.build_manifest(ds, sizes)
    mpath = os.path.join(ws.root, name + "-manifest.json")
    with open(mpath, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh)
    rdir = os.path.join(ws.root, name + "-replica")
    plan = {n: ([(0, ds.row_count)] if rows else [])
            for n, _t in columns}
    synth.build_replica(rdir, ds, plan, [13, 7], ["plain"],
                        random.Random(3))
    arc = os.path.join(ws.root, name + "-archive")
    rep = run_repair(mpath, [(os.path.basename(rdir), rdir)], arc)
    assert rep["outcome"] == "restored", rep.get("error")
    return arc


def write_spec(ws, obj, name="spec.json"):
    path = os.path.join(ws.root, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh)
    return path


def by_identity(records, field="id"):
    return {r[field]: r for r in records}


class CompareSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()
        self.cols = [("id", "integer"), ("color", "string"),
                     ("qty", "integer"), ("note", "string")]
        self.before = [
            (1, "red", 10, "a"),
            (2, "blue", 20, "b"),
            (3, "green", 30, None),
            (4, "red", 40, None),
            (5, "white", 50, "e"),
            (6, "black", 60, "f"),
        ]
        # id 2 removed; id 3 changes color+qty; id 4 changes null note;
        # id 1/5/6 unchanged; id 7 added.  Rows are deliberately re-ordered.
        self.after_rows = [
            (6, "black", 60, "f"),
            (1, "red", 10, "a"),
            (3, "olive", 31, None),
            (4, "red", 40, "was-null"),
            (5, "white", 50, "e"),
            (7, "pink", 70, "g"),
        ]
        self.a = restore_table(self.ws, "before", self.cols, self.before)
        self.b = restore_table(self.ws, "after", self.cols, self.after_rows)
        self.spec = write_spec(self.ws, {"identity_fields": ["id"]})
        self.ledger = os.path.join(self.ws.root, "changes.jsonl")

    def tearDown(self):
        self.ws.cleanup()

    def _compare(self, **kw):
        return compare_archives(self.a, self.b, self.spec, out=self.ledger,
                                verify=False, **kw)

    def test_add_remove_change_and_unchanged(self):
        rep = self._compare()
        recs = list(read_ledger(self.ledger))
        idx = by_identity(recs)
        self.assertEqual(rep["summary"]["added"], 1)
        self.assertEqual(rep["summary"]["removed"], 1)
        self.assertEqual(rep["summary"]["changed"], 2)
        self.assertEqual(rep["summary"]["unchanged"], 3)
        self.assertEqual(len(recs), 4)  # unchanged devices not written

        removed = idx[2]
        self.assertEqual(removed["op"], "removed")
        self.assertEqual(removed["row"], 1)  # original before row
        self.assertEqual(removed["before_row"], 1)
        self.assertIsNone(removed["after_row"])

        added = idx[7]
        self.assertEqual(added["op"], "added")
        self.assertEqual(added["row"], 5)  # original after row
        self.assertEqual(added["after_row"], 5)

        changed3 = idx[3]
        self.assertEqual(changed3["op"], "changed")
        self.assertEqual(changed3["before_row"], 2)
        self.assertEqual(changed3["after_row"], 2)
        self.assertEqual(set(changed3["changes"]), {"color", "qty"})
        self.assertEqual(changed3["changes"]["color"]["before"], "green")
        self.assertEqual(changed3["changes"]["color"]["after"], "olive")
        self.assertEqual(changed3["changes"]["qty"],
                         {"before": 30, "after": 31, "old": 30, "new": 31})

        changed4 = idx[4]
        self.assertEqual(
            changed4["changes"]["note"],
            {"before": None, "after": "was-null",
             "old": None, "new": "was-null"})

        # Deterministic order: integer identity ascending (1..7 subset).
        self.assertEqual([r["id"] for r in recs], [2, 3, 4, 7])

    def test_deterministic_bytes_across_thresholds(self):
        paths = []
        for i, (rr, rb) in enumerate(((4096, 4 << 20), (2, 1), (1, 10 ** 9))):
            out = os.path.join(self.ws.root, "det-%d.jsonl" % i)
            compare_archives(self.a, self.b, self.spec, out=out,
                             verify=False, run_records=rr, run_bytes=rb)
            paths.append(out)
        with open(paths[0], "rb") as fh:
            base = fh.read()
        for p in paths[1:]:
            with open(p, "rb") as fh:
                self.assertEqual(fh.read(), base)

    def test_self_compare_is_empty_ledger(self):
        out = os.path.join(self.ws.root, "self.jsonl")
        rep = compare_archives(self.a, self.a, self.spec, out=out,
                               verify=False)
        self.assertEqual(rep["summary"]["changed"], 0)
        self.assertEqual(rep["summary"]["unchanged"], len(self.before))
        self.assertEqual(os.path.getsize(out), 0)

    def test_lazy_report_changes(self):
        rep = self._compare()
        # Lazy dict access loads ledger records without a second compare.
        self.assertEqual(len(rep["changes"]), 4)
        self.assertEqual(len(rep["events"]), 4)

    def test_zero_row_archives(self):
        empty = restore_table(self.ws, "zero", self.cols, [])
        out = os.path.join(self.ws.root, "zero.jsonl")
        rep = compare_archives(empty, empty, self.spec, out=out)
        self.assertEqual(rep["summary"]["total_events"], 0)
        self.assertEqual(os.path.getsize(out), 0)


class IdentityValidationTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()
        self.cols = [("id", "string"), ("reading", "integer")]

    def tearDown(self):
        self.ws.cleanup()

    def test_empty_identity_reports_every_row_and_publishes_nothing(self):
        rows = [("D1", 1), (None, 2), ("D3", 3), (None, 4), ("D5", 5)]
        arc = restore_table(self.ws, "nulls", self.cols, rows)
        ledger = os.path.join(self.ws.root, "nope.jsonl")
        with open(ledger, "w") as fh:
            fh.write("KEEP-ME\n")
        spec = write_spec(self.ws, {"identity_fields": ["id"]})
        with self.assertRaises(CompareFailure) as ctx:
            compare_archives(arc, arc, spec, out=ledger, verify=False)
        rep = ctx.exception.report
        empty_rows = sorted(p["row"] for p in rep["problems"]
                            if p["kind"] == "empty_identity"
                            and p["side"] == "before")
        self.assertEqual(empty_rows, [1, 3])
        self.assertEqual(rep["summary"]["empty_identities"], 4)
        # No partial result: destination untouched.
        with open(ledger) as fh:
            self.assertEqual(fh.read(), "KEEP-ME\n")
        # Full problem report exists and is valid JSONL with the row numbers.
        self.assertTrue(os.path.isfile(rep["problems_path"]))
        with open(rep["problems_path"], encoding="utf-8") as fh:
            for line in fh:
                json.loads(line)

    def test_duplicate_identity_reports_all_rows(self):
        rows = [("D1", 1), ("D2", 2), ("D3", 3), ("D2", 4),
                ("D2", 5), ("D9", 6)]
        arc = restore_table(self.ws, "dups", self.cols, rows)
        spec = write_spec(self.ws, {"identity_fields": ["id"]})
        ledger = os.path.join(self.ws.root, "nope2.jsonl")
        with self.assertRaises(CompareFailure) as ctx:
            compare_archives(arc, arc, spec, out=ledger, verify=False)
        rep = ctx.exception.report
        dup = [p for p in rep["problems"]
               if p["kind"] == "duplicate_identity"
               and p["side"] == "before"
               and p["identity"]["id"] == "D2"]
        self.assertEqual(len(dup), 1)
        self.assertEqual(dup[0]["rows"], [1, 3, 4])
        self.assertEqual(dup[0]["row_count"], 3)
        self.assertFalse(os.path.exists(ledger))

    def test_duplicate_spanning_sort_runs_is_found(self):
        # Duplicate identities far apart in row order force the equal keys
        # through multiple tiny runs and a k-way merge before adjacency.
        rows = [("D%03d" % i, i) for i in range(60)]
        rows[50] = ("D007", 999)  # duplicate of row 7, out of order
        arc = restore_table(self.ws, "crossrun", self.cols, rows)
        spec = write_spec(self.ws, {"identity_fields": ["id"]})
        with self.assertRaises(CompareFailure) as ctx:
            compare_archives(arc, arc, spec,
                             out=os.path.join(self.ws.root, "cr.jsonl"),
                             verify=False, run_records=3, run_bytes=1)
        dup = [p for p in ctx.exception.report["problems"]
               if p["kind"] == "duplicate_identity"
               and p["side"] == "before"]
        self.assertEqual(len(dup), 1)
        self.assertEqual(dup[0]["rows"], [7, 50])

    def test_problems_are_side_labelled_for_two_archives(self):
        before = restore_table(
            self.ws, "pb", self.cols,
            [("D1", 1), ("D1", 2), ("D2", 3)])
        after = restore_table(
            self.ws, "pa", self.cols,
            [("X1", 1), (None, 2), ("X2", 3)])
        spec = write_spec(self.ws, {"identity_fields": ["id"]})
        with self.assertRaises(CompareFailure) as ctx:
            compare_archives(before, after, spec,
                             out=os.path.join(self.ws.root, "x.jsonl"),
                             verify=False)
        sides = {(p["side"], p["kind"]) for p in ctx.exception.report["problems"]}
        self.assertIn(("before", "duplicate_identity"), sides)
        self.assertIn(("after", "empty_identity"), sides)


class CompositeAndTypeTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()

    def tearDown(self):
        self.ws.cleanup()

    def test_composite_identity_order_and_injectivity(self):
        cols = [("org", "string"), ("dev", "string"), ("v", "integer")]
        # ("a","bc") and ("ab","c") must stay distinct; include prefix/NUL
        # edge cases to exercise order-preserving keys.
        before = [
            ("a", "bc", 1), ("ab", "c", 2), ("a", "b", 3),
            ("a\x00", "d", 4), ("a", "b2", 5), ("org2", "z", 6),
        ]
        after_rows = list(before)
        after_rows[0] = ("a", "bc", 11)  # change first row's value
        random.Random(0).shuffle(after_rows)
        a = restore_table(self.ws, "cb", cols, before)
        b = restore_table(self.ws, "ca", cols, after_rows)
        spec = write_spec(self.ws, {"identity_fields": ["org", "dev"]})
        ledger = os.path.join(self.ws.root, "c.jsonl")
        rep = compare_archives(a, b, spec, out=ledger, verify=False)
        recs = list(read_ledger(ledger))
        # Only the changed composite identity is in the ledger.
        self.assertEqual(rep["summary"]["changed"], 1)
        self.assertEqual(recs[0]["identity"], {"org": "a", "dev": "bc"})
        self.assertEqual(recs[0]["changes"]["v"]["before"], 1)
        self.assertEqual(recs[0]["changes"]["v"]["after"], 11)

        # Ordering check on a richer add/remove set.
        rows_b = [(o, d, 0) for o, d in
                  [("a", "z"), ("aa", "a"), ("a", "a"), ("b", "a")]]
        rows_a = rows_b[:3] + [("a", "m", 0)]
        ab = restore_table(self.ws, "ob", cols, rows_b)
        aa = restore_table(self.ws, "oa", cols, rows_a)
        out2 = os.path.join(self.ws.root, "o.jsonl")
        compare_archives(ab, aa, spec, out=out2, verify=False)
        ids = [(r["identity"]["org"], r["identity"]["dev"])
               for r in read_ledger(out2)]
        self.assertEqual(ids, sorted(ids))

    def test_integer_identity_numeric_order(self):
        cols = [("id", "integer"), ("v", "string")]
        ids = [0, 1, -1, 255, 256, -256, -255, 2 ** 63 - 1, -(2 ** 63)]
        before = [(i, "x") for i in ids]
        after = [(i, "y") for i in ids]  # every row changes
        a = restore_table(self.ws, "ib", cols, before)
        b = restore_table(self.ws, "ia", cols, after)
        spec = write_spec(self.ws, {"identity_fields": ["id"]})
        ledger = os.path.join(self.ws.root, "i.jsonl")
        compare_archives(a, b, spec, out=ledger, verify=False)
        got = [r["id"] for r in read_ledger(ledger)]
        self.assertEqual(got, sorted(ids))

    def test_type_difference_of_values_is_exact(self):
        # string "1" and integer 1 can never be the same identity/value; here
        # identity is the string code and an integer column changes value.
        cols = [("code", "string"), ("n", "integer")]
        a = restore_table(self.ws, "tb", cols, [("1", 1), ("2", 2)])
        b = restore_table(self.ws, "ta", [("code", "string"), ("n", "integer")],
                          [("1", 2), ("2", 2)])
        spec = write_spec(self.ws, {"identity_fields": ["code"]})
        ledger = os.path.join(self.ws.root, "t.jsonl")
        rep = compare_archives(a, b, spec, out=ledger, verify=False)
        self.assertEqual(rep["summary"]["changed"], 1)


class SchemaAndSpecTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()

    def tearDown(self):
        self.ws.cleanup()

    def test_missing_column_between_archives(self):
        cols1 = [("id", "integer"), ("a", "string")]
        cols2 = [("id", "integer"), ("a", "string"),
                 ("extra", "integer")]
        x = restore_table(self.ws, "s1", cols1, [(1, "x")])
        y = restore_table(self.ws, "s2", cols2, [(1, "x", 9)])
        spec = write_spec(self.ws, {"identity_fields": ["id"]})
        with self.assertRaises(CompareSchemaError):
            compare_archives(x, y, spec,
                             out=os.path.join(self.ws.root, "l.jsonl"),
                             verify=False)

    def test_column_type_mismatch(self):
        x = restore_table(self.ws, "tt1", [("id", "integer")], [(1,)])
        y = restore_table(self.ws, "tt2", [("id", "string")], [("1",)])
        spec = write_spec(self.ws, {"identity_fields": ["id"]})
        with self.assertRaises(CompareSchemaError):
            compare_archives(x, y, spec,
                             out=os.path.join(self.ws.root, "l.jsonl"),
                             verify=False)

    def test_unknown_identity_column(self):
        x = restore_table(self.ws, "u1", [("id", "integer")], [(1,)])
        spec = write_spec(self.ws, {"identity_fields": ["nope"]})
        with self.assertRaises(CompareSchemaError):
            compare_archives(x, x, spec,
                             out=os.path.join(self.ws.root, "l.jsonl"),
                             verify=False)

    def test_bad_specs(self):
        x = restore_table(self.ws, "u2", [("id", "integer")], [(1,)])
        for bad in ({}, {"identity_fields": []},
                    {"identity_fields": ["id", "id"]}, 42):
            with self.assertRaises(CompareSpecError):
                compare_archives(x, x, bad,
                                 out=os.path.join(self.ws.root, "l.jsonl"),
                                 verify=False)

    def test_inline_json_spec_and_identity_kwarg(self):
        x = restore_table(self.ws, "u3", [("id", "integer")], [(1,)])
        out = os.path.join(self.ws.root, "kw.jsonl")
        rep = compare_archives(x, x, '{"identity_fields": ["id"]}',
                               out=out, verify=False)
        self.assertTrue(rep["ok"])
        out2 = os.path.join(self.ws.root, "kw2.jsonl")
        rep2 = compare_archives(x, x, identity_fields=["id"], out=out2,
                                verify=False)
        self.assertTrue(rep2["ok"])


class RandomisedDifferentialTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()

    def tearDown(self):
        self.ws.cleanup()

    def test_random_shuffled_dataset_matches_naive_diff(self):
        rng = random.Random(20261124)
        n = 700
        cols = [("id", "integer"), ("name", "string"), ("v", "integer")]
        before = [(100000 + i, "dev-%05d" % i, rng.randrange(1 << 30))
                  for i in range(n)]
        after = []
        for j, row in enumerate(r for r in before if r[0] % 19 != 0):
            row = list(row)
            if j % 13 == 0:
                row[2] += 7
            if j % 17 == 0:
                row[1] = "renamed-%d" % j
            after.append(tuple(row))
        for i in range(25):
            after.append((300000 + i, "new-%d" % i, i))
        rng.shuffle(after)

        a = restore_table(self.ws, "rb", cols, before)
        b = restore_table(self.ws, "ra", cols, after)
        spec = {"identity_fields": ["id"]}

        out = os.path.join(self.ws.root, "rand.jsonl")
        rep = compare_archives(a, b, spec, out=out, verify=False,
                               run_records=5, run_bytes=1)
        recs = list(read_ledger(out))

        bmap = {r[0]: (i, r) for i, r in enumerate(before)}
        amap = {r[0]: (i, r) for i, r in enumerate(after)}

        def expected_changes(k):
            _bi, br = bmap[k]
            _ai, ar = amap[k]
            return {cols[j][0]: (br[j], ar[j])
                    for j in range(1, len(cols)) if br[j] != ar[j]}

        expected_event_ids = []
        for k in sorted(set(bmap) | set(amap)):
            if k in bmap and k not in amap:
                expected_event_ids.append(k)
            elif k in amap and k not in bmap:
                expected_event_ids.append(k)
            elif expected_changes(k):
                expected_event_ids.append(k)

        rec_by_id = {r["id"]: r for r in recs}
        # Exactly the non-unchanged identities are present, in sorted order.
        self.assertEqual([r["id"] for r in recs], expected_event_ids)

        n_change = 0
        for k in expected_event_ids:
            rec = rec_by_id[k]
            if k in bmap and k not in amap:
                self.assertEqual(rec["op"], "removed")
                self.assertEqual(rec["before_row"], bmap[k][0])
            elif k in amap and k not in bmap:
                self.assertEqual(rec["op"], "added")
                self.assertEqual(rec["after_row"], amap[k][0])
            else:
                bi, _br = bmap[k]
                ai, _ar = amap[k]
                exp_changes = expected_changes(k)
                n_change += 1
                self.assertEqual(rec["op"], "changed")
                self.assertEqual(rec["before_row"], bi)
                self.assertEqual(rec["after_row"], ai)
                self.assertEqual(set(rec["changes"]), set(exp_changes))
                for cn, (bv, av) in exp_changes.items():
                    self.assertEqual(rec["changes"][cn]["before"], bv)
                    self.assertEqual(rec["changes"][cn]["after"], av)
        self.assertEqual(rep["summary"]["changed"], n_change)

    def test_independent_review_through_archive_reader(self):
        # Every ledger line must be re-checkable using only the archives and
        # the reported row numbers, without compare's pairing state.
        cols = [("id", "integer"), ("v", "integer"), ("s", "string")]
        before = [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")]
        after = [(2, 21, "b"), (3, 30, "C"), (4, 40, "d")]
        a = restore_table(self.ws, "iv1", cols, before)
        b = restore_table(self.ws, "iv2", cols, after)
        spec_path = write_spec(self.ws, {"identity_fields": ["id"]})
        ledger = os.path.join(self.ws.root, "iv.jsonl")
        compare_archives(a, b, spec_path, out=ledger, verify=False)

        col_index = {n: i for i, (n, _t) in enumerate(cols)}

        def value_at(arc_reader, name, row):
            return json.loads("".join(stream_text(
                arc_reader.read_row_column(name, row), arc_reader.pool)))

        with ArchiveReader(a) as ra, ArchiveReader(b) as rb:
            for ev in read_ledger(ledger):
                ident = ev["identity"]["id"]
                if ev["op"] in ("removed", "changed"):
                    self.assertEqual(value_at(ra, "id", ev["before_row"]),
                                     ident)
                if ev["op"] in ("added", "changed"):
                    self.assertEqual(value_at(rb, "id", ev["after_row"]),
                                     ident)
                for cn, pair in ev.get("changes", {}).items():
                    self.assertEqual(
                        value_at(ra, cn, ev["before_row"]), pair["before"])
                    self.assertEqual(
                        value_at(rb, cn, ev["after_row"]), pair["after"])


class MemoryBoundTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()

    def tearDown(self):
        self.ws.cleanup()

    def test_many_rows_peak_is_flat(self):
        n = 12000
        cols = [("id", "integer"), ("name", "string"), ("v", "integer")]
        rng = random.Random(7)
        before = [(i, "dev-%09d" % i, rng.randrange(1 << 30))
                   for i in range(n)]
        after = [(i, "dev-%09d" % i,
                  before[i][2] + (1 if i % 500 == 0 else 0))
                 for i in range(n)]
        a = restore_table(self.ws, "mb", cols, before)
        b = restore_table(self.ws, "ma", cols, after)
        out = os.path.join(self.ws.root, "mem.jsonl")
        tracemalloc.start()
        try:
            rep = compare_archives(a, b, {"identity_fields": ["id"]},
                                   out=out, verify=False)
            _cur, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(rep["summary"]["changed"], n // 500)
        self.assertLess(peak, 8 * 1024 * 1024,
                        "compare peak allocation %d too high" % peak)


class CliCompareTests(unittest.TestCase):
    """Subprocess CLI tests mirroring tests/test_cli.py isolation."""

    def setUp(self):
        self.ws = Workspace()
        self.cols = [("id", "string"), ("v", "integer")]
        self.a_rows = [("D1", 1), ("D2", 2), ("D3", 3)]
        self.b_rows = [("D1", 1), ("D2", 22), ("D4", 4)]
        self.a = restore_table(self.ws, "cli-a", self.cols, self.a_rows)
        self.b = restore_table(self.ws, "cli-b", self.cols, self.b_rows)
        self.spec = write_spec(self.ws, {"identity_fields": ["id"]})
        self.ledger = os.path.join(self.ws.root, "out.jsonl")

    def tearDown(self):
        self.ws.cleanup()

    def _cli(self, *args):
        return cli(*args)

    def test_cli_compare_writes_ledger(self):
        p = self._cli("compare", self.a, self.b, "--spec", self.spec,
                      "--out", self.ledger)
        self.assertEqual(p.returncode, 0, p.stderr + p.stdout)
        recs = list(read_ledger(self.ledger))
        ops = {r["id"]: r["op"] for r in recs}
        self.assertEqual(ops, {"D2": "changed", "D3": "removed",
                               "D4": "added"})

    def test_cli_positional_spec_and_diff_alias(self):
        p = self._cli("diff", self.a, self.b, self.spec,
                      "--out", self.ledger)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(len(list(read_ledger(self.ledger))), 3)

    def test_cli_identity_failure_exit_2_and_no_ledger(self):
        bad = restore_table(
            self.ws, "cli-bad", self.cols,
            [("D1", 1), ("D1", 2), (None, 3)])
        problems = os.path.join(self.ws.root, "problems.jsonl")
        p = self._cli("compare", bad, bad, "--spec", self.spec,
                      "--out", self.ledger, "--problems", problems)
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertFalse(os.path.exists(self.ledger))
        with open(problems, encoding="utf-8") as fh:
            lines = [json.loads(l) for l in fh]
        kinds = {l["kind"] for l in lines}
        self.assertIn("duplicate_identity", kinds)
        self.assertIn("empty_identity", kinds)

    def test_cli_recheck_detects_tampering(self):
        p = self._cli("compare", self.a, self.b, "--spec", self.spec,
                      "--out", self.ledger)
        self.assertEqual(p.returncode, 0, p.stderr)
        p = self._cli("recheck", self.ledger, "--before", self.a,
                      "--after", self.b, "--spec", self.spec)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        # Tamper one value in the ledger: recheck must fail.
        with open(self.ledger, encoding="utf-8") as fh:
            recs = [json.loads(l) for l in fh]
        for r in recs:
            if r["op"] == "changed":
                r["changes"]["v"]["after"] = 999
        tampered = os.path.join(self.ws.root, "tampered.jsonl")
        with open(tampered, "w", encoding="utf-8") as fh:
            for r in recs:
                fh.write(json.dumps(r) + "\n")
        p = self._cli("recheck", tampered, "--before", self.a,
                      "--after", self.b, "--spec", self.spec)
        self.assertNotEqual(p.returncode, 0)

    def test_cli_help_lists_compare(self):
        p = self._cli("compare", "--help")
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertIn("--identity-field", p.stdout)


if __name__ == "__main__":
    unittest.main()
