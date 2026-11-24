"""Tests for bound JSONL package materialization."""

import json
import os
import random
import subprocess
import sys
import tracemalloc
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

from helpers import Workspace  # noqa: E402

from archrestore import synth  # noqa: E402
from archrestore.archive import ArchiveReader, verify_archive  # noqa: E402
from archrestore.materialize import (  # noqa: E402
    IdentityConflictError,
    MaterializeError,
    PackageFormatError,
    archive_content_digest,
    materialize_archive,
)
from archrestore.pipeline import run_repair  # noqa: E402

LAUNCHER = (
    "import sys; sys.path.insert(0, %r); "
    "from archrestore.cli import main; sys.exit(main(sys.argv[1:]))" % ROOT
)


def restore_table(ws, name, columns, rows):
    class Dataset(object):
        pass

    ds = Dataset()
    ds.columns = list(columns)
    ds.row_count = len(rows)
    ds.value = lambda ci, row: rows[row][ci]

    manifest = synth.build_manifest(ds, [max(1, len(rows))])
    manifest_path = os.path.join(ws.root, name + "-manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh)
    replica = os.path.join(ws.root, name + "-replica")
    plan = {n: ([(0, len(rows))] if rows else []) for n, _t in columns}
    synth.build_replica(replica, ds, plan, [13, 7], ["plain"],
                        random.Random(3))
    archive = os.path.join(ws.root, name + "-archive")
    report = run_repair(
        manifest_path, [(os.path.basename(replica), replica)], archive)
    assert report["outcome"] == "restored", report.get("error")
    return archive


def write_package(path, baseline, operations, identity_fields=("id",),
                  digest=None, meta=None):
    envelope = dict(meta or {})
    envelope.setdefault("baseline_digest",
                        digest or archive_content_digest(baseline))
    envelope.setdefault("identity_fields", list(identity_fields))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(envelope, ensure_ascii=False) + "\n")
        for op in operations:
            fh.write(json.dumps(op, ensure_ascii=False) + "\n")
    return path


def read_rows(archive):
    with ArchiveReader(archive) as reader:
        rows = []
        for row in range(reader.header["row_count"]):
            values = {}
            for cmeta in reader.header["columns"]:
                raw = reader.read_row_column(cmeta["name"], row)
                if raw.tag == 0:
                    value = None
                elif raw.tag == 2:
                    value = int.from_bytes(raw.inline, "little", signed=True)
                else:
                    value = b"".join(raw.iter_chunks(reader.pool)).decode()
                values[cmeta["name"]] = value
            rows.append(values)
        return rows


class MaterializeSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()
        self.columns = [("id", "integer"), ("name", "string"),
                        ("qty", "integer")]
        self.rows = [(3, "c", 30), (1, "a", 10), (2, "b", 20),
                     (4, "d", 40)]
        self.base = restore_table(self.ws, "base", self.columns, self.rows)
        self.package = os.path.join(self.ws.root, "changes.jsonl")
        self.out = os.path.join(self.ws.root, "next")

    def tearDown(self):
        self.ws.cleanup()

    def test_apply_add_delete_update_sorts_and_verifies(self):
        write_package(self.package, self.base, [
            {"op": "update", "identity": {"id": 2},
             "values": {"qty": 22}},
            {"op": "delete", "identity": {"id": 4}},
            {"op": "add", "identity": {"id": 5},
             "row": {"id": 5, "name": "e", "qty": 50}},
        ])
        report = materialize_archive(self.base, self.package, self.out)
        self.assertEqual(report["summary"]["added"], 1)
        self.assertEqual(report["summary"]["deleted"], 1)
        self.assertEqual(report["summary"]["updated"], 1)
        result = verify_archive(self.out)
        self.assertTrue(result.ok, result.problems)
        self.assertEqual(read_rows(self.out), [
            {"id": 1, "name": "a", "qty": 10},
            {"id": 2, "name": "b", "qty": 22},
            {"id": 3, "name": "c", "qty": 30},
            {"id": 5, "name": "e", "qty": 50},
        ])

    def test_composite_utf8_and_numeric_order(self):
        columns = [("g", "string"), ("n", "integer"), ("v", "string")]
        rows = [("b", 2, "x"), ("a", 10, "x"), ("a", 2, "x"),
                ("ä", 1, "u"), ("z", -5, "z")]
        base = restore_table(self.ws, "composite", columns, rows)
        package = os.path.join(self.ws.root, "composite.jsonl")
        write_package(package, base, [
            {"op": "add", "identity": {"g": "a", "n": 1},
             "row": {"g": "a", "n": 1, "v": "new"}},
        ], identity_fields=("g", "n"))
        out = os.path.join(self.ws.root, "composite-out")
        materialize_archive(base, package, out)
        keys = [(r["g"], r["n"]) for r in read_rows(out)]
        expected = sorted([(r[0], r[1]) for r in rows] + [("a", 1)],
                          key=lambda x: (x[0].encode("utf-8"), x[1]))
        self.assertEqual(keys, expected)
        self.assertTrue(verify_archive(out).ok)

    def test_conflicts_and_bad_package_publish_nothing(self):
        cases = [
            ("digest", {"baseline_digest": "0" * 64},
             [{"op": "delete", "identity": {"id": 1}}],
             MaterializeError),
            ("missing", {},
             [{"op": "delete", "identity": {"id": 99}}],
             IdentityConflictError),
            ("add-existing", {},
             [{"op": "add", "identity": {"id": 1},
               "row": {"id": 1, "name": "x", "qty": 1}}],
             IdentityConflictError),
            ("duplicate-op", {},
             [{"op": "delete", "identity": {"id": 1}},
              {"op": "update", "identity": {"id": 1},
               "values": {"qty": 2}}],
             IdentityConflictError),
            ("bad-type", {},
             [{"op": "update", "identity": {"id": 1},
               "values": {"qty": "not-an-int"}}],
             PackageFormatError),
        ]
        for name, meta, ops, exc_type in cases:
            package = os.path.join(self.ws.root, name + ".jsonl")
            digest = None if "baseline_digest" in meta else None
            write_package(package, self.base, ops, digest=digest, meta=meta)
            out = os.path.join(self.ws.root, name + "-out")
            with self.assertRaises(exc_type):
                materialize_archive(self.base, package, out)
            self.assertFalse(os.path.exists(out), name)

    def test_baseline_null_and_duplicate_identities_rejected(self):
        null_base = restore_table(
            self.ws, "nulls", self.columns,
            [(1, "a", 1), (None, "b", 2)])
        package = os.path.join(self.ws.root, "nulls.jsonl")
        write_package(package, null_base, [])
        with self.assertRaises(IdentityConflictError):
            materialize_archive(null_base, package,
                               os.path.join(self.ws.root, "nulls-out"))

        dup_base = restore_table(
            self.ws, "dups", self.columns,
            [(1, "a", 1), (1, "b", 2)])
        package = os.path.join(self.ws.root, "dups.jsonl")
        write_package(package, dup_base, [])
        with self.assertRaises(IdentityConflictError):
            materialize_archive(dup_base, package,
                               os.path.join(self.ws.root, "dups-out"))


class MaterializeCliTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()
        self.base = restore_table(
            self.ws, "cli", [("id", "integer"), ("v", "string")],
            [(1, "a"), (2, "b")])
        self.package = write_package(
            os.path.join(self.ws.root, "changes.jsonl"), self.base,
            [{"op": "update", "id": 1, "values": {"v": "A"}}])
        self.out = os.path.join(self.ws.root, "next")

    def tearDown(self):
        self.ws.cleanup()

    def cli(self, *args):
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        return subprocess.run(
            [sys.executable, "-I", "-c", LAUNCHER] + list(args),
            capture_output=True, text=True, env=env, cwd=ROOT)

    def test_cli_materialize_verify_and_query(self):
        result = self.cli("materialize", self.base, self.package, self.out)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        verified = self.cli("verify", "--archive", self.out)
        self.assertEqual(verified.returncode, 0, verified.stdout)
        queried = self.cli("query", "--archive", self.out, "--rows", "0:2")
        self.assertEqual(queried.returncode, 0, queried.stderr)
        rows = json.loads(queried.stdout)
        self.assertEqual([r["id"] for r in rows], [1, 2])
        self.assertEqual(rows[0]["v"], "A")

    def test_cli_digest_mismatch_exit_2(self):
        bad = os.path.join(self.ws.root, "bad.jsonl")
        write_package(bad, self.base, [], meta={
            "baseline_digest": "0" * 64,
            "identity_fields": ["id"],
        })
        result = self.cli("apply", "--baseline", self.base,
                          "--package", bad, "--out", self.out)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertFalse(os.path.exists(self.out))


class MaterializeMemoryTests(unittest.TestCase):
    def tearDown(self):
        if hasattr(self, "ws"):
            self.ws.cleanup()

    def test_many_rows_peak_is_flat(self):
        self.ws = Workspace()
        n = 12000
        rng = random.Random(11)
        columns = [("id", "integer"), ("name", "string"),
                   ("v", "integer")]
        rows = [(i, "dev-%09d" % i, rng.randrange(1 << 30))
                for i in range(n)]
        base = restore_table(self.ws, "memory", columns, rows)
        package = write_package(
            os.path.join(self.ws.root, "memory.jsonl"), base,
            [{"op": "update", "identity": {"id": i},
              "values": {"v": -1}} for i in range(0, n, 500)])
        out = os.path.join(self.ws.root, "memory-out")
        tracemalloc.start()
        try:
            report = materialize_archive(base, package, out)
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(report["summary"]["updated"], n // 500)
        self.assertTrue(verify_archive(out).ok)
        self.assertLess(peak, 8 * 1024 * 1024, peak)


if __name__ == "__main__":
    unittest.main()
