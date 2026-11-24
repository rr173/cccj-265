"""CLI-level tests: exit codes, JSON output, verify/query/report."""

import json
import os
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from helpers import Workspace, make_replica, write_manifest  # noqa: E402

from archrestore import synth  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


LAUNCHER = (
    "import sys; sys.path.insert(0, %r); "
    "from archrestore.cli import main; sys.exit(main(sys.argv[1:]))" % ROOT
)


def cli(*args, **kw):
    # -I keeps the interpreter isolated (replica directories are untrusted),
    # so the package location is injected explicitly rather than by cwd or
    # PYTHONPATH.
    env = dict(os.environ)
    env.pop("PYTHONSTARTUP", None)
    env.pop("PYTHONPATH", None)
    return subprocess.run(
        [sys.executable, "-I", "-c", LAUNCHER] + list(args),
        capture_output=True, text=True, env=env, cwd=kw.get("cwd", ROOT))


class CliTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()
        self.ds = synth.LogicalDataset(
            [("id", "integer"), ("name", "string")], row_count=60, seed=4,
            patterns={"name": "unique"})
        _m, self.manifest = write_manifest(
            self.ds, {"id": [30, 30], "name": [60]}, self.ws.root)
        self.r1 = make_replica(self.ws.root, self.ds, "r1", {
            "id": [(0, 60)], "name": [(0, 60)]}, encodings=("plain", "dict"))
        self.out = os.path.join(self.ws.root, "out")

    def tearDown(self):
        self.ws.cleanup()

    def restore(self, *extra, out=None):
        return cli("restore", "--manifest", self.manifest,
                   "--replica", "r1=%s" % self.r1[1],
                   "--out", out or self.out, *extra)

    def test_restore_verify_query_cycle(self):
        p = self.restore()
        self.assertEqual(p.returncode, 0, p.stderr + p.stdout)
        self.assertIn("outcome: restored", p.stdout)

        p = cli("verify", "--archive", self.out)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertIn("OK", p.stdout)

        p = cli("query", "--archive", self.out, "--column", "name",
                "--rows", "10:12")
        self.assertEqual(p.returncode, 0, p.stderr)
        rows = json.loads(p.stdout)
        self.assertEqual(rows, [{"name": self.ds.value(1, 10)},
                                {"name": self.ds.value(1, 11)}])

        p = cli("report", self.out, "--json")
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(json.loads(p.stdout)["outcome"], "restored")

    def test_incomplete_exit_code_and_diagnostic_json(self):
        # Remove one replica page from the descriptor: rows become missing.
        rep_path = os.path.join(self.r1[1], "replica.json")
        with open(rep_path) as fh:
            rep = json.load(fh)
        rep["pages"] = [p for p in rep["pages"]
                        if not (p["column"] == "id" and p["start"] == 0)]
        with open(rep_path, "w") as fh:
            json.dump(rep, fh)
        p = self.restore("--json")
        self.assertEqual(p.returncode, 3, p.stderr + p.stdout)
        result = json.loads(p.stdout)
        self.assertEqual(result["outcome"], "incomplete")
        self.assertFalse(os.path.exists(os.path.join(self.out, "archive.json")))
        self.assertTrue(os.path.exists(
            os.path.join(self.out, "diagnostics.json")))
        slot = [s for s in result["slots"]
                if s["column"] == "id" and s["start"] == 0][0]
        self.assertEqual(slot["status"], "missing")

    def test_bad_manifest_exit_code(self):
        with open(self.manifest) as fh:
            man = json.load(fh)
        man["slots"] = []
        with open(self.manifest, "w") as fh:
            json.dump(man, fh)
        p = self.restore()
        self.assertEqual(p.returncode, 2, p.stderr + p.stdout)
        self.assertIn("bad_manifest", p.stdout)

    def test_verify_detects_tampering(self):
        p = self.restore()
        self.assertEqual(p.returncode, 0, p.stderr)
        with open(os.path.join(self.out, "archive.json")) as fh:
            cmeta = json.load(fh)["columns"][0]
        path = os.path.join(self.out, cmeta["file"])
        with open(path, "r+b") as fh:
            b = fh.read(1)
            fh.seek(0)
            fh.write(bytes([b[0] ^ 0xFF]))
        p = cli("verify", "--archive", self.out)
        self.assertEqual(p.returncode, 4, p.stdout)
        self.assertIn("FAILED", p.stdout)


if __name__ == "__main__":
    unittest.main()
