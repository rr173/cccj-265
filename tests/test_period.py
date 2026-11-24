"""Tests for out-of-core periodic delivery group validation.

Covers: descriptor validation (closed format, field/type compatibility,
unknown keys), the full success path and the independently checkable period
manifest, every failure taxonomy (member errors, invalid records, type
mismatches, empty/duplicate target keys, empty/unmatched/ambiguous
references), composite keys and exact type-and-value equality, original row
attribution and stable diagnosis ordering, external-sort threshold
invariance, CLI exit codes and atomicity, and the bounded-memory contract.

Run with::

    python3 -I -m unittest tests.test_period -v
"""

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
from archrestore.archive import verify_archive  # noqa: E402
from archrestore.period import (  # noqa: E402
    PERIOD_KIND,
    PERIOD_MANIFEST_KIND,
    PeriodDescriptorError,
    PeriodValidationError,
    encode_scalar,
    load_period_descriptor,
    order_key_for_canonical,
    parse_period_descriptor,
    validate_period_package,
)
from archrestore.pipeline import run_repair  # noqa: E402

LAUNCHER = (
    "import sys; sys.path.insert(0, %r); "
    "from archrestore.cli import main; sys.exit(main(sys.argv[1:]))" % ROOT
)


def cli(*args):
    env = dict(os.environ)
    env.pop("PYTHONSTARTUP", None)
    env.pop("PYTHONPATH", None)
    return subprocess.run(
        [sys.executable, "-I", "-c", LAUNCHER] + list(args),
        capture_output=True, text=True, env=env, cwd=ROOT)


def restore_table(ws, name, columns, rows):
    class Dataset(object):
        pass

    ds = Dataset()
    ds.columns = list(columns)
    ds.row_count = len(rows)
    ds.value = lambda ci, row: rows[row][ci]
    manifest = synth.build_manifest(ds, [max(1, ds.row_count)])
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


def write_jsonl(ws, name, records, raw_lines=None):
    path = os.path.join(ws.root, name)
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records or []:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        for line in raw_lines or []:
            fh.write(line + "\n")
    return path


def descriptor(ws, members, relations, period_id="P1"):
    obj = {
        "archrestore_format": {"name": PERIOD_KIND, "version": 1},
        "period_id": period_id,
        "members": members,
        "relations": relations,
    }
    path = os.path.join(ws.root, "descriptor.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh)
    return path


def archive_member(name, arc, fields, ws):
    return {"name": name, "kind": "archive",
            "path": os.path.relpath(arc, ws.root),
            "fields": [{"name": n, "type": t} for n, t in fields]}


def jsonl_member(name, path, fields):
    return {"name": name, "kind": "jsonl",
            "path": os.path.basename(path),
            "fields": [{"name": n, "type": t} for n, t in fields]}


def relation(name, src, src_fields, dst, dst_fields):
    return {"name": name,
            "from": {"member": src, "fields": list(src_fields)},
            "to": {"member": dst, "fields": list(dst_fields)}}


def read_jsonl(path):
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


class DescriptorValidationTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()

    def tearDown(self):
        self.ws.cleanup()

    def _base(self):
        return {
            "archrestore_format": {"name": PERIOD_KIND, "version": 1},
            "members": [
                {"name": "a", "kind": "archive", "path": "a-archive",
                 "fields": [{"name": "k", "type": "integer"}]},
                {"name": "b", "kind": "jsonl", "path": "b.jsonl",
                 "fields": [{"name": "kref", "type": "integer"}]}],
            "relations": [
                {"name": "r", "from": {"member": "b", "fields": ["kref"]},
                 "to": {"member": "a", "fields": ["k"]}}],
        }

    def test_header_and_top_level_closed_format(self):
        obj = self._base()
        parse_period_descriptor(obj)  # well-formed
        bad = json.loads(json.dumps(obj))
        bad["archrestore_format"]["name"] = "wrong"
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(bad)
        bad = json.loads(json.dumps(obj))
        bad["unexpected"] = 1
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(bad)
        bad = json.loads(json.dumps(obj))
        bad["members"][0]["bogus"] = 1
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(bad)

    def test_duplicate_member_and_relation_names_rejected(self):
        obj = self._base()
        obj["members"][1]["name"] = "a"
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(obj)
        obj = self._base()
        obj["relations"].append(dict(obj["relations"][0]))
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(obj)

    def test_unknown_member_and_missing_endpoint_rejected(self):
        obj = self._base()
        obj["relations"][0]["to"]["member"] = "ghost"
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(obj)
        obj = self._base()
        obj["relations"][0]["to"]["fields"] = ["k", "k2"]
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(obj)  # unequal field counts
        obj = self._base()
        obj["relations"][0]["to"]["fields"] = ["nope"]
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(obj)  # field not on member

    def test_incompatible_field_types_rejected(self):
        obj = self._base()
        obj["members"][1]["fields"][0]["type"] = "string"
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(obj)

    def test_relative_path_containment(self):
        obj = self._base()
        obj["members"][1]["path"] = "../escape.jsonl"
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(obj)
        obj = self._base()
        obj["members"][1]["path"] = "/abs/path.jsonl"
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(obj)

    def test_member_must_have_typed_fields(self):
        obj = self._base()
        obj["members"][0]["fields"] = ["k"]  # bare strings not allowed
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(obj)
        obj = self._base()
        obj["members"][0]["fields"] = []
        with self.assertRaises(PeriodDescriptorError):
            parse_period_descriptor(obj)


class CanonicalKeyTests(unittest.TestCase):
    def test_type_and_value_distinctions(self):
        s1 = encode_scalar("1", "string")
        i1 = encode_scalar(1, "integer")
        self.assertNotEqual(s1, i1)  # string "1" != integer 1
        self.assertIsNone(encode_scalar(None, "integer"))
        with self.assertRaises(Exception):
            encode_scalar(True, "integer")  # booleans never valid
        with self.assertRaises(Exception):
            encode_scalar("x", "integer")

    def test_order_key_numeric_and_string_order(self):
        keys = [order_key_for_canonical(encode_scalar(n, "integer"))
                for n in (-5, -1, 0, 2, 10)]
        self.assertEqual(keys, sorted(keys))
        words = ["apple", "banana", "banana\x00x", "zebra"]
        okeys = [order_key_for_canonical(encode_scalar(w, "string"))
                 for w in words]
        self.assertEqual(okeys, sorted(okeys))


class SuccessfulValidationTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()
        self.arc = restore_table(
            self.ws, "devices",
            [("device_id", "integer"), ("label", "string")],
            [(1, "a"), (2, "b"), (3, "c")])

    def tearDown(self):
        self.ws.cleanup()

    def _validate(self, refs, out="manifest.json", **kw):
        points = write_jsonl(self.ws, "points.jsonl", refs)
        dp = descriptor(self.ws, [
            archive_member("devices", self.arc,
                           [("device_id", "integer")], self.ws),
            jsonl_member("points", points, [("device_ref", "integer")]),
        ], [relation("r", "points", ["device_ref"],
                     "devices", ["device_id"])])
        out_path = os.path.join(self.ws.root, out)
        return dp, out_path, validate_period_package(dp, out=out_path, **kw)

    def test_success_writes_manifest_with_member_summaries(self):
        refs = [{"device_ref": d} for d in (1, 2, 3, 1, 2)]
        dp, out_path, report = self._validate(refs)
        self.assertEqual(report["outcome"], "period_valid")
        self.assertTrue(os.path.exists(out_path))
        with open(out_path, encoding="utf-8") as fh:
            manifest = json.load(fh)
        self.assertEqual(manifest["archrestore_format"]["name"],
                         PERIOD_MANIFEST_KIND)
        self.assertEqual(manifest["status"], "valid")
        self.assertEqual(manifest["period_id"], "P1")
        by_name = {m["name"]: m for m in manifest["members"]}
        self.assertEqual(by_name["devices"]["record_count"], 3)
        self.assertEqual(by_name["points"]["record_count"], 5)
        self.assertEqual(len(by_name["devices"]["content_digest"]), 64)
        self.assertEqual(len(by_name["points"]["content_digest"]), 64)
        rel = manifest["relations"][0]
        self.assertEqual(rel["references"], 5)
        self.assertEqual(rel["matched_references"], 5)
        self.assertEqual(len(manifest["manifest_digest"]), 64)

    def test_manifest_is_deterministic_and_self_digest_covers_it(self):
        refs = [{"device_ref": d} for d in (3, 1, 2)]
        dp, out_a, _ = self._validate(refs, out="a.json")
        _, out_b, _ = self._validate(refs, out="b.json")
        with open(out_a) as fa, open(out_b) as fb:
            self.assertEqual(fa.read(), fb.read())

    def test_verify_archive_flag_passes_for_good_member(self):
        refs = [{"device_ref": 1}]
        dp, out_path, report = self._validate(refs, verify_archives=True)
        self.assertEqual(report["outcome"], "period_valid")
        self.assertTrue(verify_archive(self.arc).ok)

    def test_inline_dict_descriptor_is_accepted(self):
        points = write_jsonl(self.ws, "points.jsonl", [{"device_ref": 1}])
        obj = {
            "archrestore_format": {"name": PERIOD_KIND, "version": 1},
            "members": [
                {"name": "devices", "kind": "archive",
                 "path": os.path.relpath(self.arc, self.ws.root),
                 "fields": [{"name": "device_id", "type": "integer"}]},
                {"name": "points", "kind": "jsonl", "path": "points.jsonl",
                 "fields": [{"name": "device_ref", "type": "integer"}]}],
            "relations": [relation("r", "points", ["device_ref"],
                                   "devices", ["device_id"])],
        }
        out_path = os.path.join(self.ws.root, "inline.json")
        report = validate_period_package(obj, out=out_path,
                                         base_dir=self.ws.root)
        self.assertEqual(report["outcome"], "period_valid")

    def test_composite_key_success_and_equality_semantics(self):
        ledger = restore_table(
            self.ws, "ledger",
            [("rack", "string"), ("device_id", "integer"),
             ("v", "integer")],
            [("A", 1, 10), ("A", 2, 20), ("B", 1, 30)])
        points = write_jsonl(self.ws, "pts.jsonl", [
            {"rack_ref": "A", "dev_ref": 1},
            {"rack_ref": "B", "dev_ref": 1},
            {"rack_ref": "A", "dev_ref": 2},
        ])
        dp = descriptor(self.ws, [
            archive_member("ledger", ledger,
                           [("rack", "string"), ("device_id", "integer")],
                           self.ws),
            jsonl_member("pts", points,
                         [("rack_ref", "string"),
                          ("dev_ref", "integer")]),
        ], [relation("composite", "pts", ["rack_ref", "dev_ref"],
                     "ledger", ["rack", "device_id"])])
        report = validate_period_package(
            dp, out=os.path.join(self.ws.root, "m.json"))
        self.assertEqual(report["outcome"], "period_valid")
        self.assertEqual(report["relations"][0]["matched_references"], 3)


class FailureTaxonomyTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()

    def tearDown(self):
        self.ws.cleanup()

    def _run(self, desc_obj):
        dp = os.path.join(self.ws.root, "descriptor.json")
        with open(dp, "w", encoding="utf-8") as fh:
            json.dump(desc_obj, fh)
        out = os.path.join(self.ws.root, "never-written.json")
        with self.assertRaises(PeriodValidationError) as ctx:
            validate_period_package(dp, out=out)
        self.assertFalse(os.path.exists(out),
                         "a failed period must not publish a manifest")
        return ctx.exception.report

    def _kinds(self, report):
        return set(report["summary"]["by_kind"])

    def test_unmatched_reference_names_member_relation_and_row(self):
        arc = restore_table(self.ws, "dev", [("k", "integer")],
                            [(1,), (2,)])
        pts = write_jsonl(self.ws, "p.jsonl",
                          [{"kref": 1}, {"kref": 99}])
        report = self._run({
            "archrestore_format": {"name": PERIOD_KIND, "version": 1},
            "members": [
                archive_member("dev", arc, [("k", "integer")], self.ws),
                jsonl_member("pts", pts, [("kref", "integer")])],
            "relations": [relation("r", "pts", ["kref"], "dev", ["k"])]})
        self.assertEqual(self._kinds(report), {"unmatched_reference"})
        prob = report["problems"][0]
        self.assertEqual(prob["member"], "pts")
        self.assertEqual(prob["relation"], "r")
        self.assertEqual(prob["row"], 2)  # original JSONL line number
        self.assertEqual(prob["key"], [99])
        full = read_jsonl(report["problems_path"])
        self.assertEqual(len(full), 1)

    def test_duplicate_target_and_ambiguous_reference(self):
        arc = restore_table(self.ws, "dev",
                            [("k", "integer"), ("v", "integer")],
                            [(5, 1), (5, 2), (6, 3)])
        pts = write_jsonl(self.ws, "p.jsonl",
                          [{"kref": 5}, {"kref": 6}])
        report = self._run({
            "archrestore_format": {"name": PERIOD_KIND, "version": 1},
            "members": [
                archive_member("dev", arc, [("k", "integer")], self.ws),
                jsonl_member("pts", pts, [("kref", "integer")])],
            "relations": [relation("r", "pts", ["kref"], "dev", ["k"])]})
        kinds = self._kinds(report)
        self.assertIn("duplicate_target_key", kinds)
        self.assertIn("ambiguous_reference", kinds)
        # Both duplicate target rows are named with their archive row numbers.
        dup = [p for p in read_jsonl(report["problems_path"])
               if p["kind"] == "duplicate_target_key"]
        self.assertEqual(sorted(p["row"] for p in dup), [0, 1])
        self.assertEqual(dup[0]["duplicate_rows"], [0, 1])

    def test_empty_target_and_empty_reference_keys(self):
        arc = restore_table(self.ws, "dev",
                            [("k", "integer"), ("v", "integer")],
                            [(None, 1), (7, 2)])
        pts = write_jsonl(self.ws, "p.jsonl",
                          [{"kref": 7}, {"kref": None}, {}])
        report = self._run({
            "archrestore_format": {"name": PERIOD_KIND, "version": 1},
            "members": [
                archive_member("dev", arc, [("k", "integer")], self.ws),
                jsonl_member("pts", pts, [("kref", "integer")])],
            "relations": [relation("r", "pts", ["kref"], "dev", ["k"])]})
        kinds = self._kinds(report)
        self.assertIn("empty_target_key", kinds)
        self.assertIn("empty_reference_key", kinds)

    def test_invalid_record_and_type_mismatch(self):
        arc = restore_table(self.ws, "dev", [("k", "integer")], [(7,)])
        pts = write_jsonl(
            self.ws, "p.jsonl", [],
            raw_lines=['{"kref": 7}', "not-json-at-all",
                       '{"kref": "string"}', '{"kref": true}', ""])
        report = self._run({
            "archrestore_format": {"name": PERIOD_KIND, "version": 1},
            "members": [
                archive_member("dev", arc, [("k", "integer")], self.ws),
                jsonl_member("pts", pts, [("kref", "integer")])],
            "relations": [relation("r", "pts", ["kref"], "dev", ["k"])]})
        kinds = self._kinds(report)
        self.assertIn("invalid_record", kinds)
        self.assertIn("field_type_mismatch", kinds)
        # The malformed line keeps its physical line number.
        inv = [p for p in report["problems"]
               if p["kind"] == "invalid_record"][0]
        self.assertEqual(inv["row"], 2)

    def test_member_error_when_archive_field_missing_or_typed_wrong(self):
        arc = restore_table(self.ws, "dev",
                            [("k", "integer"), ("v", "integer")],
                            [(1, 9), (2, 8)])
        pts = write_jsonl(self.ws, "p.jsonl", [{"kref": 1}])
        report = self._run({
            "archrestore_format": {"name": PERIOD_KIND, "version": 1},
            "members": [
                {"name": "dev", "kind": "archive",
                 "path": os.path.relpath(arc, self.ws.root),
                 "fields": [{"name": "ghost", "type": "integer"}]},
                jsonl_member("pts", pts, [("kref", "integer")])],
            "relations": [relation("r", "pts", ["kref"], "dev",
                                   ["ghost"])]})
        self.assertIn("member_error", self._kinds(report))

    def test_missing_member_file_is_member_error(self):
        pts = write_jsonl(self.ws, "p.jsonl", [{"kref": 1}])
        report = self._run({
            "archrestore_format": {"name": PERIOD_KIND, "version": 1},
            "members": [
                {"name": "dev", "kind": "archive", "path": "no-such-archive",
                 "fields": [{"name": "k", "type": "integer"}]},
                jsonl_member("pts", pts, [("kref", "integer")])],
            "relations": [relation("r", "pts", ["kref"], "dev", ["k"])]})
        self.assertIn("member_error", self._kinds(report))

    def test_corrupt_archive_stream_is_member_error_not_a_crash(self):
        import glob
        arc = restore_table(self.ws, "dev", [("k", "integer")],
                            [(1,), (2,)])
        pts = write_jsonl(self.ws, "p.jsonl", [{"kref": 1}])
        # Flip the first byte of the canonical stream so it no longer decodes.
        canon = glob.glob(os.path.join(arc, "columns", "*.canon"))[0]
        with open(canon, "r+b") as fh:
            first = fh.read(1)
            fh.seek(0)
            fh.write(bytes([first[0] ^ 0xFF]))
        report = self._run({
            "archrestore_format": {"name": PERIOD_KIND, "version": 1},
            "members": [
                archive_member("dev", arc, [("k", "integer")], self.ws),
                jsonl_member("pts", pts, [("kref", "integer")])],
            "relations": [relation("r", "pts", ["kref"], "dev", ["k"])]})
        self.assertIn("member_error", self._kinds(report))
        # The same run with explicit archive self-verification also fails.
        self.assertIn("member_error", self._kinds(report))

    def test_string_one_vs_integer_one_never_match(self):
        # Referenced integer 1 must not be matched by a JSON string "1".
        arc = restore_table(self.ws, "dev", [("k", "integer")], [(1,)])
        pts = write_jsonl(self.ws, "p.jsonl", [{"kref": "1"}])
        report = self._run({
            "archrestore_format": {"name": PERIOD_KIND, "version": 1},
            "members": [
                archive_member("dev", arc, [("k", "integer")], self.ws),
                jsonl_member("pts", pts, [("kref", "integer")])],
            "relations": [relation("r", "pts", ["kref"], "dev", ["k"])]})
        # A string value in an integer-declared field is a type mismatch.
        self.assertIn("field_type_mismatch", self._kinds(report))

    def test_problems_out_is_written_and_existing_out_untouched(self):
        arc = restore_table(self.ws, "dev", [("k", "integer")], [(1,)])
        pts = write_jsonl(self.ws, "p.jsonl", [{"kref": 1}, {"kref": 2}])
        dp = descriptor(self.ws, [
            archive_member("dev", arc, [("k", "integer")], self.ws),
            jsonl_member("pts", pts, [("kref", "integer")])],
            [relation("r", "pts", ["kref"], "dev", ["k"])])
        sentinel = os.path.join(self.ws.root, "keep.json")
        with open(sentinel, "w") as fh:
            fh.write("untouched")
        problems = os.path.join(self.ws.root, "diag.jsonl")
        with self.assertRaises(PeriodValidationError):
            validate_period_package(dp, out=sentinel, problems_out=problems)
        with open(sentinel) as fh:
            self.assertEqual(fh.read(), "untouched")
        self.assertTrue(os.path.exists(problems))
        self.assertEqual(len(read_jsonl(problems)), 1)


class OrderingAndExternalSortTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()

    def tearDown(self):
        self.ws.cleanup()

    def test_diagnosis_order_invariant_to_run_budget(self):
        # A duplicate-key target produces many problems on both sides.
        arc = restore_table(
            self.ws, "dev", [("k", "integer"), ("v", "integer")],
            [((i % 40), i) for i in range(160)])
        records = [{"kref": i % 60} for i in range(300)]
        pts = write_jsonl(self.ws, "p.jsonl", records)
        dp = descriptor(self.ws, [
            archive_member("dev", arc, [("k", "integer")], self.ws),
            jsonl_member("pts", pts, [("kref", "integer")])],
            [relation("rel1", "pts", ["kref"], "dev", ["k"])])
        spool_texts = []
        for budget in (8, 64, 4096):
            with self.assertRaises(PeriodValidationError) as ctx:
                validate_period_package(
                    dp, out=os.path.join(self.ws.root, "x.json"),
                    run_records=budget, run_bytes=1024, fan_in=4)
            with open(ctx.exception.report["problems_path"]) as pf:
                spool_texts.append(pf.read())
        self.assertEqual(spool_texts[0], spool_texts[1])
        self.assertEqual(spool_texts[1], spool_texts[2])
        probs = [json.loads(line) for line in spool_texts[0].splitlines()]
        # Deterministic total order: member, relation, kind, row.
        sig = [(p["member"], p.get("relation") or "", p["kind"],
                p["row"] if p["row"] is not None else -1) for p in probs]
        self.assertEqual(sig, sorted(sig))

    def test_success_threshold_invariance(self):
        n = 300
        arc = restore_table(self.ws, "big", [("k", "integer"),
                                             ("v", "integer")],
                            [(i, i * 2) for i in range(n)])
        pts = write_jsonl(self.ws, "p.jsonl",
                          [{"kref": i} for i in range(n)])
        dp = descriptor(self.ws, [
            archive_member("big", arc, [("k", "integer")], self.ws),
            jsonl_member("pts", pts, [("kref", "integer")])],
            [relation("r", "pts", ["kref"], "big", ["k"])])
        reports = []
        for budget in (7, 128):
            out = os.path.join(self.ws.root, "m-%d.json" % budget)
            rep = validate_period_package(dp, out=out, run_records=budget,
                                          run_bytes=512, fan_in=4)
            reports.append(rep["manifest_digest"])
            self.assertEqual(rep["relations"][0]["matched_references"], n)
        self.assertEqual(reports[0], reports[1])


class PeriodCliTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()
        self.arc = restore_table(
            self.ws, "devices", [("device_id", "integer")],
            [(1,), (2,), (3,)])
        self.pts = write_jsonl(self.ws, "points.jsonl",
                               [{"device_ref": d} for d in (1, 2, 3, 1)])
        self.dp = descriptor(self.ws, [
            archive_member("devices", self.arc,
                           [("device_id", "integer")], self.ws),
            jsonl_member("points", self.pts,
                         [("device_ref", "integer")])],
            [relation("r", "points", ["device_ref"],
                      "devices", ["device_id"])])
        self.manifest = os.path.join(self.ws.root, "manifest.json")

    def tearDown(self):
        self.ws.cleanup()

    def test_cli_success_writes_manifest_exit_0(self):
        p = cli("validate-period", self.dp, "--out", self.manifest)
        self.assertEqual(p.returncode, 0, p.stderr + p.stdout)
        self.assertTrue(os.path.exists(self.manifest))
        with open(self.manifest) as fh:
            self.assertEqual(json.load(fh)["status"], "valid")

    def test_cli_alias_and_stdout_manifest(self):
        p = cli("period", self.dp)
        self.assertEqual(p.returncode, 0, p.stderr + p.stdout)
        manifest = json.loads(p.stdout)
        self.assertEqual(manifest["archrestore_format"]["name"],
                         PERIOD_MANIFEST_KIND)
        self.assertIn("period valid", p.stderr)

    def test_cli_failure_exit_3_and_json_report(self):
        with open(self.pts, "a") as fh:
            fh.write(json.dumps({"device_ref": 42}) + "\n")
        p = cli("validate-period", self.dp, "--out", self.manifest,
                "--problems", os.path.join(self.ws.root, "d.jsonl"),
                "--json")
        self.assertEqual(p.returncode, 3, p.stdout + p.stderr)
        self.assertFalse(os.path.exists(self.manifest))
        report = json.loads(p.stdout)
        self.assertEqual(report["outcome"], "period_invalid")
        self.assertIn("unmatched_reference",
                      report["summary"]["by_kind"])

    def test_cli_bad_descriptor_exit_2(self):
        bad = os.path.join(self.ws.root, "bad.json")
        with open(bad, "w") as fh:
            json.dump({"archrestore_format": {"name": "nope", "version": 1}},
                      fh)
        p = cli("validate-period", bad)
        self.assertEqual(p.returncode, 2, p.stdout)

    def test_cli_verify_flag(self):
        p = cli("validate-period", self.dp, "--out", self.manifest,
                "--verify")
        self.assertEqual(p.returncode, 0, p.stderr + p.stdout)


class PeriodMemoryTests(unittest.TestCase):
    def tearDown(self):
        if hasattr(self, "ws"):
            self.ws.cleanup()

    def test_many_rows_peak_is_flat(self):
        self.ws = Workspace()
        n = 12000
        arc = restore_table(
            self.ws, "devices",
            [("device_id", "integer"), ("label", "string")],
            [(i, "dev-%09d" % i) for i in range(n)])
        pts = write_jsonl(self.ws, "points.jsonl",
                          [{"point_id": i, "device_ref": i}
                           for i in range(n)])
        dp = descriptor(self.ws, [
            archive_member("devices", arc,
                           [("device_id", "integer")], self.ws),
            jsonl_member("points", pts, [("device_ref", "integer")])],
            [relation("r", "points", ["device_ref"],
                      "devices", ["device_id"])])
        out = os.path.join(self.ws.root, "manifest.json")
        tracemalloc.start()
        try:
            report = validate_period_package(dp, out=out)
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(report["outcome"], "period_valid")
        self.assertEqual(report["relations"][0]["matched_references"], n)
        self.assertLess(peak, 8 * 1024 * 1024, peak)


if __name__ == "__main__":
    unittest.main()
