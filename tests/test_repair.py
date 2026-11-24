"""End-to-end repair contract tests.

Each test builds a synthetic logical dataset, writes several physical
replicas with different page boundaries/encodings/dictionaries, damages them,
and asserts the precise per-slot diagnosis and the archive contents.
"""

import hashlib
import json
import os
import random
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from helpers import (  # noqa: E402
    Workspace,
    corrupt_integer_page,
    damage_page,
    load_descriptor,
    make_replica,
    run,
    save_descriptor,
    write_manifest,
)

from archrestore import synth  # noqa: E402
from archrestore.archive import ArchiveReader, stream_text, verify_archive  # noqa: E402
from archrestore.errors import ArchError  # noqa: E402
from archrestore.pipeline import run_repair  # noqa: E402


def dataset(rows=240, seed=11, patterns=None, columns=None):
    return synth.LogicalDataset(
        columns or [("id", "integer"), ("name", "string"), ("note", "string")],
        row_count=rows, seed=seed,
        patterns=patterns or {"name": "unique", "note": "empty"})


def read_all(archive_dir, ds, columns=None):
    cols = columns or [n for n, _t in ds.columns]
    idx = {n: i for i, (n, _t) in enumerate(ds.columns)}
    got = []
    with ArchiveReader(archive_dir) as reader:
        for row in range(ds.row_count):
            rec = {}
            for name in cols:
                raw = reader.read_row_column(name, row)
                rec[name] = json.loads("".join(stream_text(raw, reader.pool)))
            got.append(rec)
    return got


class FullRepairTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()

    def tearDown(self):
        self.ws.cleanup()

    def test_cross_replica_stitching_and_overlap(self):
        ds = dataset()
        _man, mpath = write_manifest(ds, {"id": [100, 100, 40],
                                          "name": [70, 170],
                                          "note": [240]}, self.ws.root)
        # r1 covers everything; r2 has a hole that only r1 fills and pages at
        # different boundaries, with extra overlap (duplicate evidence).
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 150), (150, 240)],
            "name": [(0, 120), (120, 240)],
            "note": [(0, 240)],
        }, encodings=("plain",), page_sizes=(50,))
        r2 = make_replica(self.ws.root, ds, "r2", {
            "id": [(0, 60), (60, 240)],           # overlaps id 0..60? no
            "name": [(0, 90), (100, 240)],        # hole 90..100 -> r1 only
            "note": [(0, 100), (90, 240)],        # self-overlap 90..100
        }, encodings=("plain",), page_sizes=(65, 31))
        res = run(self.ws.root, mpath, [r1, r2])
        self.assertEqual(res["outcome"], "restored", res.get("error"))
        self.assertEqual(res["summary"]["slots_confirmed"], 6)
        self.assertEqual(read_all(os.path.join(self.ws.root, "out"), ds),
                         synth.reconstruct_truth(ds))
        # Overlap is reported, not silently duplicated.
        kinds = {a["kind"] for a in res["anomalies"]}
        self.assertIn("replica_pages_overlap", kinds)
        # Exactly row_count values per column in the archive.
        with ArchiveReader(os.path.join(self.ws.root, "out")) as reader:
            for c in reader.header["columns"]:
                self.assertEqual(c["value_count"], ds.row_count)

    def test_slot_satisfied_by_subrange_of_one_page(self):
        ds = dataset(rows=50)
        _m, mpath = write_manifest(ds, {"id": [5, 40, 5], "name": [50],
                                        "note": [50]}, self.ws.root)
        # One giant page per column covering far more than any one slot.
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 50)], "name": [(0, 50)], "note": [(0, 50)]},
            encodings=("plain", "dict"))
        res = run(self.ws.root, mpath, [r1])
        self.assertEqual(res["outcome"], "restored", res.get("error"))
        self.assertEqual(read_all(os.path.join(self.ws.root, "out"), ds),
                         synth.reconstruct_truth(ds))

    def test_dictionary_reused_across_replicas_and_page_numbers(self):
        ds = dataset(rows=120, patterns={"name": "unique"})
        _m, mpath = write_manifest(ds, {"id": [60, 60], "name": [60, 60],
                                        "note": [120]}, self.ws.root)
        # r1 stores the shared dictionary, but its dict page for 'name' is
        # later damaged; r2 has the same dictionary at a *different* file,
        # offset, id and page number and must be used by digest.
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 120)], "name": [(0, 120)], "note": [(0, 120)]},
            encodings=("dict",), dict_mode="shared")
        r2 = make_replica(self.ws.root, ds, "r2", {
            "id": [(0, 120)], "name": [(0, 120)], "note": [(0, 120)]},
            encodings=("dict",), dict_mode="shared", seed=99)
        # Damage r1's name page body but keep the descriptor honest (so it is
        # skipped as bad evidence, not disputed).
        damage_page(r1[1], "name", 0, mode="flip")
        res = run(self.ws.root, mpath, [r1, r2])
        self.assertEqual(res["outcome"], "restored", res.get("error"))
        self.assertEqual(read_all(os.path.join(self.ws.root, "out"), ds),
                         synth.reconstruct_truth(ds))
        # The dictionary provenance in the check index points at the verified
        # source bytes by digest, not by name.
        with open(os.path.join(self.ws.root, "out", "checks", "index.json"),
                  encoding="utf-8") as fh:
            idx = json.load(fh)
        self.assertTrue(idx["dictionaries"])
        for d in idx["dictionaries"]:
            self.assertEqual(len(d["dict_digest"]), 64)

    def test_archive_verifies_standalone(self):
        ds = dataset(rows=90)
        _m, mpath = write_manifest(ds, {"id": [30, 60], "name": [45, 45],
                                        "note": [90]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 90)], "name": [(0, 90)], "note": [(0, 90)]})
        res = run(self.ws.root, mpath, [r1])
        self.assertEqual(res["outcome"], "restored", res.get("error"))
        out = os.path.join(self.ws.root, "out")
        self.assertEqual(verify_archive(out).ok, True)
        # Tampering with one value breaks verification.
        with open(os.path.join(out, "archive.json")) as fh:
            cmeta = json.load(fh)["columns"][0]
        path = os.path.join(out, cmeta["file"])
        with open(path, "r+b") as fh:
            fh.seek(0)
            first = fh.read(1)
            fh.seek(0)
            fh.write(bytes([first[0] ^ 0xFF]))
        self.assertFalse(verify_archive(out).ok)

    def test_query_by_original_row_number(self):
        ds = dataset(rows=40)
        _m, mpath = write_manifest(ds, {"id": [20, 20], "name": [40],
                                        "note": [40]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 40)], "name": [(0, 40)], "note": [(0, 40)]},
            encodings=("dict", "plain"))
        res = run(self.ws.root, mpath, [r1])
        self.assertEqual(res["outcome"], "restored", res.get("error"))
        with ArchiveReader(os.path.join(self.ws.root, "out")) as reader:
            for row in (0, 19, 20, 39):
                raw = reader.read_row_column("id", row)
                self.assertEqual(json.loads("".join(stream_text(raw, reader.pool))),
                                 ds.value(0, row))


class FailureDiagnosisTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace()

    def tearDown(self):
        self.ws.cleanup()

    def test_missing_rows_reported_per_slot_without_archive(self):
        ds = dataset(rows=100)
        _m, mpath = write_manifest(ds, {"id": [50, 50], "name": [100],
                                        "note": [100]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 30), (70, 100)], "name": [(0, 100)],
            "note": [(0, 100)]}, encodings=("plain",))
        res = run(self.ws.root, mpath, [r1])
        self.assertEqual(res["outcome"], "incomplete")
        self.assertFalse(os.path.exists(
            os.path.join(self.ws.root, "out", "archive.json")))
        slot_of = lambda col, start: [s for s in res["slots"]
                                      if s["column"] == col and
                                      s["start"] == start][0]
        self.assertEqual(slot_of("id", 0)["status"], "missing")
        self.assertEqual(slot_of("id", 0)["missing_rows"], 20)
        self.assertEqual(slot_of("id", 0)["missing_ranges"],
                         [{"start": 30, "end": 50}])
        self.assertEqual(slot_of("id", 50)["status"], "missing")
        self.assertEqual(slot_of("id", 50)["missing_rows"], 20)
        self.assertEqual(slot_of("name", 0)["status"], "confirmed")
        self.assertEqual(slot_of("note", 0)["status"], "confirmed")
        self.assertEqual(res["summary"]["cells_missing"], 40)

    def test_dispute_is_not_missing_and_blocks_slot(self):
        ds = dataset(rows=60)
        _m, mpath = write_manifest(ds, {"id": [30, 30], "name": [60],
                                        "note": [60]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 30), (30, 60)], "name": [(0, 60)],
            "note": [(0, 60)]}, encodings=("plain",))
        r2 = make_replica(self.ws.root, ds, "r2", {
            "id": [(0, 30), (30, 60)], "name": [(0, 60)],
            "note": [(0, 60)]}, encodings=("plain",), seed=77)
        # r2's id page stays structurally valid, well-typed and correctly
        # digested, but every value is shifted: this must be a *dispute*.
        corrupt_integer_page(r2[1], "id", 0, delta=1)
        res = run(self.ws.root, mpath, [r1, r2])
        self.assertEqual(res["outcome"], "incomplete")
        slot_of = lambda col, start: [s for s in res["slots"]
                                      if s["column"] == col and
                                      s["start"] == start][0]
        slot = slot_of("id", 0)
        self.assertEqual(slot["status"], "disputed")
        self.assertGreater(slot["disputed_rows"], 0)
        self.assertEqual(slot["missing_rows"], 0)
        self.assertTrue(slot["disputes"])
        dispute = slot["disputes"][0]
        self.assertEqual(len(dispute["variants"]), 2)
        ids = {v["sources"][0]["replica_id"] for v in dispute["variants"]}
        self.assertEqual(ids, {"r1", "r2"})
        # Other slots are unaffected and still confirmed.
        self.assertEqual(slot_of("id", 30)["status"], "confirmed")
        self.assertEqual(slot_of("name", 0)["status"], "confirmed")

    def test_digest_mismatch_is_separate_from_missing_and_dispute(self):
        ds = dataset(rows=80)
        def mutate(man):
            man["slots"][0]["digest"] = "f" * 64  # id slot 0
        _m, mpath = write_manifest(ds, {"id": [40, 40], "name": [80],
                                        "note": [80]}, self.ws.root,
                                    mutate=mutate)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 80)], "name": [(0, 80)], "note": [(0, 80)]})
        res = run(self.ws.root, mpath, [r1])
        self.assertEqual(res["outcome"], "incomplete")
        slot = [s for s in res["slots"]
                if s["column"] == "id" and s["start"] == 0][0]
        self.assertEqual(slot["status"], "digest_mismatch")
        self.assertEqual(slot["missing_rows"], 0)
        self.assertEqual(slot["disputed_rows"], 0)
        self.assertNotEqual(slot["computed_digest"], slot["expected_digest"])
        self.assertEqual(res["summary"]["slots_digest_mismatch"], 1)
        self.assertEqual(res["summary"]["slots_missing"], 0)
        self.assertEqual(res["summary"]["slots_disputed"], 0)

    def test_bad_manifest_aborts_and_is_labelled(self):
        ds = dataset(rows=30)
        def mutate(man):
            man["slots"] = [s for s in man["slots"]
                            if not (s["column"] == "id" and s["start"] == 0)]
        _m, mpath = write_manifest(ds, {"id": [15, 15], "name": [30],
                                        "note": [30]}, self.ws.root,
                                    mutate=mutate)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 30)], "name": [(0, 30)], "note": [(0, 30)]})
        res = run(self.ws.root, mpath, [r1])
        self.assertEqual(res["outcome"], "bad_manifest")
        self.assertIn("gap", res["error"])

    def test_bad_replica_document_is_skipped_not_fatal(self):
        ds = dataset(rows=40)
        _m, mpath = write_manifest(ds, {"id": [40], "name": [40],
                                        "note": [40]}, self.ws.root)
        good = make_replica(self.ws.root, ds, "good", {
            "id": [(0, 40)], "name": [(0, 20), (20, 40)],
            "note": [(0, 40)]})
        bad_dir = os.path.join(self.ws.root, "bad")
        os.makedirs(bad_dir)
        with open(os.path.join(bad_dir, "replica.json"), "w") as fh:
            fh.write("{not json")
        res = run(self.ws.root, mpath, [good, ("bad", bad_dir)])
        self.assertEqual(res["outcome"], "restored", res.get("error"))
        bad = [r for r in res["replicas"] if r["replica_id"] == "bad"][0]
        self.assertEqual(bad["status"], "rejected")
        self.assertIn("replica_rejected",
                      {a["kind"] for a in res["anomalies"]})

    def test_page_with_wrong_length_is_skipped_not_dispute(self):
        ds = dataset(rows=60)
        _m, mpath = write_manifest(ds, {"id": [60], "name": [60],
                                        "note": [60]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 60)], "name": [(0, 60)], "note": [(0, 60)]},
            encodings=("plain",))
        r2 = make_replica(self.ws.root, ds, "r2", {
            "id": [(0, 60)], "name": [(0, 60)], "note": [(0, 60)]},
            encodings=("plain",), seed=8)
        # Truncate r2's data file: the declared id-page range no longer fits,
        # so that page is skipped (as are later pages in the file, correctly,
        # because their declared ranges were also cut).
        damage_page(r2[1], "id", 0, mode="truncate")
        res = run(self.ws.root, mpath, [r1, r2])
        self.assertEqual(res["outcome"], "restored", res.get("error"))
        bad = [r for r in res["replicas"] if r["replica_id"] == "r2"][0]
        self.assertTrue(bad["pages_unusable"])
        self.assertEqual(bad["pages_unusable"][0]["reason"], "page_truncated")
        # No dispute: the lone broken page never became a competing value.
        self.assertEqual(res["summary"]["cells_disputed"], 0)

    def test_dictionary_missing_from_every_replica_skips_dict_pages(self):
        ds = dataset(rows=40)
        _m, mpath = write_manifest(ds, {"id": [40], "name": [40],
                                        "note": [40]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 40)], "name": [(0, 40)], "note": [(0, 40)]},
            encodings=("dict",))
        # Remove the dictionary file entirely; descriptor still declares it.
        os.unlink(os.path.join(r1[1], "dict.bin"))
        res = run(self.ws.root, mpath, [r1])
        self.assertEqual(res["outcome"], "incomplete")
        self.assertEqual(res["summary"]["cells_missing"], 40 * 3)
        reasons = set()
        for r in res["replicas"]:
            for p in r["pages_unusable"]:
                reasons.add(p["reason"])
        self.assertEqual(reasons, {"dictionary_unavailable"})
        # A *missing dictionary* is not a dispute.
        self.assertEqual(res["summary"]["cells_disputed"], 0)

    def test_dictionary_with_wrong_length_is_rejected(self):
        ds = dataset(rows=40)
        _m, mpath = write_manifest(ds, {"id": [40], "name": [40],
                                        "note": [40]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 40)], "name": [(0, 40)], "note": [(0, 40)]},
            encodings=("dict",))
        # Grow the declared dict length by one without changing bytes: length
        # and digest can no longer both be right.
        rep = load_descriptor(r1[1])
        for p in rep["pages"]:
            p["dict_byte_length"] += 1
        save_descriptor(r1[1], rep)
        res = run(self.ws.root, mpath, [r1])
        self.assertEqual(res["outcome"], "incomplete")
        self.assertEqual(res["summary"]["cells_missing"], 120)
        self.assertEqual(res["summary"]["cells_disputed"], 0)

    def test_dictionary_code_out_of_range_skips_page(self):
        ds = dataset(rows=30)
        _m, mpath = write_manifest(ds, {"id": [30], "name": [30],
                                        "note": [30]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 30)], "name": [(0, 30)], "note": [(0, 30)]},
            encodings=("dict",))
        rep = load_descriptor(r1[1])
        page = [p for p in rep["pages"] if p["column"] == "id"][0]
        # Overwrite the first code with 0xFFFFFFFF and refresh the page digest
        # so only the code range check can reject it.
        path = os.path.join(r1[1], page["location"]["file"])
        with open(path, "r+b") as fh:
            off = page["location"]["offset"]
            fh.seek(off)
            fh.write(b"\xff\xff\xff\xff")
            fh.seek(off)
            body = fh.read(page["byte_length"])
        page["byte_digest"] = hashlib.sha256(body).hexdigest()
        save_descriptor(r1[1], rep)
        res = run(self.ws.root, mpath, [r1])
        self.assertEqual(res["outcome"], "incomplete")
        bad = [p for r in res["replicas"] for p in r["pages_unusable"]
               if p["column"] == "id"]
        self.assertEqual(bad[0]["reason"], "dict_code_out_of_range")
        self.assertEqual(res["summary"]["cells_disputed"], 0)

    def test_three_replicas_one_good_dict_others_bad_bytes(self):
        ds = dataset(rows=80)
        _m, mpath = write_manifest(ds, {"id": [80], "name": [80],
                                        "note": [80]}, self.ws.root)
        reps = []
        for i, mode in enumerate(("good", "bad_magic", "bad_digest")):
            r = make_replica(self.ws.root, ds, "r%d" % i, {
                "id": [(0, 80)], "name": [(0, 80)], "note": [(0, 80)]},
                encodings=("dict",), seed=30 + i)
            reps.append(r)
        # Damage two replicas' dictionary bytes while keeping their declared
        # digests honest: the bytes are rejected, and the third replica's
        # verified dictionary is used by digest.  Damage page bodies too, so
        # those replicas contribute nothing but their (bad) claims.
        for name, mode in (("r1", "bad_magic"), ("r2", "bad_digest")):
            d = os.path.join(self.ws.root, name)
            dpath = os.path.join(d, "dict.bin")
            with open(dpath, "r+b") as fh:
                if mode == "bad_magic":
                    fh.write(b"XXXXXXXX")
                else:
                    fh.seek(20)
                    fh.write(b"\xff")
            rep = load_descriptor(d)
            for page in rep["pages"]:
                damage_page(d, page["column"], page["start"], mode="flip")
        res = run(self.ws.root, mpath, [r for r in reps])
        self.assertEqual(res["outcome"], "restored", res.get("error"))
        self.assertEqual(read_all(os.path.join(self.ws.root, "out"), ds),
                         synth.reconstruct_truth(ds))

    def test_replica_locations_cannot_escape(self):
        ds = dataset(rows=20)
        _m, mpath = write_manifest(ds, {"id": [20], "name": [20],
                                        "note": [20]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 20)], "name": [(0, 20)], "note": [(0, 20)]})
        rep = load_descriptor(r1[1])
        rep["pages"][0]["location"] = {"file": "../../../../etc/passwd",
                                       "offset": 0}
        save_descriptor(r1[1], rep)
        res = run(self.ws.root, mpath, [r1])
        self.assertEqual(res["outcome"], "incomplete")
        for r in res["replicas"]:
            for p in r["pages_unusable"]:
                self.assertEqual(p["reason"], "io_error")

    def test_self_conflicting_replica_is_disputed(self):
        # A single replica whose two overlapping pages disagree with each
        # other is still a dispute: evidence is compared by value, not by
        # which replica supplied it.
        ds = dataset(rows=40)
        _m, mpath = write_manifest(ds, {"id": [40], "name": [40],
                                        "note": [40]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 40)], "name": [(0, 40)], "note": [(0, 40)]},
            encodings=("plain",))
        rep = load_descriptor(r1[1])
        page = [p for p in rep["pages"] if p["column"] == "id"][0]
        # Append a second, fully overlapping copy with shifted values.
        import struct
        with open(os.path.join(r1[1], page["location"]["file"]), "rb") as fh:
            fh.seek(page["location"]["offset"])
            body = fh.read(page["byte_length"])
        out = bytearray(body)
        for pos in range(0, len(out), 9):
            v = int.from_bytes(out[pos + 1:pos + 9], "little", signed=True)
            out[pos + 1:pos + 9] = struct.pack("<q", v + 5)
        data_path = os.path.join(r1[1], page["location"]["file"])
        with open(data_path, "ab") as fh:
            off = fh.tell()
            fh.write(bytes(out))
        clone = dict(page)
        clone.update({
            "page": max(p["page"] for p in rep["pages"]) + 1,
            "location": {"file": page["location"]["file"], "offset": off},
            "byte_digest": hashlib.sha256(bytes(out)).hexdigest(),
        })
        rep["pages"].append(clone)
        save_descriptor(r1[1], rep)
        res = run(self.ws.root, mpath, [r1])
        self.assertEqual(res["outcome"], "incomplete")
        slot = [s for s in res["slots"]
                if s["column"] == "id" and s["start"] == 0][0]
        self.assertEqual(slot["status"], "disputed")
        sources = {v["sources"][0]["replica_id"]
                   for v in slot["disputes"][0]["variants"]}
        self.assertEqual(sources, {"r1"})
        self.assertIn("replica_pages_overlap",
                      {a["kind"] for a in res["anomalies"]})

    def test_refuses_nonempty_output_directory(self):
        ds = dataset(rows=10)
        _m, mpath = write_manifest(ds, {"id": [10], "name": [10],
                                        "note": [10]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 10)], "name": [(0, 10)], "note": [(0, 10)]})
        out = os.path.join(self.ws.root, "occupied")
        os.makedirs(out)
        with open(os.path.join(out, "keep.txt"), "w") as fh:
            fh.write("precious")
        with self.assertRaises(ArchError):
            run_repair(mpath, [r1], out)
        with open(os.path.join(out, "keep.txt")) as fh:
            self.assertEqual(fh.read(), "precious")

    def test_duplicate_replica_id_rejected(self):
        ds = dataset(rows=10)
        _m, mpath = write_manifest(ds, {"id": [10], "name": [10],
                                        "note": [10]}, self.ws.root)
        r1 = make_replica(self.ws.root, ds, "r1", {
            "id": [(0, 10)], "name": [(0, 10)], "note": [(0, 10)]})
        with self.assertRaises(ArchError):
            run_repair(mpath, [r1, r1], self.ws.path("out"))


if __name__ == "__main__":
    unittest.main()
