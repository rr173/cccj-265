"""Demo: build a damaged three-replica archive, repair it, print the diagnosis.

Run::

    python3 -I examples/demo.py [workdir]

This is offline and self-contained; it creates its data in a temporary
directory (or the directory given) and never touches the network.
"""

import json
import os
import random
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from archrestore import synth  # noqa: E402
from archrestore.archive import ArchiveReader, stream_text  # noqa: E402
from archrestore.pipeline import run_repair  # noqa: E402


def build(root):
    ds = synth.LogicalDataset(
        [("device", "string"), ("reading", "integer"), ("note", "string")],
        row_count=1000, seed=20261124,
        patterns={"device": "unique", "note": "empty"})
    manifest = synth.build_manifest(
        ds, {"device": [400, 300, 300], "reading": [250, 250, 500],
             "note": [500, 500]})
    with open(os.path.join(root, "manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=1)

    rng = random.Random(7)
    specs = []
    layouts = {
        "site-a": {  # coarse pages, plain and dictionary, covers everything
            "device": [(0, 1000)],
            "reading": [(0, 450), (450, 1000)],
            "note": [(0, 1000)],
        },
        "site-b": {  # fine pages, different boundaries, one hole
            "device": [(0, 260), (260, 700), (740, 1000)],
            "reading": [(0, 600), (620, 1000)],
            "note": [(0, 300), (300, 800)],
        },
        "site-c": {  # different dictionary, overlapping
            "device": [(0, 500), (400, 1000)],
            "reading": [(0, 300), (700, 1000)],
            "note": [(0, 400), (400, 1000)],
        },
    }
    for name, plan in layouts.items():
        directory = os.path.join(root, name)
        synth.build_replica(directory, ds, plan, [90, 40],
                            ["plain", "dict"], rng,
                            dict_mode="shared" if name != "site-b" else "per-page")
        specs.append((name, directory))
    return ds, manifest, specs


def main(argv):
    if argv:
        root = argv[0]
        os.makedirs(root, exist_ok=True)
        cleanup = False
    else:
        root = tempfile.mkdtemp(prefix="archrestore-demo-")
        cleanup = True

    ds, manifest, specs = build(root)

    # Damage one replica page without letting the descriptor notice: the
    # bytes are then bad evidence and must be skipped, not treated as a
    # competing value.
    site_b = os.path.join(root, "site-b")
    with open(os.path.join(site_b, "replica.json")) as fh:
        rep = json.load(fh)
    page = [p for p in rep["pages"]
            if p["column"] == "reading" and p["start"] == 0][0]
    data = os.path.join(site_b, page["location"]["file"])
    with open(data, "r+b") as fh:
        fh.seek(page["location"]["offset"])
        fh.write(b"\x00")
    print("damaged site-b page", page["page"], "(%d bytes)" % page["byte_length"])

    out = os.path.join(root, "archive")
    result = run_repair(os.path.join(root, "manifest.json"), specs, out)

    print("\n== repair report ==")
    print("outcome:", result["outcome"])
    if result.get("error"):
        print("error:", result["error"])
    summary = result["summary"]
    print("slots: %d/%d confirmed"
          % (summary["slots_confirmed"], summary["slot_count"]))
    for col in result["columns"]:
        bad = [s for s in col["slots"] if s["status"] != "confirmed"]
        if bad:
            print("column %r: %s" % (col["column"], [(s["slot"], s["status"])
                                                     for s in bad]))
    print("anomalies:", [a["kind"] for a in result["anomalies"]])
    print("verification:", result.get("verification"))

    if result["outcome"] != "restored":
        return 1

    print("\n== sample rows (by original row number) ==")
    with ArchiveReader(out) as reader:
        for row in (0, 259, 260, 999):
            values = []
            for c in ("device", "reading", "note"):
                raw = reader.read_row_column(c, row)
                values.append("".join(stream_text(raw, reader.pool)))
            print("row %4d: %s" % (row, ", ".join(values)))
    print("\narchive at:", out)
    if cleanup:
        print("(temporary demo directory; pass a path to keep it)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
