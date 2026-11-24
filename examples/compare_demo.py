#!/usr/bin/env python3
"""Generate two restored archives with known differences and compare them.

This is the repeatable compare generator/demo.  It builds two *logical*
device-inspection datasets (a "before" and an "after" inspection), turns each
into a verified archrestore archive through the normal restore pipeline,
writes a JSON compare spec, and runs the compare operation.

Run::

    python3 -I examples/compare_demo.py [workdir] [rows]

With no arguments it creates a small deterministic fixture in a temporary
directory and prints the resulting JSONL ledger.  Pass a directory to keep
all artefacts (two archives, manifests, replicas, spec, ledger).  Pass a row
count to generate a larger out-of-core exercise, e.g.::

    python3 -I examples/compare_demo.py /tmp/compare-large 200000

Everything is deterministic and offline.
"""

import json
import os
import random
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from archrestore.compare import compare_archives  # noqa: E402
from archrestore.pipeline import run_repair  # noqa: E402
from archrestore.synth import build_manifest, build_replica  # noqa: E402

COLUMNS = [("device_id", "string"), ("rack", "integer"),
           ("reading", "integer"), ("firmware", "string")]


class GeneratedDevices(object):
    """Deterministic device table with explicit add/remove/change rules.

    The "after" view is the before identities with every 10th device removed,
    some new devices appended, and every 7th surviving reading changed.  Row
    numbers therefore differ between the two archives for most devices.
    """

    def __init__(self, row_count, seed=20261124, after=False, added=3):
        self.seed = seed
        self.columns = list(COLUMNS)
        self.before_count = row_count
        if not after:
            self.row_count = row_count
            self.id_of_row = list(range(row_count))
        else:
            kept = [i for i in range(row_count) if i % 10 != 9]
            new_ids = list(range(row_count, row_count + added))
            self.id_of_row = kept + new_ids
            self.row_count = len(self.id_of_row)
            self._new_start = row_count

    def _base(self, device_id):
        rng = random.Random("%d:%d" % (self.seed, device_id))
        rack = 1 + device_id % 40
        reading = rng.randrange(0, 1000)
        firmware = "fw-%d.%d" % (1 + device_id % 3, device_id % 17)
        return {
            "device_id": "D-%08d" % device_id,
            "rack": rack,
            "reading": reading,
            "firmware": firmware,
        }

    def value(self, col_index, row):
        device_id = self.id_of_row[row]
        vals = self._base(device_id)
        if hasattr(self, "_new_start") and device_id >= self._new_start:
            vals["firmware"] = "fw-9.0-new"
        # Deterministic field change for some devices in the after view.
        if getattr(self, "_after", False) and device_id % 7 == 6 \
                and device_id < self.before_count:
            vals["reading"] = vals["reading"] + 1000
        name = self.columns[col_index][0]
        return vals[name]


def make_before(n):
    ds = GeneratedDevices(n, after=False)
    ds._after = False
    return ds


def make_after(n):
    ds = GeneratedDevices(n, after=True)
    ds._after = True
    return ds


def restore_dataset(root, dirname, ds):
    base = os.path.join(root, dirname)
    manifest = build_manifest(ds, [ds.row_count])
    mpath = os.path.join(base + "-manifest.json")
    os.makedirs(base, exist_ok=True)
    with open(mpath, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=1)
    rep_dir = os.path.join(root, dirname + "-replica")
    rng = random.Random(11)
    build_replica(rep_dir, ds,
                  {name: [(0, ds.row_count)] for name, _t in ds.columns},
                  [257], ["plain"], rng)
    archive = os.path.join(base, "archive")
    replica_id = os.path.basename(rep_dir)
    result = run_repair(mpath, [(replica_id, rep_dir)], archive)
    if result["outcome"] != "restored":
        raise RuntimeError("fixture restore failed: %s" % result.get("error"))
    return archive


class BadIdentityTable(object):
    """Tiny table containing a null identity and a duplicate identity."""

    columns = [("device_id", "string"), ("reading", "integer")]
    row_count = 5
    _rows = [
        ("D-1", 10),
        (None, 11),       # row 1: empty identity
        ("D-2", 12),
        ("D-2", 13),      # row 3: duplicate of row 2
        ("D-3", 14),
    ]

    def value(self, col_index, row):
        return self._rows[row][col_index]


def build(root, rows=12):
    os.makedirs(root, exist_ok=True)
    before_archive = restore_dataset(root, "before", make_before(rows))
    after_archive = restore_dataset(root, "after", make_after(rows))

    spec_path = os.path.join(root, "compare-spec.json")
    spec = {
        "archrestore_format": {"name": "archrestore-compare-spec",
                               "version": 1},
        "identity_fields": ["device_id"],
    }
    with open(spec_path, "w", encoding="utf-8") as fh:
        json.dump(spec, fh, ensure_ascii=False, indent=1)
        fh.write("\n")

    ledger_path = os.path.join(root, "changes.jsonl")
    report = compare_archives(before_archive, after_archive, spec_path,
                              out=ledger_path)

    # A second spec demonstrating composite identity fields.
    composite_spec = os.path.join(root, "compare-spec-composite.json")
    with open(composite_spec, "w", encoding="utf-8") as fh:
        json.dump({"identity_fields": ["rack", "device_id"]}, fh, indent=1)

    # Failure fixture: null + duplicate identities, no ledger may appear.
    bad_archive = restore_dataset(root, "bad", BadIdentityTable())
    bad_ledger = os.path.join(root, "bad-changes.jsonl")
    if os.path.exists(bad_ledger):
        os.unlink(bad_ledger)
    bad_error = None
    try:
        compare_archives(bad_archive, bad_archive,
                         {"identity_fields": ["device_id"]}, out=bad_ledger)
    except Exception as exc:  # noqa: BLE001 - demo reporting
        bad_error = exc

    return {
        "root": root,
        "before_archive": before_archive,
        "after_archive": after_archive,
        "spec": spec_path,
        "ledger": ledger_path,
        "report": report,
        "bad_archive": bad_archive,
        "bad_ledger": bad_ledger,
        "bad_error": bad_error,
    }


def main(argv):
    rows = int(argv[1]) if len(argv) > 1 else 12
    if argv:
        root = argv[0]
        os.makedirs(root, exist_ok=True)
        cleanup = False
    else:
        root = tempfile.mkdtemp(prefix="archrestore-compare-demo-")
        cleanup = True

    artefacts = build(root, rows=rows)
    report = artefacts["report"]
    print("== compare summary ==")
    print(json.dumps(report["summary"], sort_keys=True))
    print("\n== %s ==" % artefacts["ledger"])
    with open(artefacts["ledger"], encoding="utf-8") as fh:
        for line in fh:
            print(line.rstrip())

    print("\n== identity-problem fixture ==")
    err = artefacts["bad_error"]
    print("correctly refused: %s" % (err.report["error"] if hasattr(
        err, "report") else err))
    if hasattr(err, "report"):
        for p in err.report["problems"]:
            print(" ", p)
    print("ledger exists on failure? %s (must be False)"
          % os.path.exists(artefacts["bad_ledger"]))

    print("\n== reproduce on the command line ==")
    launcher = ("python3 -c \"import sys; sys.path.insert(0, %r); "
                "from archrestore.cli import main; sys.exit(main())\"" % ROOT)
    print("%s compare %s %s --spec %s --out %s"
          % (launcher, artefacts["before_archive"],
             artefacts["after_archive"], artefacts["spec"],
             artefacts["ledger"] + ".cli"))
    if cleanup:
        print("\n(temporary directory %s removed; pass a path argument to "
              "keep artefacts)" % root)
        shutil.rmtree(root, ignore_errors=True)
    else:
        print("\nartefacts kept at: %s" % root)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
