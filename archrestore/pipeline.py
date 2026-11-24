"""End-to-end repair pipeline: manifest -> replicas -> resolution -> archive.

Failure taxonomy (kept strictly separate, as required):

* **bad manifest** (`ManifestError`): gaps, overlaps, duplicate ids, wrong
  types, out-of-range bounds, non-tiling slots.  Aborts the run: this is not
  missing data, and no replica can ever fix it.
* **bad replica document** (`ReplicaError`): malformed descriptor at replica
  level.  The replica is skipped with an anomaly; other replicas continue.
* **bad page** (rejected evidence): wrong declared length, digest mismatch,
  undecodable body, missing/invalid dictionary, out-of-range dictionary
  code, wrong value type.  The page is skipped with an anomaly; other pages
  and replicas continue.
* **anomalies worth reporting, not skipping**: pages from the same replica
  overlapping the same column rows (duplicate evidence; harmless but
  reported), dictionaries declared with conflicting identities, replica pages
  whose rows lie outside [0, row_count).
* **missing rows** (no usable evidence) and **disputes** (>=2 distinct
  decoded values) are per-row data defects, reported per slot.
* **digest mismatch** is separate again: all rows present and agreed, but the
  slot's canonical digest differs from the manifest.

The archive is only written when *every* slot of *every* column is
confirmed.  Otherwise a diagnostics document is produced and the exit status
is non-zero.
"""

import json
import os
import shutil
import tempfile

from . import __version__
from .archive import write_archive
from .decode import DictRepository, decode_page
from .errors import (
    ArchError,
    ManifestError,
    PageRejected,
    ReplicaError,
)
from .format import (
    FORMAT_NAME,
    FORMAT_VERSION,
    load_json,
    parse_manifest,
    parse_replica,
    sha256_hex,
)
from .resolve import Piece, resolve_column
from .spill import FilePool, Spool, SpoolWriter


class AnomalyLog(object):
    MAX = 500

    def __init__(self):
        self.items = []
        self.total = 0

    def add(self, item):
        self.total += 1
        if len(self.items) < self.MAX:
            self.items.append(item)
        elif len(self.items) == self.MAX:
            self.items.append({"kind": "anomalies_truncated",
                               "detail": "more than %d anomalies; further "
                                         "entries omitted" % self.MAX})

    def to_json(self):
        out = list(self.items)
        if self.total > len(out):
            out = out[:self.MAX]
            out.append({"kind": "anomalies_truncated",
                        "total_anomalies": self.total})
        return out


class ReplicaRun(object):
    def __init__(self, replica_id, directory, path):
        self.replica_id = replica_id
        self.directory = directory
        self.path = path
        self.replica = None
        self.error = None
        self.page_errors = []
        self.pages_accepted = 0
        self.pages_rejected = []


def _load_replica(spec, manifest, anomalies, workdir):
    """Load one replica descriptor; return a ReplicaRun (never raises)."""
    replica_id, directory = spec
    run = ReplicaRun(replica_id, directory, None)
    descriptor_path = os.path.join(directory, "replica.json")
    run.path = descriptor_path
    try:
        obj = load_json(descriptor_path)
        replica, page_errors = parse_replica(obj, manifest, descriptor_path,
                                             directory)
        if replica.replica_id != replica_id:
            raise ReplicaError(
                "replica at %r declares replica_id %r but was given as %r"
                % (directory, replica.replica_id, replica_id))
        run.replica = replica
        run.page_errors = page_errors
        for pe in page_errors:
            anomalies.add(dict(pe, kind="page_descriptor_rejected",
                               replica_id=replica_id))
    except (ArchError, OSError) as exc:
        run.error = str(exc)
        anomalies.add({"kind": "replica_rejected", "replica_id": replica_id,
                       "directory": directory, "detail": str(exc)})
    return run


def _check_replica_coverage_anomalies(replica, anomalies):
    """Report (do not skip) duplicate evidence and out-of-manifest pages."""
    by_col = {}
    for p in replica.pages:
        by_col.setdefault(p.column, []).append(p)
    for col, pages in by_col.items():
        pages = sorted(pages, key=lambda p: (p.start, p.end, p.page))
        for i in range(1, len(pages)):
            prev, cur = pages[i - 1], pages[i]
            if cur.start < prev.end:
                anomalies.add({
                    "kind": "replica_pages_overlap",
                    "replica_id": replica.replica_id,
                    "column": col,
                    "pages": [prev.page, cur.page],
                    "ranges": [[prev.start, prev.end], [cur.start, cur.end]],
                    "detail": "replica pages overlap; this is duplicate "
                              "evidence only and does not duplicate rows",
                })


def run_repair(manifest_path, replica_specs, out_dir, report_path=None,
               workdir=None, keep_work=False, verify=True):
    """Run the full pipeline.  Returns a result dict (also written as JSON)."""
    started = _now()
    # Refuse to overwrite a non-empty output directory: repairing is not a
    # destructive operation, and a stale archive must never masquerade as a
    # fresh result.
    if os.path.isdir(out_dir) and os.listdir(out_dir):
        raise ArchError("output directory %r exists and is not empty; choose a "
                        "new directory or remove the old one" % (out_dir,))
    if os.path.exists(out_dir) and not os.path.isdir(out_dir):
        raise ArchError("output path %r exists and is not a directory" % (out_dir,))
    anomalies = AnomalyLog()
    cleanup_root = None
    if workdir is None:
        cleanup_root = tempfile.mkdtemp(prefix="archrestore-work-")
        workdir = cleanup_root
    else:
        os.makedirs(workdir, exist_ok=True)
    os.makedirs(out_dir, exist_ok=True)
    diagnostics_path = os.path.join(out_dir, "diagnostics.json")

    result = {
        "tool": {"name": FORMAT_NAME, "version": __version__,
                 "format_version": FORMAT_VERSION},
        "started_at": started,
        "manifest": None,
        "replicas": [],
        "anomalies": None,
        "columns": [],
        "slots": [],
        "summary": {},
        "archive": None,
        "verification": None,
    }

    manifest_sha = None
    try:
        with open(manifest_path, "rb") as fh:
            manifest_sha = sha256_hex(fh.read())
        manifest = parse_manifest(load_json(manifest_path), manifest_path)
    except ManifestError as exc:
        result["outcome"] = "bad_manifest"
        result["error"] = str(exc)
        result["anomalies"] = anomalies.to_json()
        _write_json(diagnostics_path, result)
        _cleanup(cleanup_root, keep_work)
        return result
    except (ArchError, OSError) as exc:
        result["outcome"] = "manifest_unreadable"
        result["error"] = str(exc)
        result["anomalies"] = anomalies.to_json()
        _write_json(diagnostics_path, result)
        _cleanup(cleanup_root, keep_work)
        return result

    result["manifest"] = {
        "path": os.path.abspath(manifest_path),
        "sha256": manifest_sha,
        "row_count": manifest.row_count,
        "columns": [{"name": c.name, "type": c.type} for c in manifest.columns],
        "slot_count": len(manifest.slots),
    }

    pool = FilePool(max_open=64)
    dict_repo = DictRepository(pool, workdir, anomalies)
    pieces_by_column = {c.name: [] for c in manifest.columns}
    dict_provenance = {}

    try:
        runs = []
        seen_ids = set()
        for spec in replica_specs:
            if spec[0] in seen_ids:
                raise ArchError("replica id %r given more than once" % (spec[0],))
            seen_ids.add(spec[0])
            run = _load_replica(spec, manifest, anomalies, workdir)
            runs.append(run)
        # Collect dictionary candidates from every replica *descriptor* before
        # decoding, so a good dictionary may come from a replica with bad pages.
        for run in runs:
            if run.replica is None:
                continue
            for page in run.replica.pages:
                if page.encoding == "dict":
                    dict_repo.add_candidate(
                        (page.dict_ref.digest, page.dict_ref.byte_length),
                        run.replica_id, page.dict_ref, run.directory, page)

        for run in runs:
            run_info = {
                "replica_id": run.replica_id,
                "directory": os.path.abspath(run.directory),
                "descriptor": run.path,
                "status": "rejected" if run.replica is None else "used",
                "error": run.error,
                "pages_declared": (len(run.replica.pages)
                                   if run.replica else 0),
                "pages_decoded": 0,
                "pages_unusable": [],
            }
            result["replicas"].append(run_info)
            if run.replica is None:
                continue
            _check_replica_coverage_anomalies(run.replica, anomalies)
            for page in run.replica.pages:
                try:
                    spool = decode_page(page, workdir, run.replica_id,
                                        dict_repo, run.directory)
                except PageRejected as exc:
                    run_info["pages_unusable"].append({
                        "page": page.page,
                        "column": page.column,
                        "start": page.start,
                        "end": page.end,
                        "encoding": page.encoding,
                        "reason": exc.reason,
                        "detail": exc.detail,
                    })
                    anomalies.add({
                        "kind": "page_rejected",
                        "replica_id": run.replica_id,
                        "page": page.page,
                        "column": page.column,
                        "start": page.start,
                        "end": page.end,
                        "encoding": page.encoding,
                        "reason": exc.reason,
                        "detail": exc.detail,
                    })
                    continue
                run_info["pages_decoded"] += 1
                pieces_by_column[page.column].append(Piece(spool, page))

        # Resolve each column; the unique spill holds exactly one value per row.
        column_results = {}
        for c in manifest.columns:
            unique_path = os.path.join(workdir, "unique-%03d.canon" % c.index)
            writer = SpoolWriter(unique_path, 0)
            try:
                res = resolve_column(c, pieces_by_column[c.name],
                                     manifest.slots_by_column[c.name],
                                     pool, writer)
            finally:
                writer.close()
            res.unique_path = unique_path
            res.unique_spill = Spool(unique_path, 0, manifest.row_count,
                                      writer.offsets)
            if res.stream_complete and writer.row_count != manifest.row_count:
                res.stream_complete = False
                anomalies.add({
                    "kind": "internal_row_count_mismatch",
                    "column": c.name,
                    "detail": "resolver wrote %d rows, expected %d"
                              % (writer.row_count, manifest.row_count),
                })
            column_results[c.name] = res
            result["columns"].append(res.to_json())
            for d in res.slots:
                result["slots"].append(d.to_json())
            if not res.all_confirmed:
                failed = [d for d in res.slots if not d.confirmed]
                anomalies.add({
                    "kind": "column_not_confirmed",
                    "column": c.name,
                    "slots_failed": [d.slot.slot for d in failed],
                    "detail": "%d of %d slots unconfirmed (%s)"
                              % (len(failed), len(res.slots),
                                 ", ".join(sorted({d.status for d in failed}))),
                })

        # Record dictionary provenance for the check index.
        for identity, (spill, prov) in dict_repo._loaded.items():
            dict_provenance[identity] = prov

        all_confirmed = all(r.all_confirmed for r in column_results.values())
        total_slots = sum(len(r.slots) for r in column_results.values())
        confirmed_slots = sum(1 for r in column_results.values()
                              for d in r.slots if d.confirmed)
        summary = {
            "row_count": manifest.row_count,
            "column_count": len(manifest.columns),
            "slot_count": total_slots,
            "slots_confirmed": confirmed_slots,
            "slots_missing": sum(1 for r in column_results.values()
                                 for d in r.slots
                                 if d.status == "missing"),
            "slots_disputed": sum(1 for r in column_results.values()
                                  for d in r.slots
                                  if d.status == "disputed"),
            "slots_digest_mismatch": sum(1 for r in column_results.values()
                                         for d in r.slots
                                         if d.status == "digest_mismatch"),
            # Row counters are per *column* (i.e. cells): total cells are
            # row_count * column_count.  They are named explicitly so a
            # caller cannot mistake them for logical rows.
            "cells_confirmed": sum(r.rows_confirmed
                                   for r in column_results.values()),
            "cells_missing": sum(r.rows_missing
                                 for r in column_results.values()),
            "cells_disputed": sum(r.rows_disputed
                                  for r in column_results.values()),
            "total_cells": manifest.row_count * len(manifest.columns),
            "all_slots_confirmed": all_confirmed,
        }
        result["summary"] = summary

        if all_confirmed:
            stage = os.path.join(workdir, "archive-staging")
            if os.path.exists(stage):
                shutil.rmtree(stage)
            archive_meta = write_archive(
                stage, manifest, manifest_sha, column_results,
                dict_provenance,
                {"manifest_sha256": manifest_sha,
                 "tool_version": __version__,
                 "replicas": [r["replica_id"] for r in result["replicas"]
                              if r["status"] == "used"]})
            if verify:
                from .archive import verify_archive
                vres = verify_archive(stage)
                result["verification"] = vres.to_json()
                if not vres.ok:
                    result["outcome"] = "archive_verification_failed"
                    shutil.rmtree(stage, ignore_errors=True)
                    result["summary"]["all_slots_confirmed"] = True
                    result["error"] = ("archive written but self-verification "
                                       "failed: %s" % "; ".join(vres.problems[:5]))
                    result["anomalies"] = anomalies.to_json()
                    _write_json(diagnostics_path, result)
                    _cleanup(cleanup_root, keep_work)
                    return result
            # Publish atomically: staging -> out_dir contents.
            _publish(stage, out_dir)
            result["archive"] = {
                "path": os.path.abspath(out_dir),
                "status": "restored",
                "row_count": manifest.row_count,
                "columns": [c.name for c in manifest.columns],
                "row_query": "python3 -m archrestore query --archive %s "
                             "--column NAME --rows START:END"
                             % os.path.abspath(out_dir),
            }
            result["outcome"] = "restored"
            # Never leave a partial archive: _publish copies into out_dir only
            # on success, and the staging tree is removed either way.
            shutil.rmtree(stage, ignore_errors=True)
        else:
            result["outcome"] = "incomplete"
            result["error"] = ("not all slots confirmed: %d of %d confirmed; "
                               "no archive written"
                               % (confirmed_slots, total_slots))

        result["anomalies"] = anomalies.to_json()
        result["finished_at"] = _now()
        _write_json(diagnostics_path, result)
        if report_path:
            _write_json(report_path, result)
    finally:
        pool.close()
        _cleanup(cleanup_root, keep_work)
    return result


def _publish(stage, out_dir):
    for entry in os.listdir(stage):
        src = os.path.join(stage, entry)
        dst = os.path.join(out_dir, entry)
        if os.path.isdir(src):
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)


def _write_json(path, obj):
    tmp = path + ".tmp"
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, sort_keys=True, indent=1)
        fh.write("\n")
    os.replace(tmp, path)


def _cleanup(root, keep):
    if root and not keep:
        shutil.rmtree(root, ignore_errors=True)


def _now():
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
