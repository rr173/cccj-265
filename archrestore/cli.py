"""Command line interface.

Commands
--------
* ``restore``  -- repair a dataset from a trusted manifest and replicas.
* ``verify``   -- independently re-verify a restored archive.
* ``query``    -- print rows by original row number.
* ``report``   -- re-print the per-slot diagnostics stored with a run.

Exit status: 0 success, 2 bad manifest / unusable inputs, 3 incomplete
(one or more slots unconfirmed), 4 archive self-verification failed.
"""

import argparse
import json
import os
import sys

from . import __version__
from .archive import ArchiveReader, stream_text, verify_archive
from .errors import ArchError
from .pipeline import run_repair

EXIT_OK = 0
EXIT_BAD_INPUT = 2
EXIT_INCOMPLETE = 3
EXIT_VERIFY_FAILED = 4


def _parse_replica_arg(text):
    if "=" not in text:
        raise argparse.ArgumentTypeError(
            "replica must be REPLICA_ID=DIRECTORY")
    rid, directory = text.split("=", 1)
    if not rid or not directory:
        raise argparse.ArgumentTypeError(
            "replica must be REPLICA_ID=DIRECTORY")
    return rid, directory


def _cmd_restore(args):
    result = run_repair(args.manifest, args.replica, args.out,
                        report_path=args.report, workdir=args.workdir,
                        keep_work=args.keep_work,
                        verify=not args.no_verify)
    _print_summary(result, args.json)
    outcome = result.get("outcome")
    if outcome == "restored":
        return EXIT_OK
    if outcome == "archive_verification_failed":
        return EXIT_VERIFY_FAILED
    if outcome in ("incomplete",):
        return EXIT_INCOMPLETE
    return EXIT_BAD_INPUT


def _print_summary(result, as_json):
    if as_json:
        json.dump(result, sys.stdout, ensure_ascii=False, sort_keys=True,
                  indent=1)
        sys.stdout.write("\n")
        return
    s = result.get("summary") or {}
    print("outcome: %s" % result.get("outcome"))
    if result.get("error"):
        print("error:   %s" % result["error"])
    if s:
        print("rows:    %d logical rows x %d columns = %d cells"
              % (s.get("row_count", 0), s.get("column_count", 0),
                 s.get("total_cells", 0)))
        print("cells:   %d confirmed, %d missing, %d disputed"
              % (s.get("cells_confirmed", 0), s.get("cells_missing", 0),
                 s.get("cells_disputed", 0)))
        print("slots:   %d/%d confirmed, %d missing, %d disputed, %d digest "
              "mismatch"
              % (s.get("slots_confirmed", 0), s.get("slot_count", 0),
                 s.get("slots_missing", 0), s.get("slots_disputed", 0),
                 s.get("slots_digest_mismatch", 0)))
    for col in result.get("columns", []):
        bad = [sl for sl in col.get("slots", []) if sl.get("status") != "confirmed"]
        if bad:
            print("column %r: %d/%d slots unconfirmed"
                  % (col["column"], len(bad), len(col.get("slots", []))))
            for sl in bad[:20]:
                print("  slot %s [%d,%d): %s (missing %d, disputed %d)"
                      % (sl["slot"], sl["start"], sl["end"], sl["status"],
                         sl["missing_rows"], sl["disputed_rows"]))
                for d in sl.get("disputes", [])[:3]:
                    print("    row %d:" % d["row"])
                    for v in d["variants"][:4]:
                        print("      %s  <- %s"
                              % (v["value"],
                                 ", ".join("%s#%d" % (x["replica_id"], x["page"])
                                           for x in v["sources"])))
    for a in (result.get("anomalies") or [])[:20]:
        print("anomaly: %s %s" % (a.get("kind"), a.get("detail", "")))
    if result.get("archive"):
        print("archive: %s" % result["archive"]["path"])


def _cmd_verify(args):
    res = verify_archive(args.archive, problem_limit=args.problem_limit)
    if args.json:
        json.dump(res.to_json(), sys.stdout, ensure_ascii=False, sort_keys=True,
                  indent=1)
        sys.stdout.write("\n")
    else:
        print("archive: %s" % ("OK" if res.ok else "FAILED"))
        print("checked: %d columns, %d slots, %d rows"
              % (res.columns_checked, res.slots_checked, res.rows_checked))
        for p in res.problems:
            print("problem: %s" % p)
    return EXIT_OK if res.ok else EXIT_VERIFY_FAILED


def _parse_rows(text):
    if ":" in text:
        a, b = text.split(":", 1)
        start = int(a) if a else 0
        end = int(b) if b else None
        return start, end
    n = int(text)
    return n, n + 1


def _cmd_query(args):
    with ArchiveReader(args.archive) as reader:
        end_default = reader.header["row_count"]
        start, end = (args.rows if args.rows else (0, end_default))
        if end is None:
            end = end_default
        if start < 0 or end > end_default or start > end:
            raise ArchError("row range [%d, %d) is outside [0, %d)"
                            % (start, end, end_default))
        names = args.column or [c["name"] for c in reader.header["columns"]]
        writer = sys.stdout
        writer.write("[")
        first = True
        for row in range(start, end):
            if not first:
                writer.write(",")
            first = False
            writer.write("\n  {")
            for i, name in enumerate(names):
                if i:
                    writer.write(", ")
                writer.write(json.dumps(name, ensure_ascii=False))
                writer.write(": ")
                raw = reader.read_row_column(name, row)
                for piece in stream_text(raw, reader.pool):
                    writer.write(piece)
            writer.write("}")
        writer.write("\n]\n" if not first else "]\n")
    return EXIT_OK


def _cmd_report(args):
    path = args.path
    if os.path.isdir(path):
        path = os.path.join(path, "diagnostics.json")
    with open(path, "r", encoding="utf-8") as fh:
        result = json.load(fh)
    _print_summary(result, args.json)
    return EXIT_OK if result.get("outcome") == "restored" else EXIT_INCOMPLETE


def build_parser():
    p = argparse.ArgumentParser(
        prog="archrestore",
        description="Offline repair of a logical dataset from unreliable "
                    "replicas, driven by a trusted manifest.")
    p.add_argument("--version", action="version",
                   version="%(prog)s " + __version__)
    sub = p.add_subparsers(dest="command", required=True)

    r = sub.add_parser("restore", help="repair and write an archive")
    r.add_argument("--manifest", required=True)
    r.add_argument("--replica", action="append", required=True,
                   type=_parse_replica_arg, metavar="ID=DIR", default=[])
    r.add_argument("--out", required=True,
                   help="output directory for the archive (or diagnostics)")
    r.add_argument("--report", default=None,
                   help="also write the full JSON report here")
    r.add_argument("--workdir", default=None,
                   help="scratch directory (default: a fresh temp dir)")
    r.add_argument("--keep-work", action="store_true",
                   help="do not delete the scratch directory")
    r.add_argument("--no-verify", action="store_true",
                   help="skip the post-write archive self-verification")
    r.add_argument("--json", action="store_true")
    r.set_defaults(func=_cmd_restore)

    v = sub.add_parser("verify", help="verify a restored archive standalone")
    v.add_argument("--archive", required=True)
    v.add_argument("--json", action="store_true")
    v.add_argument("--problem-limit", type=int, default=100)
    v.set_defaults(func=_cmd_verify)

    q = sub.add_parser("query", help="print rows by original row number")
    q.add_argument("--archive", required=True)
    q.add_argument("--column", action="append", default=None)
    q.add_argument("--rows", type=_parse_rows, default=None,
                   metavar="START:END")
    q.set_defaults(func=_cmd_query)

    rep = sub.add_parser("report", help="print a stored repair report")
    rep.add_argument("path", help="report file or run directory")
    rep.add_argument("--json", action="store_true")
    rep.set_defaults(func=_cmd_report)
    return p


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ArchError as exc:
        sys.stderr.write("archrestore: %s\n" % exc)
        return EXIT_BAD_INPUT
    except BrokenPipeError:
        return EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
