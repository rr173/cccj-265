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
from .compare import (
    CompareError,
    CompareFailure,
    CompareSpec,
    coerce_compare_spec,
    compare_archives,
    read_ledger,
)
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


def _resolve_compare_invocation(args):
    """Accept both flag-based and positional archive/spec arguments.

    Supported positional shapes::

        compare BEFORE AFTER --spec spec.json
        compare BEFORE AFTER spec.json
        compare spec.json BEFORE AFTER      (classified by file vs directory)
    """
    before = getattr(args, "before", None)
    after = getattr(args, "after", None)
    spec = getattr(args, "spec", None)
    positional = list(getattr(args, "paths", None) or [])

    if len(positional) == 3 and not (before and after):
        # Classify: the existing file is the spec; the two directories keep
        # their relative order as before/after.
        files = [p for p in positional if os.path.isfile(p)]
        dirs = [p for p in positional if not os.path.isfile(p)]
        if spec is None and len(files) == 1 and len(dirs) == 2:
            spec = files[0]
            if before is None:
                before = dirs[0]
            if after is None:
                after = dirs[1]
        else:
            if before is None:
                before = positional[0]
            if after is None:
                after = positional[1]
            if spec is None:
                spec = positional[2]
    elif len(positional) >= 2:
        if before is None:
            before = positional[0]
        if after is None:
            after = positional[1]
        if len(positional) >= 3 and spec is None:
            spec = positional[2]
    elif len(positional) == 1:
        raise ArchError(
            "compare needs two archives (and a --spec); got one positional "
            "path %r" % positional[0])

    if not before or not after:
        raise ArchError(
            "compare requires BEFORE and AFTER archive directories "
            "(use --before/--after or two positional paths)")
    if not spec and not getattr(args, "identity_fields", None):
        raise ArchError(
            "compare requires a JSON spec (--spec FILE) listing the identity "
            "fields, or --identity-field NAME")
    return before, after, spec


def _compare_spec_from_args(args, spec_path):
    flag_fields = []
    # action="append", nargs="+" yields a list of lists; flatten and also
    # accept comma-separated names within any token.
    for group in getattr(args, "identity_fields", None) or []:
        items = group if isinstance(group, (list, tuple)) else [group]
        for raw in items:
            for part in str(raw).split(","):
                part = part.strip()
                if part:
                    flag_fields.append(part)
    if spec_path:
        spec = coerce_compare_spec(spec_path)
        if flag_fields:
            spec = CompareSpec(flag_fields, exclude=spec.exclude,
                               include=spec.include, order=spec.order,
                               raw=spec.raw, path=spec.path)
        return spec
    if not flag_fields:
        raise ArchError("at least one identity field is required")
    return CompareSpec(flag_fields)


def _print_compare_human(report, stream=None):
    stream = stream or sys.stdout
    s = report.get("summary", report)
    print("outcome: %s" % report.get("outcome"), file=stream)
    print("identities: %d added, %d removed, %d changed, %d unchanged"
          % (s.get("added", 0), s.get("removed", 0),
             s.get("changed", 0), s.get("unchanged", 0)), file=stream)
    if report.get("ledger_path"):
        print("ledger: %s" % report["ledger_path"], file=stream)


def _cmd_compare(args):
    try:
        before, after, spec_path = _resolve_compare_invocation(args)
        spec = _compare_spec_from_args(args, spec_path)
    except ArchError as exc:
        if getattr(args, "json", False):
            json.dump({"ok": False, "outcome": "bad_compare_invocation",
                       "status": "failed", "error": str(exc)},
                      sys.stdout, ensure_ascii=False, indent=1)
            sys.stdout.write("\n")
        else:
            sys.stderr.write("archrestore: %s\n" % exc)
        return EXIT_BAD_INPUT

    if getattr(args, "order", None):
        spec.order = args.order

    out_target = getattr(args, "out", None)
    to_stdout = not out_target or out_target == "-"
    # When the ledger itself is on stdout, all reports/summaries move to
    # stderr so the JSONL stream stays clean and independently parseable.
    report_stream = sys.stderr if to_stdout else sys.stdout

    kwargs = dict(
        spec=spec,
        verify=getattr(args, "verify", False),
        workdir=getattr(args, "workdir", None),
        keep_work=getattr(args, "keep_work", False),
        problem_limit=getattr(args, "problem_limit", 100),
        problems_out=getattr(args, "problems", None),
    )

    try:
        if to_stdout:
            report = compare_archives(before, after, out=sys.stdout, **kwargs)
        else:
            report = compare_archives(before, after, out=out_target, **kwargs)
    except CompareFailure as exc:
        report = exc.report
        if args.json:
            json.dump(report, sys.stdout, ensure_ascii=False, sort_keys=True,
                      indent=1)
            sys.stdout.write("\n")
        else:
            # Failure reports go to stdout (like `restore` summaries); no
            # ledger bytes were emitted, so stdout stays a readable report.
            out = sys.stdout
            print("compare failed: %s" % report.get("error"), file=out)
            for p in report.get("problems", [])[:args.problem_limit]:
                print("problem: %s" % _format_problem(p), file=out)
            sp = report.get("problems_path")
            if sp:
                print("full problem report: %s" % sp, file=out)
        return EXIT_BAD_INPUT
    except CompareError as exc:
        if args.json:
            json.dump({"ok": False, "outcome": "compare_error",
                       "error": str(exc)}, sys.stdout, ensure_ascii=False,
                      indent=1)
            sys.stdout.write("\n")
        else:
            sys.stderr.write("archrestore: %s\n" % exc)
        return getattr(exc, "exit_code", EXIT_BAD_INPUT)

    if args.json:
        # With a stdout ledger, keep stdout as pure JSONL and send the
        # summary JSON to stderr.
        json_stream = sys.stderr if to_stdout else sys.stdout
        json.dump(report, json_stream, ensure_ascii=False, sort_keys=True,
                  indent=1)
        json_stream.write("\n")
    elif not getattr(args, "quiet", False):
        _print_compare_human(report, stream=report_stream)
    return EXIT_OK


def _format_problem(p):
    kind = p.get("kind")
    side = p.get("side")
    if kind == "empty_identity":
        return ("empty_identity in %s archive at row %d (null fields: %s)"
                % (side, p.get("row"), p.get("null_fields")))
    if kind == "duplicate_identity":
        rows = p.get("rows")
        more = ""
        if p.get("rows_truncated"):
            more = " (shown rows truncated; row_count=%s)" % p.get("row_count")
        return ("duplicate_identity in %s archive %r at rows %s%s"
                % (side, p.get("identity"), rows, more))
    return json.dumps(p, ensure_ascii=False, sort_keys=True)


def _cmd_recheck(args):
    """Independently re-check a published ledger against its two archives."""
    spec = _compare_spec_from_args(args, args.spec)
    ledger = args.ledger
    if not os.path.isfile(ledger):
        raise ArchError("ledger %r does not exist" % ledger)
    expected = compare_archives(args.before, args.after, spec, out=None,
                                 verify=getattr(args, "verify", False))
    expected_records = list(read_ledger(expected["ledger_path"]))
    actual_records = list(read_ledger(ledger))
    ok = expected_records == actual_records
    result = {
        "ok": ok,
        "ledger": os.path.abspath(ledger),
        "expected_events": len(expected_records),
        "ledger_events": len(actual_records),
    }
    if not ok:
        # Find the first discrepancy for an actionable message.
        for i, (e, a) in enumerate(zip(expected_records, actual_records)):
            if e != a:
                result["first_difference_line"] = i + 1
                break
        if len(expected_records) != len(actual_records):
            result["reason"] = "event count differs"
        else:
            result["reason"] = "event content/order differs"
    if args.json:
        json.dump(result, sys.stdout, ensure_ascii=False, indent=1,
                  sort_keys=True)
        sys.stdout.write("\n")
    else:
        print("ledger: %s" % ("OK" if ok else "MISMATCH"))
        print("events: %d in ledger, %d expected"
              % (len(actual_records), len(expected_records)))
    return EXIT_OK if ok else EXIT_BAD_INPUT


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

    _add_compare_parser(sub, "compare")
    _add_compare_parser(sub, "diff", aliases=())
    _add_compare_parser(sub, "compare-archives", aliases=("archive-diff",))

    rc = sub.add_parser(
        "recheck",
        help="re-verify a compare JSONL ledger against its two archives")
    rc.add_argument("ledger", help="JSONL ledger to re-check")
    rc.add_argument("--before", "--old", "--a", dest="before", required=True)
    rc.add_argument("--after", "--new", "--b", dest="after", required=True)
    rc.add_argument("--spec", "-s", required=True)
    rc.add_argument("--identity-field", "--identity-fields", "--key",
                    "--keys", dest="identity_fields", action="append",
                    default=[])
    rc.add_argument("--verify", dest="verify", action="store_true",
                    default=False)
    rc.add_argument("--no-verify", dest="verify", action="store_false")
    rc.add_argument("--json", action="store_true")
    rc.set_defaults(func=_cmd_recheck)
    return p


def _add_compare_parser(sub, name, aliases=None):
    c = sub.add_parser(
        name, aliases=list(aliases or []),
        help="compare two restored archives into a JSONL change ledger",
        description="Compare two restored archrestore archives using a JSON "
                    "spec that lists one or more identity fields. Emits a "
                    "deterministically ordered JSONL ledger of added, removed "
                    "and changed identities.")
    c.add_argument("paths", nargs="*",
                   help="BEFORE_ARCHIVE AFTER_ARCHIVE [SPEC_JSON]")
    c.add_argument("--before", "--archive-a", "--a", "-a", "--old", "--left",
                   "--base", "--archive1", "--first", "--from", "--source",
                   "--old-archive", dest="before",
                   default=None, help="the older/before archive directory")
    c.add_argument("--after", "--archive-b", "--b", "-b", "--new", "--right",
                   "--target", "--archive2", "--second", "--to", "--dest",
                   "--new-archive", dest="after",
                   default=None, help="the newer/after archive directory")
    c.add_argument("--spec", "-s", "--config", "--instructions",
                   "--compare-spec", "--spec-file", dest="spec", default=None,
                   help="JSON compare spec listing identity_fields")
    c.add_argument("--identity-field", "--identity-fields", "--identities",
                   "--id-fields", "--key", "--keys",
                   dest="identity_fields", action="append", nargs="+",
                   default=[], metavar="FIELD",
                   help="identity column; give several after one flag or "
                        "repeat the flag (comma-separated also accepted); "
                        "overrides the spec's identity list")
    c.add_argument("--out", "-o", "--output", "--output-file", "--ledger",
                   dest="out", default=None,
                   help="write the JSONL ledger here atomically "
                        "(default: stdout)")
    c.add_argument("--problems", "--problems-out", dest="problems",
                   default=None,
                   help="on identity failure, publish the full JSONL problem "
                        "report here")
    c.add_argument("--workdir", "--temp-dir", dest="workdir", default=None,
                   help="scratch directory for external sorts")
    c.add_argument("--keep-work", action="store_true",
                   help="keep the scratch directory after the run")
    c.add_argument("--verify", dest="verify", action="store_true",
                   default=False,
                   help="fully self-verify both archives first (slow full "
                        "read); archives are normally already verified")
    c.add_argument("--no-verify", "--skip-verify", dest="verify",
                   action="store_false",
                   help="do not run full self-verification (default)")
    c.add_argument("--problem-limit", type=int, default=100,
                   help="max problems kept in the JSON/human report")
    c.add_argument("--order", choices=("logical", "canonical"), default=None,
                   help="ledger ordering (default: natural identity order)")
    c.add_argument("--json", action="store_true",
                   help="print the summary report as JSON (to stdout; with a "
                        "stdout ledger the summary goes to stderr)")
    c.add_argument("--quiet", "-q", action="store_true",
                   help="suppress the human summary")
    c.set_defaults(func=_cmd_compare)
    return c


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
