#!/usr/bin/env bash
# Repeatable archrestore compare walkthrough.
#
#   examples/compare_example.sh [workdir] [rows]
#
# Generates two verified archives with known differences via
# examples/compare_demo.py, then runs the compare CLI and the ledger
# re-check.  Offline; Python 3.8+.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$HERE")"
WORK="${1:-$(mktemp -d /tmp/archrestore-compare-sh.XXXXXX)}"
ROWS="${2:-60}"
mkdir -p "$WORK"

PY=(python3 -I -c "import sys; sys.path.insert(0, '$ROOT'); from archrestore.cli import main; sys.exit(main(sys.argv[1:]))")

echo "== generating two restored archives + specs in $WORK =="
python3 -I "$HERE/compare_demo.py" "$WORK" "$ROWS" >/dev/null

BEFORE="$WORK/before/archive"
AFTER="$WORK/after/archive"
SPEC="$WORK/compare-spec.json"
LEDGER="$WORK/changes.cli.jsonl"

echo "== compare =="
"${PY[@]}" compare "$BEFORE" "$AFTER" --spec "$SPEC" --out "$LEDGER"

echo "ledger: $LEDGER ($(wc -l < "$LEDGER") change lines)"
head -n 5 "$LEDGER"

echo "== independent re-check =="
"${PY[@]}" recheck "$LEDGER" --before "$BEFORE" --after "$AFTER" --spec "$SPEC" --json

echo "== spot-check a reported row with the existing query command =="
"${PY[@]}" query --archive "$AFTER" --column device_id --rows 0:1

echo "OK; artefacts kept at $WORK"
