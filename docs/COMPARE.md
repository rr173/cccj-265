# Archive comparison (`archrestore compare`)

This note defines the compare operation added on top of the restore, verify
and query capabilities. It does not change any existing restore/verify/query
behaviour.

## 1. Inputs

```sh
python3 -m archrestore compare BEFORE_ARCHIVE AFTER_ARCHIVE \
    --spec compare-spec.json \
    --out changes.jsonl
```

* `BEFORE_ARCHIVE` and `AFTER_ARCHIVE` are two restored archive directories
  (FORMAT.md §6), which the task premise treats as already independently
  verified. Compare reads them through the existing `ArchiveReader`
  facilities. Pass `--verify` to additionally run the full existing
  `verify_archive` on both sides first (a failing archive exits `4`); this is
  opt-in because it performs a second full read.
* The two archives must have the **same column names with the same types**
  (order does not matter). A missing/extra column or a `string` vs `integer`
  mismatch aborts with exit code `2`.
* The compare spec is JSON. Its one required item is the non-empty list of
  identity columns, documented name `identity_fields` (common aliases such as
  `keys`, `key_fields`, `identity_columns`, `primary_key` and an
  `identity: {fields: [...]}` object are accepted too):

  ```json
  {
    "archrestore_format": {"name": "archrestore-compare-spec", "version": 1},
    "identity_fields": ["rack", "device_id"]
  }
  ```

  An inline JSON string is accepted by the API and the CLI `--spec` option.
  Identity columns must exist in both archives and be listed once.

`diff` and `compare-archives` are CLI aliases of `compare`.

## 2. Identity, empty identities and uniqueness

* An identity is the ordered tuple of the **canonical values** (FORMAT.md §1)
  of the identity columns. Internally the composite key is the concatenation
  of the self-delimiting canonical encodings, which is injective:
  `("a","bc")` and `("ab","c")` can never collide.
* A **null in any identity column makes the identity empty/invalid**. Such a
  row is reported as an `empty_identity` problem with its **original 0-based
  archive row number** and the list of null identity fields.
* Identities must be **unique within each archive**. A repeated identity is
  reported as a `duplicate_identity` problem carrying the identity values and
  **all original row numbers** that carry it.
* Empty and duplicate identities are fatal to the comparison: the tool never
  guesses a pairing and never publishes a partial ledger. It scans **both**
  archives completely first, so all such problems (on either side) are
  reported together. Exit code is `2`.

  The human/JSON report keeps the first `--problem-limit` problems in memory;
  the complete, unbounded problem list is streamed as JSONL to
  `--problems report.jsonl` (or to a preserved temporary file whose path is in
  `report["problems_path"]`). A hugely duplicated identity streams its row
  array and never grows memory with its row count.

## 3. Comparison and ledger schema

Every **non-identity** column is compared by exact type-and-value equality
(canonical byte equality, so string `"1"` and integer `1` are different, and
`null` is an ordinary comparable value for non-identity columns). Only columns
that actually differ are listed per event. Unchanged identities produce no
ledger line.

The ledger is JSONL, one event per line, in one deterministic order:
lexicographic order of the identity tuple — strings by UTF-8/binary order,
integers numerically, multiple fields lexicographically. (This matches an
SQLite-style `ORDER BY identity_fields`.) Two runs on the same archives and
spec produce byte-identical files.

Record shape (compatibility row aliases are included):

```json
{"op":"added","type":"added","action":"add","change_type":"added",
 "identity":{"device_id":"D-00000012"},"identity_values":["D-00000012"],
 "device_id":"D-00000012",
 "row":11,"before_row":null,"after_row":11,"old_row":null,"new_row":11,
 "rows":{"before":null,"after":11,"old":null,"new":11,"a":null,"b":11},
 "side":"after"}
```

```json
{"op":"removed","type":"removed","action":"remove",
 "identity":{"device_id":"D-00000009"}, "...": "...",
 "row":9,"before_row":9,"after_row":null,
 "rows":{"before":9,"after":null}, "side":"before"}
```

```json
{"op":"changed","type":"changed","action":"change",
 "identity":{"device_id":"D-00000006"},
 "before_row":6,"after_row":6,
 "rows":{"before":6,"after":6}, "side":"both",
 "changed_columns":["reading"],
 "changes":{"reading":{"before":274,"after":1274,"old":274,"new":1274}}}
```

* `added` exists only in `AFTER`; `removed` only in `BEFORE`.
* `row` is the single existing-side row for add/remove.
* `before_row`/`after_row` are the two original row numbers for `changed`.
* `changes.<column>` gives both values (`before`/`after`, with `old`/`new`
  aliases). JSON values use the canonical text mapping: strings are JSON
  strings, integers are JSON numbers, null is `null`.

## 4. Atomicity and failure behaviour

* All schema/identity validation finishes before any ledger destination is
  created or written.
* `--out changes.jsonl` is written to a temporary file in the same directory
  and atomically `rename`d into place only after the complete ledger is
  produced. On any failure the temp file is removed and the requested path is
  never created or replaced: a pre-existing ledger is left untouched.
* With no `--out` (or `--out -`) the ledger is JSONL on stdout; the summary is
  then written to stderr so stdout stays a clean JSONL stream.

Exit codes: `0` compared (changes may be present), `2` bad spec/invocation,
schema mismatch, or empty/duplicate identity problems, `4` an archive failed
self-verification.

## 5. Memory boundary and external sort

Neither archive, nor the full identity set, nor all records are loaded into
memory:

1. Each archive is scanned row by row through the existing block-indexed
   cursors. Only compact `(order_key, canonical_key, row)` records are built.
2. Records are sorted into bounded runs (`--` API: `run_records`,
   `run_bytes`) and spilled; runs are merged with a cascading k-way merge of
   bounded fan-in. Duplicate detection is an adjacent-key streaming scan.
3. Pairing is a streaming merge of the two unique sorted files. Matched rows
   use persistent block cursors with forward-within-block seeking, so a
   monotone pairing reads each value once and any jump costs at most one
   1024-row block skip.
4. Unchanged columns are compared chunk-wise from canonical bytes. Changed
   values are streamed into the JSONL line through the same bounded-chunk
   path used by `query`; a value far larger than memory is never joined into
   one Python string.

The default run budget keeps peak Python allocation around 1 MiB for compact
identities regardless of row count (see
`tests/test_compare.py::MemoryBoundTests`).

## 6. Independent re-check

Every line carries the identity values, the original row number(s) and both
reported values, so a reviewer can reopen the archives with `query` (or
`ArchiveReader`) and verify each claim directly. The command

```sh
python3 -m archrestore recheck changes.jsonl \
    --before BEFORE_ARCHIVE --after AFTER_ARCHIVE --spec compare-spec.json
```

re-runs the deterministic comparison and checks that the ledger is exactly the
expected event sequence (content and order); it exits `0` only on full match.

## 7. Repeatable generator

```sh
python3 -I examples/compare_demo.py /tmp/compare-demo 200
```

builds two restored archives with deterministic additions, removals, row
reordering and field changes, plus a null/duplicate-identity failure fixture,
runs compare, and prints the ledger and the equivalent CLI command.
`tests/test_compare.py` is the repeatable automated check (including a
randomised differential test and forced multi-run external sorts).
