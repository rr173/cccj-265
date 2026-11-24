# archrestore format, version 1

This document is the normative description of the three JSON documents and
the two byte encodings used by `archrestore`, plus the rules that decide
whether a dataset may be called *restored*.

The design keeps three things strictly apart:

* the **trusted handover manifest** — authority, but itself validated;
* **replica evidence** — untrusted; any page may be missing, damaged or lie;
* the **restored archive** — a self-contained, independently verifiable
  output, written only when every slot is confirmed.

Nothing here depends on file names, page numbers, dictionary names or
physical layout for identity. A dictionary is identified by the sha256 of its
bytes and its byte length alone.

---

## 1. Logical values

Supported column types are `string` and `integer`, plus an explicit `null`
that is distinct from every other value.

| logical value        | canonical bytes                                   |
|----------------------|---------------------------------------------------|
| `null`               | `00`                                              |
| `string s`           | `01` · u32le(`len`) · utf8(s)                     |
| `integer n`          | `02` · i64le(n)                                   |

* `utf8(s)` is **strict** UTF-8 over Unicode scalar values: no surrogates, no
  overlong encodings, no BOM special-casing.
* `n` is a signed 64-bit integer, little-endian two's complement.
* Every value is self-delimiting, so a value stream is unambiguous.
* `""` encodes as `01 00000000`; `null` as `00`; the integer `1` as
  `02 0100000000000000`; the string `"1"` as `01 01000000 31`. All four are
  distinct and can never compare equal.

**Canonical text form** (used in reports and in the archive's optional text
exports): a JSON scalar — `json.dumps(s, ensure_ascii=False)` for strings, the
decimal ASCII form for integers, the four bytes `null` for null. The text form
is a *rendering*; digests are always over canonical bytes, never text.

## 2. Digests

* `sha256(x)` means the hex-encoded SHA-256 of the byte string `x`.
* Page and dictionary digests are `sha256` over **exactly the declared byte
  range** (dictionary: the whole blob, not just entries).
* A manifest slot digest covers a contiguous half-open row range of one
  column, in row order:

  ```
  H = sha256()
  H.update("archrestore/slot/v1\0")
  for part in (column.name, column.type):
      H.update(u32le(len(utf8(part))) · utf8(part))
  H.update(i64le(start) · i64le(end))
  for r in [start, end):
      H.update(canonical_value(v_r))
  digest = hex(H.digest())
  ```

  Including the column name, type, `start` and `end` prevents a digest from
  being replayed at a different place in the dataset. Values are fed in row
  order with no separator; self-delimiting encodings make that unambiguous.

## 3. Manifest (`archrestore-manifest`)

```json
{
  "archrestore_format": {"name": "archrestore-manifest", "version": 1},
  "row_count": 1000,
  "columns": [
    {"name": "device",  "type": "string"},
    {"name": "reading", "type": "integer"}
  ],
  "slots": [
    {"column": "device", "slot": 0, "start": 0,   "end": 400, "digest": "…"},
    {"column": "device", "slot": 1, "start": 400, "end": 700, "digest": "…"},
    {"column": "reading","slot": 2, "start": 0,   "end": 250, "digest": "…"}
  ]
}
```

Rules enforced by `parse_manifest` (any violation is a **bad manifest** and
aborts the run — it is never treated as missing data):

* `row_count` is a non-negative integer.
* `columns` is a non-empty list of `{name, type}`; names are unique,
  non-empty, valid UTF-8; types are `string` or `integer`.
* Each slot has exactly `column`, `slot`, `start`, `end`, `digest`.
  `0 ≤ start < end ≤ row_count`; `(column, slot)` is unique; `digest` is 64
  lowercase hex characters.
* For **each column independently**, slots tile `[0, row_count)` exactly:
  sorted by `start`, the first starts at 0, each next starts where the
  previous ended, and the last ends at `row_count`. Gaps, overlaps and
  duplicate coverage are manifest defects, not evidence defects.
* Unknown fields at any level are rejected (closed format).

A manifest with `row_count == 0` must have no slots.

## 4. Replica (`archrestore-replica`)

```json
{
  "archrestore_format": {"name": "archrestore-replica", "version": 1},
  "replica_id": "site-a",
  "pages": [
    {
      "page": 3,
      "column": "reading",
      "start": 450,
      "end": 1000,
      "encoding": "plain",
      "byte_length": 4950,
      "byte_digest": "…",
      "location": {"file": "data.bin", "offset": 123456}
    },
    {
      "page": 7,
      "column": "device",
      "start": 0,
      "end": 1000,
      "encoding": "dict",
      "byte_length": 4000,
      "byte_digest": "…",
      "location": {"file": "data.bin", "offset": 200000},
      "dict_id": "shared-device",
      "dict_digest": "…",
      "dict_byte_length": 31000,
      "dict_location": {"file": "dict.bin", "offset": 0}
    }
  ]
}
```

Field rules:

* `replica_id` is a non-empty string and must equal the ID the operator
  supplied on the command line.
* Page ids are unique within a replica; `column` must exist in the manifest;
  `0 ≤ start < end ≤ row_count`.
* `encoding` is `plain` or `dict`. A `plain` page must not carry any
  `dict_*` field; a `dict` page must carry all four.
* `byte_length` is the exact body length; `byte_digest` is the sha256 of those
  bytes. Both must be right, or the page is unusable.
* `location` is `{file, offset}`; `file` is relative to the replica
  directory, must not be absolute, must not contain `..` or empty components,
  and must resolve (after symlinks) to a regular file inside that directory.
* Pages of one replica may leave gaps, may start and end anywhere, and may
  overlap each other. Overlap is duplicate evidence; it never duplicates a
  restored row.
* A page whose descriptor is malformed is reported and skipped; the rest of
  the replica remains usable. A replica whose *document* is malformed (bad
  JSON, bad header, bad top-level fields) is skipped as a whole.

### 4.1 `plain` page body

The concatenation of one canonical value per row, in ascending row order:
`byte_length` must be consumed exactly by `end - start` values, each of the
column's type. Extra bytes, truncation, wrong type tags or invalid UTF-8 make
the page unusable.

### 4.2 `dict` page body and dictionaries

A `dict` page body is exactly `row_count × u32le(code)`; each code indexes the
dictionary's entry list (0-based). A body whose length is not exactly
`4 × row_count`, or containing a code `≥ count`, is unusable. It is never a
"disputed value": a malformed page is not evidence.

A dictionary blob is

```
"ARCHDICT" · u32le(count) · value[0] · value[1] · … · value[count-1]
```

where each `value` is a canonical value (§1). `byte_length` must be the blob's
exact length and `byte_digest` must be its sha256. Any replica may provide the
bytes: candidates are grouped by the pair `(dict_digest, dict_byte_length)` and
the first candidate that verifies is used. The dictionary's `dict_id`, page
number, file and offset are **provenance only**; they never establish identity.
A dictionary page with no verified candidate is unusable.

## 5. Restoration semantics

For each column:

1. Every usable page is decoded exactly once into its logical row range.
   Evidence is keyed by row coordinate, so a manifest slot may be satisfied
   by a whole page, by a sub-range of a page, or by several sub-ranges
   decoded from different replicas.
2. At each row with evidence, compare all covering decoded values.
   * all equal → **confirmed** row;
   * two or more distinct values → **disputed** row (up to a few samples are
     reported; there is no per-row authority in this format, so no subset
     search is attempted);
   * no usable evidence → **missing** row.
3. For each slot, feed the confirmed rows in order into the slot hash of §2.
   * every row confirmed, digest equals manifest → **confirmed**;
   * every row confirmed, digest differs → **digest_mismatch**;
   * any disputed row → **disputed** (missing rows, if any, are also
     reported);
   * otherwise (missing rows) → **missing**.

`digest_mismatch`, `disputed` and `missing` are distinct diagnoses. Only a
column whose every slot is `confirmed` is complete; only a dataset whose every
column is complete is written as an archive.

## 6. Archive (`archrestore-archive`)

```
<out>/archive.json
<out>/columns/c000.canon     canonical value stream, row order
<out>/columns/c000.idx       u64le block offsets, one per 1024 rows
<out>/checks/index.json      sub-interval → original page/dictionary provenance
<out>/diagnostics.json       the full run report
```

`archive.json` contains:

* `status`: always `"restored"` (an archive is never written otherwise);
* `row_count` and the source `manifest_sha256`;
* per column: `name`, `type`, `file`, `index_file`, `block_rows` (1024),
  `value_count`, the stream's `byte_length` and `sha256`, and the column's
  slot list (`slot`, `start`, `end`, `digest`);
* `checks`: relative path to the provenance index.

The `.canon` stream is exactly the §1 encoding, one value per row in original
row order, so any tool can verify it against the slot digests without this
implementation. `verify_archive` recomputes the stream length, sha256 and
every slot digest from the archive alone; it also checks value types and
UTF-8.

The `.idx` file makes row *n* reachable in O(1) blocks: seek to the block at
row `1024⌊n/1024⌋` and decode at most 1024 values. Individual values are
streamed, so values may exceed memory.

`checks/index.json` records, per slot, the evidence sub-intervals and the
replica/page each came from, plus the verified dictionary provenance. It is a
check index, not a second copy of the data.

## 7. JSONL change package

A periodic change package is JSON Lines.  It binds itself to one baseline
archive content digest and declares the identity fields:

```json
{"archrestore_format":{"name":"archrestore-change-package","version":1},
 "baseline_digest":"<sha256 of baseline content>",
 "identity_fields":["device_id"]}
{"op":"add","identity":{"device_id":7},"row":{"device_id":7,"state":"new"}}
{"op":"delete","identity":{"device_id":2}}
{"op":"update","identity":{"device_id":3},"values":{"state":"seen"}}
```

Identity values use the same JSON scalar mapping as canonical text values:
strings are JSON strings, integers are JSON numbers (never booleans), and
`null` is not allowed in an identity field.  Add rows are complete rows;
update values are a partial field map.  The receiver validates baseline
identity non-nullness/uniqueness, exact one-row matching for delete/update,
newness of add identities, and uniqueness of operated package identities
before publishing a materialized archive.  Output rows are sorted by the
composite identity using UTF-8 byte order for strings and numeric order for
integers.

`archrestore materialize` accepts several neighbouring field/op aliases used
by compare ledgers and common patch formats; the fields above are the primary
format.

## 8. Exit status

| code | meaning |
|------|---------|
| 0 | dataset restored and archive verified |
| 2 | bad/unreadable manifest or invalid invocation |
| 3 | at least one slot unconfirmed (missing / disputed / digest mismatch) |
| 4 | archive written but failed independent self-verification |
