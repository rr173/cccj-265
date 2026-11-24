# Periodic package validation (`archrestore validate-period`)

A periodic delivery bundles several independently produced archive members —
for example a device ledger and the inspection points that reference it. Each
member may be individually complete, yet the receiver still has to prove that
the whole group is coherent: every referencing record must point at exactly
one unique referenced record. `validate-period` checks the whole group from
one small JSON descriptor and, only when every check passes, writes a period
manifest that can be re-checked without this tool.

## 1. The period descriptor

The descriptor is a closed JSON document,
`archrestore-period-package` version 1:

```json
{
  "archrestore_format": {"name": "archrestore-period-package", "version": 1},
  "period_id": "2026-W48",
  "members": [
    {"name": "devices", "kind": "archive", "path": "devices-archive",
     "fields": [{"name": "device_id", "type": "integer"}]},
    {"name": "points", "kind": "jsonl", "path": "points.jsonl",
     "fields": [{"name": "device_ref", "type": "integer"}]}
  ],
  "relations": [
    {"name": "points->devices",
     "from": {"member": "points",  "fields": ["device_ref"]},
     "to":   {"member": "devices", "fields": ["device_id"]}}
  ]
}
```

* `members` is a non-empty list. Each member has a unique non-empty `name`, a
  `kind` of `archive` (a restored archrestore archive directory) or `jsonl`
  (one JSON object per line), a relative `path` (resolved against the
  descriptor's directory; absolute and `..` paths are rejected), and the
  ordered list of fields the relations may use, each with a declared
  `type` of `string` or `integer`.
* `relations` pairs a referencing endpoint (`from`) with a referenced
  endpoint (`to`). Each endpoint names a member and an ordered list of
  fields. The two field lists must have the same length; fields are paired
  positionally and each pair must have the **same declared type**. Unknown
  keys, duplicate member/relation names, unknown endpoint members, fields
  absent from a member, and incompatible paired types are descriptor errors
  that abort before any data is read (exit code `2`).
* A self-relation (both endpoints on one member) and multiple relations are
  allowed; a member may appear in several relations.

## 2. Keys, types and equality

* A relation may use one field or several. A composite key is the
  concatenation of the fields' canonical encodings (FORMAT.md §1), so the
  tuple is injective: `("a","bc")` and `("ab","c")` can never collide.
* Equality is exact **type-and-value** equality per field. The string `"1"`
  never equals the integer `1`; `null` is its own distinct value.
* A JSON value must belong to the declared field type. Booleans are never
  valid, an integer outside signed 64-bit range is invalid, and a value of
  the wrong JSON type is a `field_type_mismatch`. A missing key or an
  explicit `null` encodes as a null key component.

## 3. The validation rules

The period is valid only when every rule holds for every relation:

1. **Referenced keys are non-null.** A composite key counts as empty only
   when *every* component is null (a `(null, 2)` tuple is a real, non-empty
   key). An empty key on the `to` side is an `empty_target_key`, reported per
   referenced row.
2. **Referenced keys are unique.** A repeated referenced key is a
   `duplicate_target_key`; every archive row / JSONL line carrying it is
   named, together with the full duplicate row list.
3. **Every reference matches exactly one referenced record.**
   * a null reference key is `empty_reference_key`;
   * a key with no referenced record is `unmatched_reference`;
   * a key matching more than one referenced record is
     `ambiguous_reference` (the target rows are listed).
4. **Every member is itself well-formed.** An archive that cannot be opened,
   is missing a declared field, or declares a different field type is a
   `member_error`. A JSONL line that is not valid UTF-8/JSON or is not a JSON
   object is an `invalid_record`; that line is excluded from the key scans.

Any failure invalidates the whole period. Diagnostics are never collapsed:
all problems across every member and relation are reported together, each
carrying the `member`, the `relation` (null for member-level findings) and
the original row — the 0-based archive row number or the 1-based JSONL line
number.

## 4. The period manifest

On full success the period manifest (`archrestore-period-manifest`,
status `valid`) is written atomically (temp file + rename) to `--out`. It is
the only artifact that represents a valid period, and it is created **only**
on success:

* `period_id`, the `descriptor_sha256`, and a self-covering
  `manifest_digest`;
* one entry per member with its kind, resolved path, declared fields,
  record count, `source_sha256` and a domain-separated `content_digest`
  (`archrestore/archive-content/v1` for archives — the same value printed by
  `archrestore digest`; `archrestore/jsonl-content/v1` for JSONL members);
* one entry per relation with the field pairs and the number of references
  and matched references.

A reviewer can re-open each member, recompute its content digest and confirm
the referential claims independently of this tool. With no `--out` (or
`--out -`) the manifest JSON is streamed to stdout and the human summary goes
to stderr, so stdout stays a single clean document.

## 5. Failure behaviour and exit codes

* All validation completes before publication. On failure no manifest is
  created and an existing `--out` file is left untouched.
* The full, unbounded diagnosis is an ordered JSONL report. Use
  `--problems report.jsonl` to publish it at a chosen path; otherwise it is
  preserved in a temporary file whose path is in the JSON report
  (`problems_path`). The on-screen/JSON report keeps the first
  `--problem-limit` problems in memory.
* Diagnostics have one stable total order — member, then relation, then
  problem kind, then original row — independent of external-sort run
  boundaries, so two runs on the same inputs produce byte-identical reports.

Exit codes: `0` valid and manifest written, `2` bad descriptor / bad
invocation, `3` one or more members or relations failed (no manifest).

## 6. Memory boundary

Neither member is loaded whole. Each member is streamed through bounded
in-memory runs that spill and merge with a cascading k-way external sort
(`--run-records`, `--run-bytes`, with bounded fan-in). Target uniqueness is an
adjacent-key streaming scan and the referential check is a streaming two-way
merge join; the only buffered target group is the current equal-key group,
which is a single row for a valid period. The diagnosis itself is
externally sorted, so millions of bad rows cannot grow memory. A member used
by more than one relation is scanned once per relation, each scan still
O(run budget) in memory regardless of row count. Peak Python allocation
stays around 1–2 MiB for compact keys even at tens of thousands of rows.

## 7. Usage

```sh
# Validate and write the period manifest (exit 0 only when fully valid)
python3 -m archrestore validate-period period.json --out period-manifest.json

# Stream the manifest to stdout; human summary on stderr
python3 -m archrestore validate-period period.json

# On failure, publish the full ordered diagnosis
python3 -m archrestore validate-period period.json \
    --out period-manifest.json --problems diagnosis.jsonl

# Also self-verify every archive member first
python3 -m archrestore validate-period period.json --verify --out m.json
```

`period` and `validate-package` (plus `validate-group`, `period-validate`,
`check-period`) are command aliases.
