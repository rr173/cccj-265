# Materializing the next archive

`materialize` rolls a complete baseline archive forward with a JSONL change
package.  The result is an ordinary archrestore archive: use the existing
`verify` and `query` commands unchanged.

## Command

```sh
python3 -m archrestore materialize \
    --baseline baseline-archive \
    --package changes.jsonl \
    --out next-archive

python3 -m archrestore verify --archive next-archive
python3 -m archrestore query --archive next-archive --rows 0:20
```

`apply`, `apply-changes`, `materialise`, `patch`, `roll-forward` and
`next-archive` are command aliases.  The common positional form is also
accepted:

```sh
python3 -m archrestore materialize baseline-archive changes.jsonl next-archive
```

## Binding and identity declaration

The first non-empty JSONL line is normally an envelope:

```json
{
  "archrestore_format": {"name": "archrestore-change-package", "version": 1},
  "baseline_digest": "<64 lowercase hex chars>",
  "identity_fields": ["rack", "device_id"]
}
```

Print the canonical baseline content digest with:

```sh
python3 -m archrestore digest baseline-archive
```

The digest is domain-separated and covers row count, column names/types and
every canonical column stream.  For compatibility, packages may use
`baseline_sha256`, `base_digest`, `archive_digest`, `content_digest` or a
nested `baseline: {sha256: ...}` declaration; several common whole-archive
summary constructions are accepted.  An explicit `--spec` and
`--baseline-digest` can supply the two envelope requirements for operation-only
JSONL.

## Operations

```jsonl
{"op":"add","identity":{"id":7},"row":{"id":7,"state":"new","qty":3}}
{"op":"delete","identity":{"id":2}}
{"op":"update","identity":{"id":3},"values":{"qty":31}}
```

Accepted vocabulary includes `insert/create/added`,
`remove/delete/deleted`, and `change/modify/patch/updated`; `key`, `keys`,
`where`, flat identity fields, compare-ledger `identity_values`, and
`row/record/values/data/changes` payloads are accepted as aliases.  Compare
style `changes: {"qty": {"before":30,"after":31}}` is also understood.

Rules:

* baseline identity values must be non-null and unique;
* delete/update must match one baseline record;
* an add identity must be absent from the baseline;
* a package may operate on any one lookup identity at most once;
* values are strictly checked against the baseline column types;
* all validation and staging verification finish before the output directory
  is published.

The output rows are sorted by the identity tuple: strings by strict UTF-8
byte order and integers numerically, with fields compared in declaration
order.  Every column for an action is written before moving to the next row,
so columns remain record-aligned.

## Memory and failure behaviour

Baseline identities and resulting actions use the same bounded external frame
sort as `compare`.  The complete baseline table is never assembled in memory.
Unchanged values are copied from canonical streams in bounded chunks.

A malformed package, digest mismatch, null/duplicate baseline identity,
missing/ambiguous operation match, duplicate package operation, or resulting
identity collision exits `2` and does not publish the requested archive.  A
staged archive that fails standalone verification exits `4`.
