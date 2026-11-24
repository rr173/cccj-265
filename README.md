# archrestore

Offline repair of a logical tabular dataset from several unreliable physical
replicas, driven by a trusted handover manifest.

Three machine rooms each hold a copy of an old device-inspection dataset.
Every copy has bad pages. The copies are *logical* replicas: they may use
different page boundaries, different dictionaries, different encodings and
different file layouts, so they cannot be concatenated. This tool restores the
original logical dataset from a trusted manifest that lists, per column, the
expected values' digests over disjoint logical row ranges, and hands back a
self-contained archive that can be queried by original row number and
re-verified without this tool. If it cannot restore everything, it says
exactly which evidence is missing instead of pretending.

See [`FORMAT.md`](FORMAT.md) for the normative format and
[`docs/CONTRACT.md`](docs/CONTRACT.md) for the acceptance rules. There is no
third-party dependency; Python 3.8+ is enough.

## What it guarantees

* **The manifest is authority, and is validated.** Gaps, overlaps, duplicate
  slot ids, wrong types, out-of-range bounds or non-tiling slots are a *bad
  manifest* and abort the run. They are never confused with missing data.
* **Replicas are evidence, never authority.** A page is used only if its
  declared bytes hash to the declared digest, its length is exactly right and
  its body decodes canonically. Bad pages are reported and skipped; the rest
  of the replica stays usable.
* **Dictionaries are identified by content.** A dictionary is `(sha256,
  byte_length)`; its name, page number, file and offset are provenance only.
  A verified dictionary may come from any replica, including one whose pages
  are otherwise useless.
* **Rows, not pages, are compared.** A slot may be satisfied by a page, by
  part of a page, or by ranges stitched from several replicas. Overlap is
  duplicate evidence and never duplicates a restored row.
* **Three failure modes stay distinct.** Missing rows (no usable evidence),
  disputes (two or more distinct decoded values), and digest mismatch (all
  rows present and agreed but the slot hash differs) are reported separately
  per slot.
* **"Restored" means restored.** The archive is written only if every slot of
  every column is confirmed. Anything less is reported slot by slot and the
  exit status is non-zero.
* **Memory stays flat.** Values, columns and slots are streamed. A 24 MiB
  value, a 20 000-row slot and a 1000-page replica all run in a few MiB of
  working set; a page is decoded exactly once and never re-opened per row.

## Usage

```sh
# Restore into ./archive (must not exist or must be empty)
python3 -m archrestore restore \
    --manifest manifest.json \
    --replica site-a=/data/site-a \
    --replica site-b=/data/site-b \
    --replica site-c=/data/site-c \
    --out archive

# Verify the archive from its own bytes (no manifest, no replicas)
python3 -m archrestore verify --archive archive

# Read rows by original row number
python3 -m archrestore query --archive archive --column device --rows 250:260

# Compare two restored inspections into a JSONL change ledger
cat > compare-spec.json <<'JSON'
{"identity_fields": ["device_id"]}
JSON
python3 -m archrestore compare before-archive after-archive \
    --spec compare-spec.json --out changes.jsonl

# Re-verify a published ledger against the two archives
python3 -m archrestore recheck changes.jsonl \
    --before before-archive --after after-archive --spec compare-spec.json

# Bind a JSONL change package to a baseline and materialize the next archive
python3 -m archrestore digest archive
python3 -m archrestore materialize \
    --baseline archive --package next-changes.jsonl --out next-archive

# Re-print the diagnosis of a run
python3 -m archrestore report archive
```

`restore` prints per-slot status. Row counters in the JSON report are cell
counters (logical rows × columns): look at `row_count`, `column_count`,
`cells_confirmed`, `cells_missing`, `cells_disputed` and per-slot `status`.

Every replica directory contains a `replica.json` descriptor plus the data
files it points at. Exit codes: `0` restored, `2` bad manifest / bad
invocation, `3` some slot unconfirmed, `4` archive failed self-verification.

## The archive

```
archive/
  archive.json            status, row_count, per-column stream hashes, slot digests
  columns/c000.canon      canonical values, one per row, original order
  columns/c000.idx        block offsets for O(1) random access
  checks/index.json       per slot: evidence sub-intervals -> replica/page/dictionary
  diagnostics.json        the full run report
```

`verify_archive` recomputes every slot digest from `columns/*.canon` alone.
The check index points each restored sub-interval at the original page and
verified dictionary bytes it came from, so a reviewer can audit the result
without trusting this tool's decoder.

## Comparing two restored archives

`compare` answers what changed between two restored inspections. Give it the
two archive directories and a JSON spec listing one or more **identity
fields**:

```json
{"identity_fields": ["rack", "device_id"]}
```

* The archives must share the same column names and types.
* Identity is the tuple of the fields' **canonical values**. A null in any
  identity field is an empty identity; identities must be unique in each
  archive. Empty/duplicate identities abort the compare and are reported with
  their original row numbers; no pairing is guessed and **no partial ledger is
  published** (an existing `--out` file is left untouched).
* Every other column is compared by exact type-and-value equality.
* The JSONL ledger lists `added` / `removed` / `changed` identities in a
  deterministic identity order. Changed entries list only the columns that
  changed with both values; every entry carries the original row number on the
  existing side, or `before_row`/`after_row` for changes. Unchanged devices
  are omitted. Writes to `--out` are atomic (temp file + rename).
* Comparison is **out of core**: identities are externally sorted and merged,
  so neither archive nor the full identity/record set is loaded into memory;
  peak Python allocation stays around 1 MiB for compact keys regardless of
  row count. See [`docs/COMPARE.md`](docs/COMPARE.md) for the full schema,
  memory contract and exit codes.

```sh
python3 -I examples/compare_demo.py /tmp/compare-demo 200   # generator
bash examples/compare_example.sh /tmp/compare-sh 200        # full walkthrough
```

`recheck` independently re-runs the deterministic comparison and proves a
published ledger is exactly the expected event sequence.

## Materializing the next archive

A receiver that has only the previous complete archive and a bound JSONL
change package can run `materialize` to produce the next complete archive:

```sh
python3 -m archrestore digest previous-archive
python3 -m archrestore materialize \
    --baseline previous-archive \
    --package next-changes.jsonl \
    --out next-archive
python3 -m archrestore verify --archive next-archive
python3 -m archrestore query --archive next-archive --rows 0:20
```

The package declares `baseline_digest` and one or more `identity_fields`.
Its records add complete rows, delete one identity, or partially update an
existing record.  Baseline identities must be non-null and unique; deletes and
updates must match exactly one row; adds must use a new identity; and each
identity may be operated on at most once per package.  Digest mismatch,
identity conflicts or malformed packages abort before publication.  Output is
sorted by the identity tuple (UTF-8 byte order for strings, numeric order for
integers), keeps every column aligned, and is built with external sorting and
streaming canonical copies rather than loading the dataset into memory.  See
[`docs/MATERIALIZE.md`](docs/MATERIALIZE.md) for the package schema and CLI
aliases.

## Contract at a glance

| situation | reported as |
|-----------|-------------|
| manifest gaps/overlaps/wrong types | `bad_manifest`, run aborts |
| replica document malformed | replica skipped (`replica_rejected`) |
| page length/digest/body wrong | page skipped (`page_rejected`) |
| dictionary missing/unverifiable/wrong length | dict pages unusable (`dictionary_unavailable`) |
| dictionary code out of range | page skipped (`dict_code_out_of_range`) |
| no usable evidence for some rows | slot `missing` |
| ≥2 distinct decoded values | slot `disputed` (with samples) |
| all rows agree but hash differs | slot `digest_mismatch` |
| every slot confirmed | archive written, `restored` |

## Reproducing offline

```sh
python3 -I -m unittest discover -s tests -v   # synthetic fixtures, incl. compare
python3 -I examples/demo.py /tmp/demo          # three damaged replicas -> archive
```

`examples/demo.py` builds a three-replica dataset with different page
boundaries, encodings and shared dictionaries, damages one page, repairs it,
and prints the report and sample rows. Everything is offline.

## Layout

```
archrestore/
  format.py   canonical values, digests, strict manifest/replica validation
  spill.py    out-of-core decoded-value staging and block-indexed cursors
  decode.py   single-pass page/dictionary verification and decoding
  resolve.py  segment sweep, row agreement, per-slot diagnosis
  archive.py  archive writer, standalone verifier, random-access reader
  compare.py  out-of-core two-archive comparison and JSONL change ledger
  materialize.py bound JSONL package application and sorted next archive
  pipeline.py orchestration, failure taxonomy, reporting
  cli.py      restore / verify / query / report / compare / recheck
  synth.py    deterministic synthetic datasets for tests and the demo
tests/        unittest suite (format, repair, resources, randomised, CLI, compare)
```

## Security posture

Replicas are untrusted data. All payload reads are positional and bounded by
declared lengths; paths must resolve inside their replica directory and be
regular files; JSON is parsed with duplicate-key and non-finite rejection;
archive filenames derive from column indices, never from column names.

Run every invocation with `python3 -I`. Isolation matters here: if you `cd`
into a replica directory and run Python without `-I`, a planted `json.py`
there is imported instead of the standard library. `-I` (plus an explicit
`sys.path` entry for the package) removes the script directory and `cwd` from
the import path, so replica data can never shadow a module. The test suite
and demo already do this.
