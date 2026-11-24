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
python3 -I -m unittest discover -s tests -v   # 53 tests, synthetic fixtures
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
  pipeline.py orchestration, failure taxonomy, reporting
  cli.py      restore / verify / query / report
  synth.py    deterministic synthetic datasets for tests and the demo
tests/        unittest suite (format, repair, resources, randomised, CLI)
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
