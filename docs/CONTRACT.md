# Acceptance rules

This note states precisely when the tool may call a dataset *restored*, and
how every other outcome is labelled. The format itself is in
[`../FORMAT.md`](../FORMAT.md).

## Inputs and their trust level

| input | trust | on defect |
|-------|-------|-----------|
| handover manifest | authority, but validated | `bad_manifest`; run aborts, nothing is inferred |
| replica descriptor | untrusted | replica-level defect: replica skipped; page-level defect: page skipped |
| replica bytes | untrusted | verified per page/dictionary; failures skipped |
| dictionary bytes | untrusted | accepted only if length and sha256 match a descriptor |

A manifest defect — a gap, an overlap, a missing column, a wrong type, an
out-of-range slot, a duplicate slot id — is **not** missing data and is never
repaired from replicas. It aborts the run.

## Per-row verdict

For one column and one logical row `r`:

* collect every decoded value from every usable page covering `r`;
* **missing** if that set is empty;
* **disputed** if it contains two or more distinct values (distinctness is
  byte equality of the canonical encoding, not of a truncated rendering);
* **confirmed** otherwise, with the single agreed value.

No majority or "best replica" rule is applied. A row with conflicting
evidence blocks its slot; the tool does not search for a subset of replicas
that would agree.

## Per-slot verdict

For a manifest slot `[start, end)` of a column:

1. If any row is disputed → `disputed`. Missing rows, if any, are counted and
   listed too, but the status is `disputed`.
2. Else if any row is missing → `missing`, with the exact missing ranges.
3. Else every row is confirmed. Feed them in order into the slot hash:
   * hash equals the manifest digest → `confirmed`;
   * otherwise → `digest_mismatch` (rows present and agreed, hash wrong).

`missing`, `disputed` and `digest_mismatch` are three different findings and
are never merged into one another.

## When an archive is written

Only when **every slot of every column** is `confirmed`. Then:

* the archive is written to a staging directory in the scratch area;
* it is self-verified from its own bytes (stream lengths, stream sha256,
  canonical decoding, value types, every slot digest);
* only on successful verification is it published into `--out`;
* the run exits `0`.

If any slot is not confirmed, no archive is written; the run writes
`diagnostics.json` with per-slot findings and exits `3`.

If the archive is written but self-verification fails, it is discarded, the
run exits `4`, and the failure is recorded.

## The archive

The archive is the deliverable:

* `columns/<c>.canon` — exactly the canonical encoding, one value per row,
  original row order, verifiable against slot digests without any replica;
* `columns/<c>.idx` — block offsets for O(1)-block random access by row;
* `checks/index.json` — for each slot, the evidence sub-intervals and the
  replica/page/dictionary each came from;
* `archive.json` — status, row count, per-column stream hashes and the
  manifest's slot digests;
* `diagnostics.json` — the full run report.

An overlapping replica page contributes evidence, never extra rows: the
archive has exactly one value per column per original row number.

## Reproducing

The synthetic fixtures under `tests/` and `examples/demo.py` build datasets,
split them across replicas at deliberately mismatched boundaries, share
dictionaries across replicas, damage pages and dictionaries in each failure
mode, and assert the labels above. Run:

```sh
python3 -I -m unittest discover -s tests -v
```
