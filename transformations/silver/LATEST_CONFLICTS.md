# Local silver latest-capture conflict rule

Both local transformers call `select_latest_records` after constructing their
canonical columns. Existing business keys remain unchanged:

- Energy: source area, resource ID, source record ID and event timestamp.
- Weather: source area, city and event timestamp.

For each key, including legacy null-key groups, only rows at its maximum retained
ingestion timestamp are considered. A strictly newer capture still supersedes
older values; an old conflict does not block a later unambiguous capture.

When multiple latest rows remain, all canonical fields except `source_file` must
agree. Measurements, optional missing values, provider IDs and metadata are part
of that comparison. Conflicting latest rows raise `ValueError` identifying source
filenames, before a result is returned or the main command invokes its saver.
Neither lexical filename order nor record order can choose a conflicting value.

Matching canonical rows collapse to one row. The lexicographically greatest
source filename is retained for deterministic provenance only; this does not
claim it was captured later. Equality uses pandas duplicate semantics after the
existing type conversions, not original raw JSON-byte equality. Source files are
not modified, deleted, quarantined or repaired. The reported filename list is
bounded to ten names with a remaining count.

The helper handles local canonical DataFrames, not arbitrary unvalidated input.
It does not validate numeric ranges, authenticate ingestion timestamps, replace
the existing filename/mtime timestamp fallback, or prove complete source history.
The existing loaders' error policy is unchanged in this independent PR; the
separate #118 change stops batches on malformed/unreadable files. Combined
integration must retain both #118's input checks and this selector.

This increment is deliberately **local-only**. The canonical Fabric Spark notebook
is unchanged and does not yet implement this latest-tie rejection rule. No Spark
runtime parity or Fabric execution is claimed. A reviewed Fabric parity change is
required before relying on the same behavior there. Output publication and its
existing collision/partial-write limitations are also unchanged.

Run the local regression boundary:

```bash
python -m pytest -q tests/test_silver_latest_conflicts.py tests/test_silver_transforms.py
```

The tests invoke both actual local transforms and main entry points, using temporary
captures with controlled ingestion times. Full constrained repository CI is required
for acceptance; no live provider or credential access is part of this workflow.
