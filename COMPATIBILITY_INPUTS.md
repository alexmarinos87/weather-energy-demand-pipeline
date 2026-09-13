# Compatibility command input fidelity

Both `forecasting.run_interval_policy_compatibility` and
`forecasting.run_interval_policy_retained_compatibility` use the local reader
`read_compatibility_frame` from `forecasting/interval_policy_compatibility_io.py`.
Command arguments and output formats have not changed.

## CSV is a text transport, not an identity-conversion step

CSV fields enter the existing domain validators as text. An identifier `0012`
remains `0012`, not `12`. Literal `NA`, `NULL` and `1e3` values in identity columns
remain those strings, not missing values or numbers. The domain contracts still
control their usual normalization, numeric bounds, boolean parsing and timestamp
requirements. Blank required identifiers and non-numeric check observations are
not made valid by preserving their original spelling.

CSV input must be UTF-8, with an optional byte-order mark. Headers must be unique
and non-empty; duplicates are rejected instead of renamed. Records must have the
same number of fields as the header. Standard CSV quoting, embedded delimiters,
quoted multiline values and empty physical lines are supported. No delimiter
sniffing, spreadsheet formula evaluation or locale-dependent numeric conversion
is performed. Input files are only read, never rewritten.

Parquet (`.parquet` or `.pq`) retains its explicit column types; its header is
checked by the same name validation. Producers must store identifiers as text:
a reader cannot restore leading zeros already discarded by a producer.

## Scope and limits

This reader is specific to the two compatibility commands. It does not change
the shared sensitivity reader or claim that unrelated commands have migrated.
The canonical comparison engines, policy values, source digest definition,
manifest schema and output publication code are unchanged.

Tables are materialized in memory, as expected by the existing assessment APIs;
this is not a streaming ingestion engine or a size-bounded sandbox. The caller
must retain stable local input files while reading. Parsing does not authenticate
the source and is not an atomic snapshot against concurrent file mutation.

Previously generated evidence is not rewritten. A separately identified rerun
from the original input is required to replace an assessment affected by earlier
CSV inference; retain the earlier evidence for review rather than relabelling it.

## Regression command

```bash
python -m pytest -q tests/test_interval_policy_csv_fidelity.py
```

These tests invoke both real commands, compare CSV/Parquet output and the canonical
source digest, and exercise exact identities, invalid values, header ambiguity,
UTF-8 BOM, quoting and source-file immutability. Full constrained CI is required
before integration.
