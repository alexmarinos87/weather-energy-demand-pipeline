# Local silver batch failure boundary

`transform_energy_files` and `transform_weather_files` either return the complete
transformed selection or raise. Previously a failed raw file was printed and
skipped, allowing the caller to save an incomplete batch as an ordinary success.
There is no longer an implicit skip-and-continue mode.

## Input selection and errors

The input root must exist and be a directory. A missing root raises
`FileNotFoundError`; a file used as a root raises `NotADirectoryError`. Directory
stat/listing errors propagate. Enumeration selects names ending in lowercase
`.json` directly within that directory and sorts them as before. It does not
recurse into subdirectories or select non-JSON staging files.

A selected file that cannot be read, decoded, parsed or transformed raises a
`ValueError` naming the source file. Its `__cause__` retains the original exception
for diagnosis, including `PermissionError`, `UnicodeDecodeError` or
`JSONDecodeError`. Interrupts are not converted into successful results.
Both main entry points transform before saving, so an input failure prevents the
save step. The transforms do not modify or delete raw files.

An energy input must be a JSON object containing a `result` object and an explicit
`records` list. An absent or malformed envelope is not an empty capture. A valid
empty list and an existing empty directory still return an empty frame with the
same canonical column names.

## Preserved behavior and limits

Legacy raw files without area metadata remain supported with null source areas.
Modern provider schemas are not retroactively imposed on that history. Successful
column mappings, UTC parsing, filename/mtime ingestion-time convention, latest-
ingestion deduplication and output writers are unchanged. This correction does
not add record-level numeric/range checks, JSON duplicate-key rejection, complete
source identity validation or an explicit quarantine workflow.

Treat a raised error as a failed batch. Inspect the named input and its cause,
preserve the original evidence, and correct the input selection or permissions
through a separate reviewed action. Do not delete retained evidence merely to
make a rerun succeed. Previously published silver outputs are not rewritten by
this change; any replacement or corrected historical assessment is separate.

Enumeration and reading do not form a concurrent filesystem snapshot. Inputs
must remain stable during processing when reproducibility is required. The
unchanged silver writers are not transactional across date partitions and can
still encounter output collisions or write failures after transformation. This
input gate does not grant stronger output-publication guarantees or change the
separate Fabric implementation.

Regression coverage: `tests/test_silver_batch_failures.py`, alongside the
unchanged `tests/test_silver_transforms.py`. Tests exercise real transforms,
mixed valid/invalid files, file-read errors, missing/non-directory roots,
explicit empty captures, legacy input preservation and main saver suppression.
