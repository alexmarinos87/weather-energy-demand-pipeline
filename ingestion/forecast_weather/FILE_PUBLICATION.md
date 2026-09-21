# Local forecast file publication

The raw and normalized savers publish one completed file at a time through
`publication.publish_new_file`. Private random `.part` staging files live in the
final directory. A hard link exposes the completed bytes without replacing an
existing filename; existing regular files and symlinks are rejected. There is no
check-then-replace race and no overwriting fallback on unsupported filesystems.
Caught write/publication failures attempt cleanup of this invocation's inode,
including interruption immediately after linking. Replacements owned by another
writer are not removed. Cleanup errors are attached to the original exception.

Before output mutation, snapshot IDs must be 64 lowercase hexadecimal characters
and retrieval timestamps must be nonmissing and timezone-aware. Paths are derived
from the UTC instant, not its textual offset. All normalized records must share
the same full snapshot ID and retrieval instant. No ID is recomputed or verified.
The other three normalized timestamp columns retain their existing conversion.

Raw JSON keeps the previous indentation and key sorting but now refuses nonfinite
JSON values (`allow_nan=False`). Serialization occurs before any output creation.
Parquet retains its existing columns and UTC timestamp conversion. Both savers
return their final path. The old predictable `.tmp.parquet` filename is no longer
used or cleaned automatically; unrelated legacy staging files remain. New files
inherit private staging permissions rather than a shared umask-derived mode;
operators needing cross-user access must arrange it explicitly.

The output hierarchy must be trusted and stable and support local hard links.
This is NOT an atomic transaction across raw and normalized outputs. If the second
saver fails, the first successful output remains. A process crash, power loss,
repeated interruption or inaccessible filesystem can leave staging or a completed
but unpaired output. There is no fsync durability, completion manifest, locking,
resume mode, automatic retry or stale-file deletion. Empty directories may remain.
Consumers must not infer pair completeness from one file. This helper is not a
sandbox for hostile directory changes and does not validate all forecast contents.

No historical evidence, fetch behavior, model, schema, source-area catalogue or
Fabric writer is changed. Full constrained CI is required before acceptance.
The repository regression tests are in `tests/test_forecast_file_publication.py`.

```bash
python -m pytest -q tests/test_forecast_file_publication.py
```
