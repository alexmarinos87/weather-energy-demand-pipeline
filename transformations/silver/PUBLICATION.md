# Local silver partition publication

Both `save_clean_data` entry points delegate to `write_silver_partitions` in
`publication.py`. Existing `dt=YYYY-MM-DD` directories, second-resolution UTC
basenames, Parquet columns and wrapper `None` returns remain unchanged. Empty
frames remain a no-op. This helper publishes already constructed canonical rows;
it does not change transformation, deduplication or numeric-validation rules.

Every nonempty frame must have unique columns and non-null `event_date_utc` values
that are actual calendar dates written as exact `YYYY-MM-DD` strings. Validation
happens before filesystem changes or grouping, so null keys cannot silently drop
rows and malformed labels cannot become output paths. The helper does not infer
or correct dates from event timestamps; that mapping belongs to transformation.

All intended destinations are checked, including dangling symlinks. Every
partition is then exclusively serialized into a unique private staging directory
on the output filesystem, with non-Parquet `.part` filenames. Only after all
serialization succeeds are the completed files published through no-replace hard
links. Existing or competing final files are not replaced. Success messages follow
the completed publication call, not each intermediate write.

A same-second retry now raises `FileExistsError`, even when its payload is
identical, instead of overwriting existing evidence. There is no automatic retry,
renaming, deduplication, stale-file deletion or resume mode.

Serialization errors publish no final files. Caught publication failures and
interruptions attempt rollback of only this invocation's file identities, retaining
unrelated or replacement files. Cleanup continues after individual removal errors
and adds diagnostic notes to the original exception. Permissions or filesystem
availability may obstruct cleanup. Newly created empty directories can remain.

Hard-link support and a trusted, stable local output hierarchy are required.
Unsupported filesystems fail without an overwriting fallback. This is **not a
crash-atomic transaction across partitions**, a power-loss durability guarantee,
a concurrent-writer lock or protection against hostile directory mutation. A crash
may leave an incomplete set or private staging. There is no completion manifest;
consumers must not infer batch completeness from one partition file. Do not delete
unmanifested files automatically: this interface has no manifest by design.

This change applies only to the local Python savers. Fabric Delta publication,
raw capture writers, historical inputs and forecasting calculations are unchanged.
Combined integration must preserve independent cleaner input-failure and latest-tie
checks; this writer does not replace those responsibilities.

Regression command:

```bash
python -m pytest -q tests/test_silver_partition_publication.py
```

Tests invoke both actual savers with real transformed frames, including round
trips, first/later collisions, symlinks, serialization failure, competing writes,
interruptions before/after link creation, replacement preservation, and invalid
partition labels. Full constrained repository CI remains the acceptance gate.
