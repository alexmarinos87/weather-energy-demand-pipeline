# Observed raw capture publication

The observed-weather and energy `save_raw_data` entry points retain their
existing directories, second-resolution UTC filenames, JSON indentation and
`None` return behaviour. They now call `write_raw_json` rather than opening the
final path with overwrite mode. Fetching, provider contracts and metadata are
unchanged.

## Publication contract

An existing destination, including a dangling symlink, raises `FileExistsError`.
The writer creates unique same-filesystem private staging and serializes into
`payload.part`, not a `.json` filename that raw-file glob readers could mistake
for a completed capture. JSON serialization rejects `NaN` and infinities. Only
after serialization and file closure is the complete inode linked to the final
name without replacement. A competing destination causes failure, not overwrite.
Success output is printed only after the helper returns.

On ordinary failure or a caught interruption, staging is removed when filesystem
operations permit it. If publication created a final link, rollback checks its
file identity before removing it, so a replacement belonging to another writer
is not deliberately removed. Rollback errors are attached to the original error.
Filesystem permissions or availability can prevent cleanup; inspect the exception
and retained files rather than assuming every failure leaves an empty directory.

## Compatibility and operating limits

Two calls for the same dataset in the same second now collide explicitly rather
than replacing the first capture. This is not automatic deduplication: equal
payloads also raise on collision, and there is no overwrite or implicit retry
mode. Preserve existing evidence and arrange a separately identified capture;
do not delete a retained file just to make a retry succeed.

The implementation requires a trusted local output hierarchy and hard-link
support. Unsupported filesystems fail with no overwriting fallback. A process or
host crash can leave private staging or a completed file before the caller sees
success. No `fsync`-based power-loss durability, filesystem lock, hostile-directory
sandbox, multi-file transaction or source-authentication guarantee is provided.
Readers must ignore staging. This helper does not make files unmodifiable by
other processes with write permission.

This increment does not change the separate forecast raw/normalized writers,
Fabric notebook publication, schemas, models, CI or dependencies. Tests in
`tests/test_observed_raw_publication.py` invoke both observed savers without
provider calls and cover collisions, symlinks, partial serialization, non-finite
numbers, competing publication, caught interrupts and unchanged successful output.

Implementation references: [Python JSON serialization](https://docs.python.org/3.11/library/json.html)
and [filesystem links](https://docs.python.org/3.11/library/os.html#os.link).
