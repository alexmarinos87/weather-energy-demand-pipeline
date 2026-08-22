# Verified evidence compaction staging

## Purpose

The lifecycle planner can identify groups of small CSV or Parquet evidence files. This layer creates and verifies a staged Parquet output while leaving every source file in place.

It does not replace, quarantine, or delete sources.

```text
source_files_mutated             = false
source_files_permanently_deleted = false
replacement_authorized           = false
```

## Preconditions

The stage command requires:

- the lifecycle plan and matching summary;
- an exact `--confirm-plan-id`;
- a named actor and reason; and
- at least the planned `min_compaction_files` in each category/parent/format group.

Before writing anything, all groups are preflighted:

1. the deterministic lifecycle plan ID is recomputed;
2. only `compact_candidate` rows are selected;
3. candidate-protected and always-protected rows are rejected;
4. every source path is relative, below the data root, and free of symbolic links;
5. current source sizes and SHA-256 hashes match the plan;
6. every source is CSV or Parquet and within its planned byte bound;
7. all files in a group have the same column order and pandas dtypes; and
8. output and manifest paths do not already exist.

## Deterministic row contract

Sources are read in ascending relative-path order. Rows retain their original order within each source, then are concatenated with a new continuous index. No provenance columns are injected into the compacted dataset; source lineage remains in the manifest.

The staged Parquet is read back and must match:

- source schema;
- total source row count; and
- a deterministic dataframe content fingerprint.

The output file also receives its own size and SHA-256 identity.

## Output

```text
data/compacted/<plan-id>/<category>/<group-id>/
  compacted_<operation-id>.parquet
  compaction_manifest_<operation-id>.json
```

The manifest records source order, paths, hashes, sizes, row counts and modification timestamps, plus output identity, schema and content verification.

If any output or manifest fails, all artifacts created by the current staging call are removed. Source evidence is never part of that rollback because it was never moved.

## Commands

Stage all compaction groups in one reviewed plan:

```bash
python3 -m forecasting.run_evidence_compaction stage \
  --data-root data \
  --plan data/lifecycle/evidence_lifecycle_plan_<plan-id>.parquet \
  --summary data/lifecycle/evidence_lifecycle_summary_<plan-id>.json \
  --confirm-plan-id <plan-id> \
  --actor alexmarinos87 \
  --reason "Stage verified compacted evidence"
```

Verify a staged group later:

```bash
python3 -m forecasting.run_evidence_compaction verify \
  --data-root data \
  --manifest data/compacted/<plan-id>/<category>/<group-id>/compaction_manifest_<operation-id>.json
```

Verification checks that every source still exists with the planned hash and schema and that the compacted output remains exactly equivalent.

## Contract

- `data-contracts/evidence_compaction_manifest_schema.json`

## Boundary

A verified compacted output is not yet authoritative. Replacing source files requires a later explicit workflow that binds the compaction manifest to a reviewed quarantine operation and proves recovery. Permanent deletion remains unavailable.
