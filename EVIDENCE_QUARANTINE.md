# Reversible evidence quarantine

## Purpose

The lifecycle planner can recommend `quarantine_candidate` files, but it cannot mutate evidence. This layer applies only those reviewed recommendations through a reversible, hash-verified move.

It never permanently deletes data.

## Preconditions

The apply command requires:

- a lifecycle plan file;
- the corresponding lifecycle summary JSON;
- an exact `--confirm-plan-id` value;
- a named actor and reason; and
- a local data root containing the planned source files.

Before moving anything, the command verifies all candidates together:

1. plan and summary IDs match;
2. the deterministic plan ID recomputes from policy identity, timestamp, paths, hashes, and actions;
3. only `quarantine_candidate` rows are selected;
4. no selected row is marked candidate-protected or always-protected;
5. every path is relative and resolves below the data root;
6. no source or destination path uses a symbolic link;
7. every source is a regular file;
8. current source size and SHA-256 match the lifecycle plan; and
9. every quarantine destination is absent.

A failure in any row prevents all moves.

## Quarantine layout

```text
data/quarantine/<plan-id>/files/<original-relative-path>
data/quarantine/<plan-id>/quarantine_manifest_<operation-id>.json
```

The original relative path is preserved beneath `files/`. Moves use same-filesystem atomic replacement. After every move, the destination hash and size are verified and the source must be absent.

If a later move or manifest write fails, completed moves are rolled back before the error is raised.

## Manifest

The immutable quarantine manifest records:

- plan and operation identity;
- actor, reason, and timestamp;
- source and destination relative paths;
- planned file category, size, and hash;
- original modification timestamp;
- total file and byte counts;
- `permanent_deletion_performed=false`;
- `restore_available=true`; and
- a SHA-256 manifest hash.

## Restore

Restore requires the manifest, exact operation confirmation, actor, and reason. It verifies the manifest hash and every quarantined file before moving anything.

Restore refuses to overwrite a recreated source path. Successful restore writes an immutable restore event and verifies that each source has the planned hash while each quarantine file is absent.

```text
data/quarantine/<plan-id>/restore_event_<operation-id>_<restore-id>.json
```

## Commands

Apply a reviewed plan:

```bash
python3 -m forecasting.run_evidence_quarantine apply \
  --data-root data \
  --plan data/lifecycle/evidence_lifecycle_plan_<plan-id>.parquet \
  --summary data/lifecycle/evidence_lifecycle_summary_<plan-id>.json \
  --confirm-plan-id <plan-id> \
  --actor alexmarinos87 \
  --reason "Move expired, unprotected evidence into reversible quarantine"
```

Verify current state:

```bash
python3 -m forecasting.run_evidence_quarantine verify \
  --data-root data \
  --manifest data/quarantine/<plan-id>/quarantine_manifest_<operation-id>.json
```

Restore:

```bash
python3 -m forecasting.run_evidence_quarantine restore \
  --data-root data \
  --manifest data/quarantine/<plan-id>/quarantine_manifest_<operation-id>.json \
  --confirm-operation-id <operation-id> \
  --actor alexmarinos87 \
  --reason "Restore evidence for review"
```

## Contracts

- `data-contracts/evidence_quarantine_manifest_schema.json`
- `data-contracts/evidence_restore_event_schema.json`

## Boundary

There is no purge command. Permanent deletion remains out of scope until quarantine age, independent bundle/recovery verification, explicit approval, and a separate reviewed implementation exist.
