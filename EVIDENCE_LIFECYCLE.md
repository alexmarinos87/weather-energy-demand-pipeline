# Evidence lifecycle inventory and dry-run plan

## Purpose

Forecast ingestion, reconciliation, model comparison, promotion assessment, provider monitoring, and candidate review all create durable evidence. This layer inventories that local evidence and recommends retention, compaction, or reversible quarantine without changing any source file.

The planner always emits:

```text
mutation_performed = false
```

No delete, move, rewrite, compaction, or storage-class operation exists in this increment.

## Policy

`data-contracts/evidence_retention_policy.json` defines ordered path patterns and category controls:

- retention period;
- minimum latest files to retain;
- optional age and small-file thresholds for compaction review;
- always-protected categories; and
- candidate states whose referenced evidence is protected.

Unclassified evidence is retained by default.

The default policy deliberately protects:

- all model-registry manifests and review events;
- promotion summaries; and
- provider-health summaries.

Evidence referenced by a `review_requested` or `approved` candidate is retained when its identifier appears in the evidence path. Candidate-package completeness is expanded further by the later evidence-bundle goal.

## Actions

Every inventoried file receives exactly one recommendation:

```text
retain
compact_candidate
quarantine_candidate
```

### Retain

A file is retained when any of these rules applies:

1. its category is always protected;
2. it is referenced by a protected model candidate;
3. it is among the latest `min_keep_latest` files in its category;
4. it is unclassified; or
5. it is still inside retention and compaction thresholds.

### Compact candidate

A CSV or Parquet file can be marked `compact_candidate` only when:

- the category explicitly supports compaction;
- the file is older than `compact_after_days`;
- its size is within the source-file bound; and
- at least `min_compaction_files` eligible files exist in the same category and parent path.

This is a recommendation only. Schema-compatible compaction and source verification are a separate increment.

### Quarantine candidate

A file can be marked `quarantine_candidate` only after:

- candidate protection and always-protect rules have been evaluated;
- the minimum latest-file floor has been applied; and
- the configured retention age has been exceeded.

The next mutation layer uses reversible quarantine rather than direct deletion.

## Inventory evidence

Each file records:

- relative path and parent path;
- category and classification status;
- size, suffix, modification time, and age;
- SHA-256 content digest;
- effective policy thresholds; and
- `evidence-inventory-v1`.

Symbolic links are rejected to prevent path escape or unexpected target mutation.

## Candidate protection

The planner verifies model-registry histories before extracting references from candidates in configured protected states. It collects:

- candidate ID;
- promotion assessment ID;
- comparison run ID;
- reconciliation run ID;
- provider-health monitor run ID;
- code commit SHA; and
- Git tree SHA.

A broken candidate hash chain blocks lifecycle planning rather than being ignored.

## Cost estimate

The CLI accepts an optional user-supplied monthly storage cost per GiB. The repository embeds no provider price. The estimate reports current inventoried cost and the portion associated with quarantine candidates.

## Run

```bash
python3 -m forecasting.run_evidence_lifecycle \
  --data-root data \
  --policy data-contracts/evidence_retention_policy.json \
  --as-of-utc 2026-08-22T16:00:00Z \
  --monthly-storage-cost-per-gib 0.00 \
  --output-dir data/lifecycle \
  --output-format parquet
```

Outputs:

```text
evidence_inventory_<plan-id>.parquet
evidence_lifecycle_plan_<plan-id>.parquet
evidence_lifecycle_summary_<plan-id>.json
```

`data/lifecycle/` and `data/quarantine/` are excluded from subsequent inventories to prevent lifecycle evidence from recursively planning itself.

## Contracts

- `data-contracts/evidence_inventory_schema.json`
- `data-contracts/evidence_lifecycle_plan_schema.json`
- `data-contracts/evidence_lifecycle_summary_schema.json`

## Boundary

The next increment may apply only `quarantine_candidate` rows through an explicit, hash-verifying, reversible move. Permanent deletion remains out of scope until quarantine expiry, independent verification, and a separate reviewed command exist.
