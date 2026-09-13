# Retained interval-policy compatibility review ledger

## Purpose

G40 creates one append-only ledger from independently verified G39 review
receipts. It provides chronological accountability across retained policy
compatibility assessments without turning those reviews into policy, annotation,
monitoring, Fabric, or deployment authority.

## Input manifest

The CLI accepts one explicit JSON binding manifest:

```json
{
  "bindings": [
    {
      "review": "reviews/review.json",
      "compatibility_summary": "compatibility/summary.parquet",
      "compatibility_manifest": "compatibility/manifest.json",
      "artifact_directory": "compatibility"
    }
  ]
}
```

Every binding is reopened and verified through the complete G39/G38 contract.
Directory discovery is not used.

## Ledger rules

The ledger:

- rejects duplicate review IDs;
- rejects more than one review for the same compatibility assessment;
- orders reviews by UTC review timestamp and review ID;
- assigns contiguous review sequence numbers;
- retains review, compatibility-run, trend-run, summary, and manifest digests;
- counts accepted transitions, annotation requests, and reassessment requests;
- records whether follow-up human action is required; and
- self-binds entries and summary with SHA-256.

A conflicting second decision is rejected rather than silently replacing or
superseding the first retained review.

## Outputs

```text
interval_policy_retained_compatibility_review_ledger_<run-id>.csv|parquet
interval_policy_retained_compatibility_review_ledger_summary_<run-id>.csv|parquet
interval_policy_retained_compatibility_review_ledger_<run-id>.md
interval_policy_retained_compatibility_review_ledger_<run-id>.json
```

Existing outputs are never overwritten.

Run locally:

```bash
python3 -m forecasting.run_interval_policy_retained_compatibility_review_ledger \
  --binding-manifest data/compatibility-review-bindings.json \
  --ledger-run-timestamp-utc 2026-09-02T10:00:00Z \
  --output-dir data/interval-policy-retained-compatibility-review-ledgers \
  --output-format parquet
```

## Contract

The summary schema is:

```text
data-contracts/interval_policy_retained_compatibility_review_ledger_summary_schema.json
```

The ledger contract version is:

```text
interval-policy-retained-compatibility-review-ledger-v1
```

## Authority boundary

Every entry, summary, and manifest keeps all mutation and operational authority
false. The ledger does not:

- rewrite a historical monitor status;
- apply a historical annotation;
- mutate a source review or compatibility assessment;
- rerun monitoring;
- update or activate a policy;
- recalibrate an interval;
- change a model or schedule;
- execute Fabric;
- deliver an alert;
- promote a candidate;
- deploy; or
- publish externally.
