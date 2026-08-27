# Interval-policy implementation dry-run review

## Purpose

G34 records one immutable named human review over a verified G33
repository-base-bound implementation dry run.

It separates review of an exact patch proposal from creation or application of a
code change.

## Supported decisions

```text
accept_for_separate_code_change_pr
reject_implementation_dry_run
request_dry_run_revision
```

Acceptance means that a separate code-change PR should be prepared. It does not
authorize, create, merge, or apply that PR.

A revision request must contain one or more unique requested updates. Acceptance
and rejection cannot carry revision instructions.

## Verification

Before retaining a review, G34 reopens and verifies the complete G33/G32/G29
chain against:

- the exact repository base commit;
- the exact repository base tree;
- the policy source SHA-256 and Git blob SHA-1;
- the active and proposed policy digests;
- the exact changed-threshold list;
- the intended source and test paths; and
- the validation commands.

A changed repository base or source file makes the review fail closed.

## Immutable outputs

```text
interval_policy_implementation_dry_run_review_<review-id>.json
interval_policy_implementation_dry_run_review_<review-id>.md
```

Existing outputs are never overwritten.

## Credential-free example

```bash
python3 -m forecasting.run_interval_policy_implementation_dry_run_review \
  --implementation-dry-run evidence/g33/proposal.json \
  --disposition evidence/g32/disposition.json \
  --revision-sensitivity-summary evidence/g31/summary.parquet \
  --revision-sensitivity-manifest evidence/g31/manifest.json \
  --revision-package evidence/g29/package.json \
  --source-decision evidence/g27/decision.json \
  --source-sensitivity-summary evidence/g26/summary.parquet \
  --policy-source forecasting/interval_monitoring.py \
  --repository-base-commit "$BASE_COMMIT" \
  --repository-base-tree "$BASE_TREE" \
  --review-decision accept_for_separate_code_change_pr \
  --reviewer-name "Named reviewer" \
  --reviewer-role "Data platform owner" \
  --review-ticket "GOV-134" \
  --rationale "The exact base-bound diff and validation plan were reviewed." \
  --output-dir data/interval-policy-implementation-dry-run-reviews
```

## Authority boundary

Every review fixes code-change authorization and creation, implementation,
source mutation, threshold activation, active-policy update, recalibration,
model, schedule, promotion, alert, deployment, and publication fields to
`false`.

The accepted review produces only:

```text
separate_code_change_pr_required
```

A later PR must still implement and independently validate the policy change.
