# Interval-policy code-change request reviews

## Purpose

G35b records one immutable named human review of a verified G35a repository-bound code-change request.

```text
accepted G34 dry-run review
        ↓
G35a repository-bound request
        ↓
G35b named request review
```

The review never creates a branch, commit or pull request and never applies the reviewed policy patch.

## Decisions

```text
accept_for_separate_policy_defaults_pr
    → separate_policy_defaults_pr_required

reject_code_change_request
    → no_further_action_recorded

request_code_change_request_revision
    → revised_code_change_request_required
```

Acceptance means only that a separate policy-defaults PR may be prepared. It is not authorisation to create or merge that PR and it does not activate the proposed monitoring thresholds.

## Verified evidence

Before recording a decision, the implementation reopens and verifies the complete G35a request and its G34/G33/G32/G31/G29/G27/G26 evidence chain. It retains:

- the request, G34 review and G33 proposal identities and SHA-256 digests;
- the current repository commit and tree;
- the policy source path, SHA-256 and Git blob SHA-1;
- the active and proposed policy digests;
- revised-candidate identity and version;
- changed threshold names, count and digest;
- reviewed patch digest;
- intended paths and validation commands;
- requested branch name and PR title;
- reviewer name, role, ticket, rationale and UTC timestamp.

Any change to those bindings invalidates the receipt.

## Immutable outputs

```text
interval_policy_code_change_request_review_<review-id>.json
interval_policy_code_change_request_review_<review-id>.md
```

The contract version is:

```text
interval-policy-code-change-request-review-v1
```

Existing receipts are never overwritten.

## CLI

```bash
python -m forecasting.run_interval_policy_code_change_request_review \
  --code-change-request request.json \
  --implementation-dry-run-review dry_run_review.json \
  --implementation-dry-run dry_run.json \
  --disposition disposition.json \
  --revision-sensitivity-summary revision_summary.parquet \
  --revision-sensitivity-manifest revision_manifest.json \
  --revision-package revision_package.json \
  --source-decision decision.json \
  --source-sensitivity-summary source_summary.parquet \
  --policy-source forecasting/interval_monitoring.py \
  --current-repository-commit <40-character-sha> \
  --current-repository-tree <40-character-tree-sha> \
  --review-decision accept_for_separate_policy_defaults_pr \
  --reviewer-name "Named Reviewer" \
  --reviewer-role "Data Platform Owner" \
  --review-ticket GOV-136 \
  --rationale "Review rationale describing the evidence and retained boundary." \
  --output-dir data/interval-policy-code-change-request-reviews
```

## Authority boundary

Every review keeps branch, PR, implementation, patch, source mutation, threshold activation, policy update, interval recalibration, model, schedule, promotion, alert, deployment and publication fields false.
