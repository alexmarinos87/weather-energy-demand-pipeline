# Interval-policy code-change requests

## Purpose

G35a records an explicit human request to prepare a separate implementation branch and pull request for one G34 review whose decision is:

```text
accept_for_separate_code_change_pr
```

The request is repository-base-bound evidence. It does not create a branch, create a pull request, apply the reviewed patch, or activate the proposed monitoring thresholds.

## Evidence chain

```text
verified G33 implementation dry run
        ↓
accepted G34 named review
        ↓
G35a repository-bound code-change request
```

The request reopens and verifies the complete G34/G33/G32/G31/G29/G27/G26 evidence chain. It then binds the reviewed proposal to the exact repository commit and tree at request time.

## Retained fields

A request records:

- G34 review ID and SHA-256;
- G33 dry-run ID and SHA-256;
- G32 disposition ID and SHA-256;
- reviewed repository commit and tree;
- current repository commit and tree;
- policy source path, SHA-256, and Git blob SHA-1;
- active and proposed policy digests;
- revised candidate identity and version;
- changed threshold names, count, and digest;
- reviewed patch digest;
- intended paths and required validation commands;
- requested branch name and PR title;
- named requester, role, ticket, rationale, and UTC timestamp.

The current policy source must still match the source reviewed in G33. A source edit or blob change makes the request invalid and requires a new dry run and review.

## Status and authority

A valid request has:

```text
request_status=ready_for_named_code_change_request_review
next_action=named_code_change_request_review_required
```

It does not authorise implementation. All branch, PR, patch, source mutation, threshold activation, recalibration, model, schedule, alert, promotion, deployment, and publication fields remain `false`.

## CLI

```bash
python -m forecasting.run_interval_policy_code_change_request \
  --implementation-dry-run-review review.json \
  --implementation-dry-run proposal.json \
  --disposition disposition.json \
  --revision-sensitivity-summary revision_summary.parquet \
  --revision-sensitivity-manifest revision_manifest.json \
  --revision-package revision_package.json \
  --source-decision decision.json \
  --source-sensitivity-summary source_summary.parquet \
  --policy-source forecasting/interval_monitoring.py \
  --current-repository-commit <40-char-sha> \
  --current-repository-tree <40-char-tree-sha> \
  --requested-branch-name agent/apply-reviewed-interval-policy \
  --requested-pr-title "Apply reviewed interval monitoring defaults" \
  --requested-by "Named Requester" \
  --requester-role "Data Platform Owner" \
  --request-ticket GOV-135 \
  --rationale "Prepare a separately reviewed implementation PR from the retained evidence." \
  --output-dir data/interval-policy-code-change-requests
```

The command writes immutable JSON and Markdown evidence and refuses to overwrite an existing request.
