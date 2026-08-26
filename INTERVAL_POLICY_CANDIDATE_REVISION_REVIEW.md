# Named review of interval-policy candidate revisions

## Purpose

This layer records one named human judgement over a verified G29 candidate
revision package.

It supports:

```text
accept_for_sensitivity_review
reject_revision_package
request_package_revision
```

The review reopens the complete evidence chain:

```text
G26 retained sensitivity summary
        ↓
G27 request-candidate-revision decision
        ↓
G29 candidate revision package
        ↓
G30 named revision-package review
```

## Decision semantics

### Accept for sensitivity review

Acceptance means the package is eligible to be supplied to a separately
requested G31 sensitivity comparison. It does not run that comparison and does
not activate any candidate thresholds.

The retained next action is:

```text
separate_sensitivity_review_request_required
```

### Reject revision package

Rejection records that the package should not progress. It performs no policy or
evidence mutation.

### Request package revision

A revision request must contain at least one unique, meaningful requested
change. It requires a newly authored G29 package; the reviewed package is never
edited in place.

## Retained evidence

The immutable review retains:

- revision-package identity, contract and SHA-256 digest;
- source G27 decision identity and digest;
- G26 sensitivity and trend identities and summary digest;
- source and revised candidate identities, versions and digests;
- the exact threshold changes and requested-change responses;
- the source decision's scenario evidence;
- named reviewer, role, ticket, rationale and UTC review timestamp; and
- a review-level SHA-256 digest.

The review timestamp cannot precede the package preparation time.

## Immutable outputs

```text
interval_policy_candidate_revision_review_<review-id>.json
interval_policy_candidate_revision_review_<review-id>.md
```

Existing evidence is never overwritten.

## CLI

```bash
python3 -m forecasting.run_interval_policy_candidate_revision_review \
  --revision-package interval_policy_candidate_revision_ipr-....json \
  --source-decision interval_policy_review_decision_ipd-....json \
  --sensitivity-summary interval_policy_sensitivity_summary_ips-....parquet \
  --review-decision accept_for_sensitivity_review \
  --reviewer-name "Named reviewer" \
  --reviewer-role "Data platform owner" \
  --review-ticket GOV-130 \
  --rationale "The package is complete enough for a separate sensitivity review." \
  --reviewed-at-utc 2026-01-20T03:00:00Z \
  --output-dir data/interval-policy-candidate-revision-reviews
```

For `request_package_revision`, repeat `--requested-change` for each requested
change.

## Authority boundary

Every review fixes these actions to false:

```text
sensitivity_review_executed
automatic_sensitivity_review_allowed
sensitivity_review_execution_authorized
threshold_activation_authorized
candidate_thresholds_activated
active_policy_updated
source_package_mutated
source_decision_mutated
source_sensitivity_evidence_mutated
interval_recalibration_performed
model_change_performed
schedule_change_performed
promotion_change_performed
alert_delivery_performed
deployment_performed
external_publication_performed
```

A review decision is retained evidence only. G31 must remain a separate,
explicitly invoked evaluation layer.
