# Named review of retained interval-policy compatibility

## Purpose

G39 records one immutable named human decision over a complete G38 retained
policy-compatibility assessment.

The source assessment compares the previous five-point and reviewed three-point
recent coverage-shortfall limits over identical retained trend slices. G39 does
not reinterpret, rerun, or rewrite that evidence. It binds the exact G38 summary
and self-hashed manifest to an accountable human review.

## Required source evidence

A review requires:

```text
one complete G38 compatibility summary
one valid G38 self-hashed manifest
exact slices, summary, and report files referenced by the manifest
```

Verification reopens the manifest and hashes the exact artifact bytes. The
review also retains the normalized compatibility-summary SHA-256, source trend
run, old/current policy identities and thresholds, and scenario-level status
comparisons.

Any alteration of the retained summary, manifest, report, slices, or source
bindings invalidates the review.

## Decisions

### Accept non-retroactive transition

```text
accept_non_retroactive_transition
    -> non_retroactive_transition_accepted
```

This records that future monitoring may continue using the checked-in
three-point policy while historical monitoring rows remain unchanged. It does
not update a policy version, rewrite a status, or activate a schedule.

### Request historical annotation

```text
request_historical_annotation
    -> separate_historical_annotation_proposal_required
```

This requires at least one explicit requested action. It records a request for a
separate, reviewed annotation proposal. It does not apply an annotation.

### Request compatibility reassessment

```text
request_compatibility_reassessment
    -> new_compatibility_assessment_required
```

This also requires explicit requested actions. A new G38 assessment must be
created from new or corrected retained evidence; the existing assessment is not
mutated.

## Named accountability

Every review retains:

```text
reviewer name and role
review ticket
rationale
UTC review timestamp
source G38 run and trend-run identities
source summary and manifest digests
previous and current policy identities and thresholds
scenario-level retained, previous-policy, and current-policy statuses
changed and newly failed slice counts
requested actions, where applicable
```

The review timestamp must be timezone-aware and cannot precede the source G38
assessment.

## Immutable outputs

```text
interval_policy_retained_compatibility_review_<review-id>.json
interval_policy_retained_compatibility_review_<review-id>.md
```

The JSON contract is:

```text
interval-policy-retained-compatibility-review-v1
```

Existing outputs are never overwritten.

Run locally:

```bash
python3 -m forecasting.run_interval_policy_retained_compatibility_review \
  --compatibility-summary data/compatibility/summary.csv \
  --compatibility-manifest data/compatibility/manifest.json \
  --artifact-directory data/compatibility \
  --decision accept_non_retroactive_transition \
  --reviewer-name "Named Reviewer" \
  --reviewer-role "Data Platform Owner" \
  --review-ticket GOV-140 \
  --rationale "Accept the non-retroactive transition while retaining historical evidence." \
  --output-dir data/interval-policy-retained-compatibility-reviews
```

## Authority boundary

Every review fixes the following to `false`:

```text
historical_statuses_rewritten
historical_annotation_applied
retained_evidence_mutated
monitoring_rerun_performed
policy_version_updated
threshold_activation_performed
interval_recalibration_performed
model_change_performed
fabric_execution_performed
schedule_change_performed
promotion_change_performed
alert_delivery_performed
deployment_performed
external_publication_performed
```

An acceptance is human review evidence only. It does not rewrite historical
rows, apply annotations, execute local or Fabric monitoring, update a policy
version, activate thresholds or schedules, recalibrate intervals, alter models,
deliver alerts, deploy, or publish externally.
