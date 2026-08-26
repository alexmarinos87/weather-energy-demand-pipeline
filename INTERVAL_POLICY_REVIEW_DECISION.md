# Named interval-policy review decisions

## Purpose

G27 records one immutable, named human decision over one complete retained G26
policy-sensitivity summary. The decision layer does not rerun monitoring,
recalculate interval evidence, or activate a candidate policy.

Supported decisions are:

```text
retain_active_policy
reject_candidate
request_candidate_revision
```

## Input evidence

The command consumes one G26 summary file:

```text
interval_policy_sensitivity_summary_<sensitivity-run-id>.csv
```

or the equivalent Parquet output.

The input must contain one `sensitivity_run_id`, one `trend_run_id`, one
sensitivity timestamp, one complete scenario set for every candidate, and an
`active-reference` candidate that reproduces the retained canonical monitor
status. All G26 authority fields must remain false.

The complete validated summary is normalised, sorted deterministically, and
bound to the decision through:

```text
sensitivity_summary_sha256
```

Changing any retained scenario/candidate outcome, classification, slice count,
or authority flag invalidates the decision receipt.

## Decision rules

### Retain the active policy

```text
decision=retain_active_policy
target_candidate_id=active-reference
```

This records that the checked-in policy remains the active reference. It does
not modify a repository configuration or monitoring threshold.

### Reject a candidate

```text
decision=reject_candidate
target_candidate_role=review_candidate
```

The rejected candidate remains retained evidence. No thresholds are removed or
changed automatically.

### Request candidate revision

```text
decision=request_candidate_revision
target_candidate_role=review_candidate
```

At least one unique, meaningful requested change is required. The decision only
requests a later human-authored candidate revision; it does not create or
activate one.

## Named accountability

Every decision records:

```text
reviewer_name
reviewer_role
review_ticket
rationale
decision_timestamp_utc
```

The timestamp must be timezone-aware and cannot precede the retained G26
sensitivity run.

## Outputs

```text
interval_policy_review_decision_<decision-id>.json
interval_policy_review_decision_<decision-id>.md
```

The JSON document is self-hashed and also binds the complete sensitivity
summary. Existing files are never overwritten.

## Command

```bash
python3 -m forecasting.run_interval_policy_review_decision \
  --sensitivity-summary \
    data/interval-policy-sensitivity/interval_policy_sensitivity_summary_<run>.parquet \
  --decision request_candidate_revision \
  --target-candidate-id stricter-review \
  --reviewer-name "Alex Reviewer" \
  --reviewer-role "Data Platform Owner" \
  --review-ticket GOV-127 \
  --rationale "The candidate requires a narrower and better documented tolerance." \
  --requested-change "Provide a bounded revised candidate with documented threshold rationale." \
  --decision-timestamp-utc 2026-01-20T01:00:00Z \
  --output-dir data/interval-policy-decisions
```

## Authority boundary

Every receipt fixes the following to false:

```text
threshold_activation_authorized
active_policy_updated
candidate_thresholds_activated
retained_evidence_mutated
interval_recalibration_performed
model_change_performed
schedule_change_performed
promotion_change_performed
alert_delivery_performed
deployment_performed
external_publication_performed
```

A decision is review evidence only. Any later candidate revision or policy
implementation requires a separate reviewed change.
