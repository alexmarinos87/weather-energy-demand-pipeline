# Interval-monitoring candidate revision packages

## Purpose

This layer records a human-authored response to one retained G27
`request_candidate_revision` decision. It turns the requested changes into a
complete, validated review-candidate configuration without changing the
checked-in monitoring policy.

The output is evidence for a later named review and a later, separately invoked
sensitivity comparison. It is not threshold activation.

## Required evidence chain

A package requires:

1. one valid `interval-policy-review-decision-v1` document whose decision is
   `request_candidate_revision`;
2. the complete retained `interval-policy-sensitivity-v1` summary bound by that
   decision;
3. the exact source review-candidate configuration identified by the decision;
4. one complete revised review-candidate configuration; and
5. a response for every requested change in the decision.

The source decision is reopened and verified against the full sensitivity
summary before package construction.

## Candidate contract

Both source and revised candidates contain the complete
`PredictionIntervalMonitoringConfig` field set plus:

```text
candidate_id
candidate_role=review_candidate
candidate_version
rationale
```

The revised candidate must:

- use a new candidate ID;
- use a new candidate version;
- preserve `prediction-interval-monitoring-policy-v1` as its source policy
  contract;
- pass the checked-in monitoring configuration validation;
- change at least one threshold; and
- remain distinct from `active-reference`.

## Requested-change coverage

Every G27 requested change must have one response containing:

```text
requested_change
response
changed_threshold_fields
```

The request strings must match the decision exactly and in order. Every changed
threshold must be covered by at least one response, and responses cannot claim a
threshold that did not change.

This verifies structural coverage of the human request. It does not claim that
free-text rationale is objectively sufficient; the named G30 reviewer remains
responsible for that judgement.

## Immutable evidence

The package retains:

- source decision ID and digest;
- sensitivity and trend run identities;
- sensitivity-summary digest;
- source and revised candidate snapshots and digests;
- checked-in active-policy snapshot and digest;
- exact threshold changes;
- requested-change responses;
- preparer name, role, ticket, rationale and UTC timestamp; and
- a package-level SHA-256 digest.

The writer produces:

```text
interval_policy_candidate_revision_<package-id>.json
interval_policy_candidate_revision_<package-id>.md
```

Existing evidence is never overwritten.

## CLI

The revision-plan JSON contains:

```json
{
  "source_candidate": {"candidate_id": "...", "...": "..."},
  "revised_candidate": {"candidate_id": "...", "...": "..."},
  "requested_change_responses": [
    {
      "requested_change": "Exact text from the G27 decision",
      "response": "How the revised candidate addresses it",
      "changed_threshold_fields": ["max_recent_coverage_shortfall_pct_points"]
    }
  ]
}
```

Run:

```bash
python3 -m forecasting.run_interval_policy_candidate_revision \
  --decision interval_policy_review_decision_ipd-....json \
  --sensitivity-summary interval_policy_sensitivity_summary_ips-....parquet \
  --revision-plan revision_plan.json \
  --prepared-by "Named engineer" \
  --preparer-role "Data platform owner" \
  --revision-ticket GOV-129 \
  --rationale "Bounded rationale for the proposed revision package." \
  --prepared-at-utc 2026-01-20T02:00:00Z \
  --output-dir data/interval-policy-candidate-revisions
```

## Authority boundary

A valid package has:

```text
compatibility_status=compatible_for_new_sensitivity_review
next_review_action=named_revision_package_review_required
automatic_sensitivity_rerun_allowed=false
```

It does not:

- activate candidate thresholds;
- update the active policy;
- mutate source decision or sensitivity evidence;
- recalibrate an interval;
- change a model or schedule;
- promote a candidate;
- deliver an alert;
- deploy; or
- publish externally.
