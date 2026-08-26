# Named interval-policy review decisions

## Purpose

This layer binds one complete retained `interval-policy-sensitivity-v1` summary to one immutable named human decision. It records review accountability without granting threshold-activation authority.

Supported outcomes are:

```text
retain_active_policy
reject_candidate
request_revision
```

## Evidence binding

The complete sensitivity summary is normalized, deterministically sorted, canonically serialized, and bound through `sensitivity_summary_sha256`. Verification can reopen the supplied summary and fail when any reviewed scenario or policy result changes.

Every decision retains the sensitivity run, trend run, active and target policy identities, named reviewer, reviewer role, review ticket, rationale, UTC decision time, and scenario-level status evidence.

## Decision rules

- `retain_active_policy` must target `active-reference` and cannot contain a revision request.
- `reject_candidate` must target a review candidate and cannot contain a revision request.
- `request_revision` must target a review candidate and include a meaningful requested change.

The decision timestamp cannot precede the sensitivity evidence.

## Local command

```bash
python3 -m forecasting.run_interval_policy_review_decision \
  --sensitivity-summary interval_policy_sensitivity_summary_ips-....parquet \
  --decision request_revision \
  --target-policy-id stricter-review \
  --reviewer-name "Named Reviewer" \
  --reviewer-role "Data Platform Owner" \
  --review-ticket "REV-123" \
  --rationale "The candidate is useful but needs a narrower freshness change." \
  --requested-revision "Retain the active freshness limit and revise only coverage drift." \
  --output-dir data/interval-policy-decisions
```

Outputs are immutable and decision-qualified:

```text
interval_policy_review_decision_<decision-id>.json
interval_policy_review_decision_<decision-id>.md
```

## Authority boundary

A decision receipt does not activate candidate thresholds, update the checked-in policy, mutate retained evidence, recalibrate intervals, change models or schedules, promote anything, deliver alerts, deploy, or publish externally. Any implementation remains a separate reviewed code change.
