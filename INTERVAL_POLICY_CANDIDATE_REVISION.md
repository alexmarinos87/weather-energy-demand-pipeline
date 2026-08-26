# Interval-policy candidate revision packages

This optional local workflow turns one verified G27 `request_candidate_revision`
decision into an immutable candidate revision package. It does not edit or
activate the checked-in monitoring policy.

## Evidence chain

```text
retained G26 sensitivity summary
        +
immutable G27 request-revision decision
        +
complete proposed monitoring configuration
        ↓
interval-policy-candidate-revision-v1 package
```

The decision is independently verified against the complete retained sensitivity
summary before a package is created. The package retains the decision and
sensitivity digests, source and revised candidate identities, requested changes,
full proposed policy, the checked-in active-policy snapshot, exact changed
thresholds, proposer identity, ticket, rationale, and evidence notes.

## Command

```bash
python3 -m forecasting.run_interval_policy_candidate_revision \
  --decision data/policy-decisions/interval_policy_review_decision_ipd-....json \
  --sensitivity-summary data/policy-sensitivity/interval_policy_sensitivity_summary_ips-....parquet \
  --proposed-policy proposed_policy.json \
  --revised-candidate-id stricter-review-r2 \
  --revised-candidate-version interval-monitoring-review-candidate-v2 \
  --proposed-by "Named engineer" \
  --proposer-role "Data platform engineer" \
  --revision-ticket "GOV-129" \
  --rationale "Address the reviewed calibration-history and width concerns." \
  --evidence-note "Retain the causal calibration minimum requested by the reviewer." \
  --output-dir data/policy-candidate-revisions
```

`proposed_policy.json` must contain every field accepted by the checked-in
`PredictionIntervalMonitoringConfig`. Unknown or missing fields, invalid values,
an unchanged active configuration, an altered source decision, duplicate notes,
or a package timestamp before the human decision fail closed.

Outputs are immutable:

```text
interval_policy_candidate_revision_<revision-id>.json
interval_policy_candidate_revision_<revision-id>.md
```

## Authority boundary

A package is a candidate revision for another human review. It does not authorize
threshold activation, update the active policy, mutate retained evidence,
recalibrate intervals, change a model or schedule, deliver an alert, promote a
candidate, deploy, or publish externally.
