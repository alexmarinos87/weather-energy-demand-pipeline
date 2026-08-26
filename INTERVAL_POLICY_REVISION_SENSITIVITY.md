# Reviewed candidate-revision sensitivity comparison

## Purpose

This layer performs one explicitly requested, evidence-only comparison between:

```text
active-reference
revised review candidate from an accepted G29 package
```

It reuses the canonical G26 `evaluate_policy_sensitivity` implementation. It
does not create a second monitoring-policy evaluator.

## Required chain

The run requires:

1. retained `interval-health-trend-v1` slice evidence;
2. one G30 `accept_for_sensitivity_review` receipt;
3. the exact G29 revision package bound by that receipt;
4. the exact G27 source decision; and
5. the complete G26 sensitivity summary used by the G27 decision.

Every source document is reopened and verified before evaluation. The retained
trend-run ID must match the reviewed evidence chain.

## Candidate scope

The comparison contains exactly:

```text
active-reference
accepted revised candidate
```

The active reference must reproduce the retained canonical monitor status for
every scenario. The revised candidate uses the package's complete validated
threshold configuration.

Retained G25 slice trends contain metrics calculated under the checked-in recent
and reference window geometry. Therefore this layer rejects packages that change:

```text
recent_interval_run_count
reference_interval_run_count
```

Those changes require a new trend-building contract from retained interval-run
history rather than reinterpretation of already aggregated slices.

## Outputs

```text
interval_policy_revision_sensitivity_slices_<run-id>
interval_policy_revision_sensitivity_summary_<run-id>
interval_policy_revision_sensitivity_report_<run-id>.md
interval_policy_revision_sensitivity_manifest_<run-id>.json
```

CSV and Parquet are supported for tabular outputs.

The slices and summary retain:

- G30 review ID and digest;
- G29 package ID and digest;
- G27 decision ID and digest;
- revised candidate ID, version and digest;
- the inherited G26 run, scenario, exact-slice and policy outcomes; and
- `interval-policy-revision-sensitivity-v1`.

The manifest binds all three output artifacts through byte-level SHA-256 hashes
and row counts.

## CLI

```bash
python3 -m forecasting.run_interval_policy_revision_sensitivity \
  --slice-trends interval_health_slice_trends_iht-....parquet \
  --revision-review interval_policy_candidate_revision_review_irv-....json \
  --revision-package interval_policy_candidate_revision_ipr-....json \
  --source-decision interval_policy_review_decision_ipd-....json \
  --source-sensitivity-summary interval_policy_sensitivity_summary_ips-....parquet \
  --sensitivity-run-id ips-222222222222222222222222 \
  --sensitivity-run-timestamp 2026-01-20T04:00:00Z \
  --output-format parquet \
  --output-dir data/interval-policy-revision-sensitivity
```

The run timestamp cannot precede the G30 review.

## Interpretation

The output answers whether the revised candidate changes retained healthy,
warning, or failed outcomes relative to the active reference. It is
counterfactual retrospective evidence only.

It does not:

- update or activate the monitoring policy;
- automatically rerun itself;
- mutate review, package, decision, trend, or sensitivity evidence;
- recalibrate an interval;
- change a model or schedule;
- promote a candidate;
- deliver an alert;
- deploy; or
- publish externally.
