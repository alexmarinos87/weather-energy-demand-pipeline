# Interval-policy decision ledger

## Purpose

G28 collects independently verified G27 review decisions into one deterministic,
append-only ledger. It detects duplicate decision IDs and rejects more than one
decision for the same sensitivity run and target candidate.

The ledger is evidence management only. It does not activate candidate
thresholds, update the checked-in monitoring policy, recalibrate intervals,
change models or schedules, deliver alerts, deploy, or publish externally.

## Input binding

The command reads an explicit JSON manifest:

```json
{
  "bindings": [
    {
      "decision": "decisions/interval_policy_review_decision_ipd-....json",
      "sensitivity_summary": "sensitivity/interval_policy_sensitivity_summary_ips-....parquet"
    }
  ]
}
```

Every decision is reopened and verified against its complete retained G26
sensitivity summary before it can enter the ledger. A modified decision or
modified source summary is rejected.

## Conflict contract

The first ledger version deliberately fails closed when two decisions target the
same:

```text
sensitivity_run_id / target_candidate_id
```

A future supersession workflow must be introduced through a separate reviewed
contract rather than silently replacing earlier human evidence.

## Outputs

```text
interval_policy_decision_ledger_<ledger-run-id>
interval_policy_decision_ledger_summary_<ledger-run-id>
interval_policy_decision_ledger_<ledger-run-id>.md
interval_policy_decision_ledger_<ledger-run-id>.json
```

The manifest retains a SHA-256 digest over the verified entries and summary. CSV
and Parquet outputs are supported, and existing files are not overwritten.

## Command

```bash
python3 -m forecasting.run_interval_policy_decision_ledger \
  --bindings data/policy-decisions/bindings.json \
  --ledger-run-id ipl-222222222222222222222222 \
  --ledger-run-timestamp-utc 2026-01-20T02:00:00Z \
  --output-dir data/policy-decisions/ledger \
  --output-format parquet
```

## Authority boundary

Every ledger entry, summary, and manifest keeps the following false:

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

The ledger can show that human follow-up remains required, but it cannot perform
that follow-up or implement a policy change.
