# Named disposition over reviewed candidate sensitivity

## Goal

G32 records one immutable named human disposition over a complete retained G31
reviewed-candidate sensitivity result. It does not update or activate the
monitoring policy.

The evidence chain is:

```text
G25 retained interval-health trends
        ↓
G26 counterfactual policy sensitivity
        ↓
G27 named request/retain/reject decision
        ↓
G29 candidate revision package
        ↓
G30 named package review
        ↓
G31 revised-candidate sensitivity result
        ↓
G32 named disposition
```

## Supported dispositions

```text
retain_active_policy
reject_revised_candidate
request_another_revision
suitable_for_separate_implementation_proposal
```

`request_another_revision` requires one or more unique meaningful requested
actions. The other outcomes cannot carry requested actions.

A result marked `suitable_for_separate_implementation_proposal` only permits a
later human-authored proposal to be reviewed. It does not authorize a code
change, activate thresholds, or modify the checked-in policy.

## Source verification

The disposition layer requires:

- one complete G31 summary containing exactly `active-reference` and the reviewed
  revised candidate;
- one self-hashed G31 manifest;
- matching sensitivity-run, review, package, candidate, and artifact bindings;
- identical scenario coverage for both candidates;
- active-reference outcomes that reproduce retained canonical monitoring; and
- every G31 safety field fixed to `false`.

The disposition binds the normalized summary digest and the retained manifest
digest. Altering either source invalidates the receipt.

## Retained evidence

Each receipt records:

```text
disposition ID and SHA-256
source sensitivity run and timestamp
source trend run
source G31 summary and manifest digests
source review, package, and decision IDs
revised candidate identity, version, and digest
disposition and effect
target candidate identity
scenario-level retained and candidate outcomes
named reviewer, role, ticket, rationale, and UTC timestamp
requested actions, when revision is requested
```

## Credential-free command

```bash
python3 -m forecasting.run_interval_policy_revision_disposition \
  --revision-sensitivity-summary evidence/revision_sensitivity_summary.parquet \
  --revision-sensitivity-manifest evidence/revision_sensitivity_manifest.json \
  --disposition suitable_for_separate_implementation_proposal \
  --reviewer-name "Alex Reviewer" \
  --reviewer-role "Data Platform Owner" \
  --review-ticket "GOV-132" \
  --rationale "The retained comparison is suitable for a separate proposal review." \
  --disposed-at-utc "2026-08-27T22:58:00Z" \
  --output-dir evidence/revision-dispositions
```

The command writes immutable JSON and Markdown and refuses to overwrite an
existing receipt.

## Authority boundary

Every receipt keeps these actions false:

```text
implementation_authorized
implementation_applied
threshold_activation_authorized
active_policy_updated
candidate_thresholds_activated
source_manifest_mutated
source_summary_mutated
retained_evidence_mutated
interval_recalibration_performed
model_change_performed
schedule_change_performed
promotion_change_performed
alert_delivery_performed
deployment_performed
external_publication_performed
```

A disposition is review evidence only. Any implementation proposal, policy
change, threshold activation, recalibration, model operation, schedule change,
alert delivery, deployment, or publication remains a separate reviewed action.
