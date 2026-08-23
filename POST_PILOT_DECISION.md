# Post-pilot human decision

## Purpose

A Fabric pilot receipt and assessment are evidence, not a decision. This layer records one named human disposition against the exact approved candidate and exact immutable pilot evidence chain.

The allowed outcomes are:

```text
continue_evidence_collection
revise_candidate
retire_candidate
```

The decision recommends a next step but does not perform it.

## Required evidence

Decision recording verifies:

- the complete model-candidate registry history;
- that the latest candidate state is `approved`;
- the immutable Fabric pilot run receipt;
- the immutable Fabric pilot run assessment;
- exact candidate, pilot, receipt, assessment, external-run, and hash identity;
- decision time after both the candidate approval and pilot assessment; and
- a named decision maker, role, review ticket, rationale, and follow-up actions.

## Outcome rules

### Continue evidence collection

This outcome is available only when:

```text
assessment_outcome = eligible_for_post_pilot_review
run_status = completed
failed_check_count = 0
rollback_required = false
active_model_unchanged = true
```

It recommends creating a separately reviewed follow-up pilot plan. It does not authorize that plan or consume a credential.

### Revise candidate

This outcome requires one or more explicit revision requirements. It recommends registering a new candidate version with new evidence. It does not alter the current approved candidate or fabricate a replacement evidence chain.

### Retire candidate

This outcome requires a retirement reason. It recommends a separate immutable registry retirement event. It does not mutate the registry itself.

## Safety contract

Every decision records:

```text
human_decision_confirmed=true
automatic_decision_used=false
registry_mutation_performed=false
new_pilot_authorized=false
schedule_activation_allowed=false
deployment_authorized=false
model_activation_authorized=false
automatic_model_activation_allowed=false
active_model_unchanged=true
source_evidence_mutated=false
follow_up_human_action_required=true
```

## Record a decision

```bash
python3 -m forecasting.run_post_pilot_decision record \
  --candidate-dir data/model-registry/<candidate-id> \
  --receipt data/fabric-pilots/<pilot-id>/pilot_run_receipt_<authorization-id>.json \
  --assessment data/fabric-pilots/<pilot-id>/pilot_run_assessment_<receipt-id>.json \
  --decision continue_evidence_collection \
  --decided-by alexmarinos87 \
  --decision-role model-owner \
  --review-ticket PILOT-REVIEW-2026-001 \
  --reason "Pilot remained within the reviewed bounds" \
  --follow-up-action "Collect a second independent pilot window" \
  --confirm-assessment-id <assessment-id>
```

For a revision decision, include one or more:

```text
--revision-requirement "Reduce 60-minute humidity sensitivity"
```

For retirement, include:

```text
--retirement-reason "Candidate did not improve the protected test slices"
```

Default output:

```text
data/fabric-pilots/<pilot-id>/post_pilot_decision_<assessment-id>.json
```

The filename is assessment-bound and opened exclusively, so a second decision cannot silently replace the first.

## Verify a decision

```bash
python3 -m forecasting.run_post_pilot_decision verify \
  --decision data/fabric-pilots/<pilot-id>/post_pilot_decision_<assessment-id>.json
```

## Contract

- `data-contracts/fabric_post_pilot_decision_schema.json`

## Boundary

This ledger does not execute Fabric, create a schedule, deploy code, activate a model, or mutate the model registry. Every recommended next step remains a separate reviewed and explicitly invoked operation.
