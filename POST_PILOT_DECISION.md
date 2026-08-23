# Post-pilot human decision

## Purpose

This layer records one named human disposition after a controlled Fabric pilot has been assessed. It does not connect to Fabric, mutate the model registry, reuse the consumed authorization, create a schedule, deploy code, or activate a model.

The accepted decisions are:

```text
continue_evidence_collection
revise_candidate
retire_candidate
```

The evidence chain is re-verified before a decision is recorded:

```text
verified pilot plan
    -> verified single-use authorization
    -> verified immutable run receipt
    -> verified post-run assessment
    -> named human decision
```

## Decision rules

`continue_evidence_collection` is allowed only when the run assessment is `eligible_for_post_pilot_review`. It requests a completely new pilot cycle. The previous authorization cannot be reused.

`revise_candidate` is allowed after either a successful or failed pilot. It records that a new candidate revision and a new evidence cycle are required.

`retire_candidate` is allowed after either outcome. It records a retirement request, but does not mutate the candidate registry. A separate reviewed registry transition remains necessary.

Every decision requires:

- exact pilot, receipt, and assessment confirmation IDs;
- a named human decision maker rather than an automation identity;
- a review ticket;
- a reason of at least 20 characters; and
- at least one unique action item.

## Safety boundary

Every record hard-codes:

```text
human_decision_recorded=true
automatic_decision_allowed=false
model_registry_mutation_allowed=false
authorization_reuse_allowed=false
pilot_reexecution_authorized=false
schedule_activation_allowed=false
deployment_authorized=false
model_activation_authorized=false
active_model_before=ridge_weather_lag
active_model_expected_after=ridge_weather_lag
active_model_unchanged=true
source_evidence_mutated=false
```

A decision therefore records human intent but cannot implement that intent automatically.

## Record a decision

```bash
python3 -m forecasting.run_post_pilot_decision decide \
  --plan data/fabric-pilots/<pilot-id>/pilot_plan_v001.json \
  --authorization data/fabric-pilots/<pilot-id>/pilot_authorization_<id>.json \
  --receipt data/fabric-pilots/<pilot-id>/pilot_run_receipt_<id>.json \
  --assessment data/fabric-pilots/<pilot-id>/pilot_run_assessment_<id>.json \
  --confirm-pilot-id <pilot-id> \
  --confirm-receipt-id <receipt-id> \
  --confirm-assessment-id <assessment-id> \
  --decision continue_evidence_collection \
  --decision-maker "Named human reviewer" \
  --review-ticket PILOT-DECISION-001 \
  --reason "Continue evidence collection under a newly reviewed pilot cycle" \
  --action-item "Prepare a new pilot plan against the latest approved evidence" \
  --action-item "Repeat repository and environment preflight"
```

Default output:

```text
data/fabric-pilots/<pilot-id>/post_pilot_decision_<decision-id>.json
```

The file is created exclusively and cannot overwrite an existing decision.

## Verify

```bash
python3 -m forecasting.run_post_pilot_decision verify \
  --decision-record data/fabric-pilots/<pilot-id>/post_pilot_decision_<decision-id>.json
```

## Contract

- `data-contracts/post_pilot_decision_schema.json`

## Next dependency

The next layer should create a deterministic post-pilot closure bundle containing the complete pilot chain, the human decision, and the referenced run evidence. It must support hash verification and clean recovery without changing the active model or candidate registry.
