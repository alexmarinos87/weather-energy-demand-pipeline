# Post-pilot follow-through request

## Purpose

This layer turns one verified human post-pilot decision and one verified/recovered closure into a bounded work request. It does not execute the request.

The request type is derived from the recorded decision and cannot be selected independently:

```text
continue_evidence_collection -> prepare_new_pilot_cycle
revise_candidate             -> prepare_candidate_revision
retire_candidate             -> prepare_registry_retirement_review
```

## Required evidence

Creation re-verifies:

- the immutable post-pilot decision;
- the deterministic closure archive;
- the clean recovered closure directory; and
- exact decision/closure identity and hash binding.

The closure must contain the same decision ID/hash, candidate identity/version, pilot ID, and external run identity as the supplied decision record.

## Follow-through boundaries

### Prepare a new pilot cycle

This request requires:

```text
new_pilot_plan_required=true
new_preflight_required=true
new_authorization_required=true
new_evidence_cycle_required=true
```

The consumed authorization remains unusable and no pilot is authorized to run.

### Prepare a candidate revision

This request requires:

```text
new_candidate_version_required=true
new_evidence_cycle_required=true
```

It does not create or register the new candidate version. The existing candidate-registration and evidence workflows remain separate.

### Prepare a registry retirement review

This request requires:

```text
registry_retirement_review_required=true
```

It does not mutate the registry or retire the active model. Any retirement transition must use the separately reviewed registry workflow.

## Human request fields

Every request requires:

- exact decision and closure confirmations;
- a named human requester;
- a non-empty owner;
- a review ticket;
- a reason of at least 20 characters; and
- at least one unique action item.

## Safety boundary

Every record hard-codes:

```text
automatic_execution_allowed=false
model_registry_mutation_allowed=false
pilot_execution_authorized=false
authorization_reuse_allowed=false
schedule_activation_allowed=false
deployment_authorized=false
model_activation_authorized=false
active_model_before=ridge_weather_lag
active_model_expected_after=ridge_weather_lag
active_model_unchanged=true
closure_mutation_allowed=false
source_evidence_mutated=false
permanent_deletion_allowed=false
```

## Create

```bash
python3 -m forecasting.run_post_pilot_followthrough create \
  --decision-record data/fabric-pilots/<pilot-id>/post_pilot_decision_<id>.json \
  --closure-bundle data/pilot-closures/<pilot-id>/post_pilot_closure_<id>.tar \
  --recovered-directory recovered/pilot-closures/<closure-id> \
  --confirm-decision-id <decision-id> \
  --confirm-closure-id <closure-id> \
  --requested-by "Named human requester" \
  --owner "Data Engineering" \
  --review-ticket PILOT-FOLLOWTHROUGH-001 \
  --reason "Prepare the separately reviewed next step from the recorded decision" \
  --action-item "Create the required new evidence or registry workflow artifact"
```

Default output:

```text
data/pilot-followthrough/<pilot-id>/post_pilot_followthrough_<request-id>.json
```

The file is created exclusively and cannot overwrite an existing request.

## Verify

```bash
python3 -m forecasting.run_post_pilot_followthrough verify \
  --request-record data/pilot-followthrough/<pilot-id>/post_pilot_followthrough_<request-id>.json
```

## Contract

- `data-contracts/post_pilot_followthrough_request_schema.json`

## Next dependency

The next layer should route each request into the already separated workflow that can prepare—but still not automatically execute—the relevant artifact:

- a new pilot plan;
- a new candidate revision specification; or
- a reviewed candidate-registry retirement transition.
