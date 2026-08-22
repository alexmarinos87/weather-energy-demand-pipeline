# Reviewed model-candidate registry

## Purpose

The repository can now assess whether target-weather evidence is eligible for human review. This registry packages that evidence into an immutable candidate history and records explicit human decisions without deploying, activating, or scheduling a model.

The registered candidate is always:

```text
baseline model  = ridge_weather_lag
candidate model = ridge_target_weather
```

## State model

```text
registered       -> draft
draft            -> review_requested
review_requested -> approved | rejected
any non-retired  -> retired
```

`approved` means only that a named reviewer approved the candidate evidence for a controlled manual trial. It does not mean the candidate is active in Fabric or production.

Every manifest revision records:

```text
automatic_promotion_allowed = false
deployment_authorized       = false
active_model_unchanged       = true
```

No registry command contains a deployment or model-switch operation.

## Candidate package

Registration binds:

- promotion assessment ID and status;
- comparison and reconciliation run IDs;
- provider-health monitor ID and status;
- baseline and candidate model identities;
- repository, commit SHA, and Git tree SHA;
- candidate semantic version;
- training-data boundary;
- feature-contract versions;
- forecast-weather contract version; and
- registration actor, timestamp, and rationale.

The candidate ID is deterministic over this content. Registering the same evidence and code produces the same candidate ID.

## Immutable revisions and hash chain

Each state change writes two new files:

```text
manifest_vNNN.json
event_vNNN.json
```

Existing files are opened exclusively and are never overwritten. Events contain `previous_event_hash`; manifests contain `previous_manifest_hash` and the latest event hash. `verify` recomputes the full chain and rejects missing, reordered, altered, or inconsistent revisions.

## Review eligibility

A draft can enter `review_requested` only when:

```text
promotion assessment status = eligible_for_human_review
promotion failed checks      = 0
provider health status       = healthy
provider failed error checks = 0
automatic promotion flag     = false
automatic remediation flag   = false
```

A warning or failed provider-health result must be resolved with new evidence rather than silently overridden in the registry.

## Commands

Register a candidate:

```bash
python3 -m forecasting.run_model_registry register \
  --promotion-summary data/promotion/target_weather/target_weather_promotion_summary_<id>.parquet \
  --provider-health-summary data/monitoring/forecast_provider/forecast_provider_health_summary_<id>.parquet \
  --repository alexmarinos87/weather-energy-demand-pipeline \
  --code-commit-sha <40-character-commit-sha> \
  --code-tree-sha <40-character-tree-sha> \
  --candidate-version 0.1.0 \
  --training-data-boundary-utc 2026-08-22T12:00:00Z \
  --feature-contract-version time-horizon-v1 rolling-origin-v1 weather-model-comparison-v1 \
  --forecast-weather-contract-version target-weather-v1 \
  --actor alexmarinos87 \
  --reason "Package reviewed target-weather evidence" \
  --output-root data/model-registry
```

Request review:

```bash
python3 -m forecasting.run_model_registry request-review \
  --candidate-dir data/model-registry/<candidate-id> \
  --actor alexmarinos87 \
  --reason "Evidence package is complete for review"
```

Record a decision:

```bash
python3 -m forecasting.run_model_registry decide \
  --candidate-dir data/model-registry/<candidate-id> \
  --decision approved \
  --reviewer alexmarinos87 \
  --review-ticket REVIEW-2026-001 \
  --reason "Approved for a controlled manual Fabric trial"
```

Retire a candidate:

```bash
python3 -m forecasting.run_model_registry retire \
  --candidate-dir data/model-registry/<candidate-id> \
  --actor alexmarinos87 \
  --review-ticket REVIEW-2026-002 \
  --reason "Superseded by a newer evidence package"
```

Verify the immutable history:

```bash
python3 -m forecasting.run_model_registry verify \
  --candidate-dir data/model-registry/<candidate-id>
```

## Contracts

- `data-contracts/model_candidate_manifest_schema.json`
- `data-contracts/model_candidate_event_schema.json`

## Boundary

The registry is a local evidence and decision ledger. It does not call GitHub, Fabric, a model registry, a scheduler, or an external deployment system. A later integration may consume an approved package, but it must still require an explicit manual operation and a rollback plan.
