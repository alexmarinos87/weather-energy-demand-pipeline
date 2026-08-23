# Controlled Fabric pilot plan

## Purpose

This layer prepares one tightly bounded **non-production** Fabric pilot without
connecting to Fabric, reading credentials, creating a schedule, or changing the
active model.

The plan is accepted only when it is bound to:

- a verified `approved` model-candidate history;
- a verified candidate evidence bundle;
- a verified clean recovery of that same bundle;
- the candidate repository, commit SHA, and Git tree SHA;
- one named non-production workspace, Lakehouse, and capacity;
- an explicit notebook and contract allowlist;
- credential **reference names**, never credential values;
- duration, capacity, input, output, retry, and quality-failure limits; and
- at least three rollback/abort steps.

## Safety boundary

Every plan records:

```text
pilot_state                 = draft
execution_mode              = manual
manual_execution_required   = true
automatic_execution_allowed = false
schedule_activation_allowed = false
execution_authorized        = false
execution_performed         = false
deployment_authorized       = false
active_model_before         = ridge_weather_lag
active_model_expected_after = ridge_weather_lag
active_model_unchanged      = true
credential_values_recorded  = false
source_evidence_mutated     = false
```

Creating or verifying a plan therefore cannot execute a notebook or authorize
the later human-operated run.

## Default notebook allowlist

```text
fabric/notebooks/01b_ingest_forecast_weather_to_bronze.py
fabric/notebooks/02b_forecast_weather_to_silver.py
fabric/notebooks/05c_target_weather_model_comparison.py
fabric/notebooks/06c_target_weather_comparison_quality_checks.py
```

The established observed-weather baseline remains the active control.

## Default limits

```text
duration                         <= 60 minutes
planned capacity                 <= 16 units
OpenWeather forecast rows        <= 40
comparison prediction rows       <= 100,000
comparison metric rows           <= 10,000
failed blocking quality checks   <= 0
automatic notebook retries       <= 0
```

The repository enforces absolute upper bounds as well. Raising a limit requires
an explicit reviewed plan change rather than an unbounded runtime argument.

## Create a plan

```bash
python3 -m forecasting.run_fabric_pilot plan \
  --candidate-dir data/model-registry/<candidate-id> \
  --evidence-bundle data/bundles/<candidate-id>/evidence_bundle_<bundle-id>.tar \
  --recovered-bundle-dir recovered/<bundle-id> \
  --environment non-production \
  --workspace-name weather-pilot \
  --lakehouse-name weather_energy_lakehouse \
  --capacity-name sandbox-capacity \
  --credential-reference OPENWEATHER_API_KEY \
  --actor alexmarinos87 \
  --review-ticket PILOT-2026-001 \
  --reason "Prepare a bounded non-production Fabric pilot"
```

Default output:

```text
data/fabric-pilots/<pilot-id>/pilot_plan_v001.json
```

The pilot ID is deterministic over the approved evidence, code identity,
environment, allowlists, limits, actor, ticket, reason, and planning timestamp.
The file is created exclusively and cannot overwrite an existing plan.

## Verify

```bash
python3 -m forecasting.run_fabric_pilot verify-plan \
  --plan data/fabric-pilots/<pilot-id>/pilot_plan_v001.json
```

Verification recomputes the plan hash and rechecks all non-execution,
non-production, credential, path, limit, and active-model constraints.

## Contract

- `data-contracts/fabric_pilot_plan_schema.json`

## Next dependency

The next layer must perform repository/environment preflight and create a
separate, time-bounded **human authorization** record. It must still contain no
Fabric API call or automatic execution path.
