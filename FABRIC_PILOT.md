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

# Repository/environment preflight and human authorization

## Separation of duties

Pilot planning, preflight, authorization, and execution are separate evidence
steps:

```text
draft plan
    -> repository/environment preflight
    -> time-bounded human authorization
    -> external human-operated pilot
    -> immutable run receipt and post-run assessment
```

A successful preflight does not authorize execution. It emits either:

```text
blocked
eligible_for_human_authorization
```

and always records:

```text
execution_authorized=false
execution_performed=false
automatic_authorization_allowed=false
```

## Environment snapshot

Preflight consumes an operator-exported JSON object containing only environment
facts, never secret values:

```json
{
  "snapshot_id": "env-2026-001",
  "captured_at_utc": "2026-08-23T12:00:00Z",
  "environment": "non-production",
  "workspace_name": "weather-pilot",
  "lakehouse_name": "weather_energy_lakehouse",
  "capacity_name": "sandbox-capacity",
  "workspace_exists": true,
  "lakehouse_exists": true,
  "capacity_available": true,
  "available_capacity_units": 32,
  "current_capacity_utilization_pct": 20,
  "active_job_count": 0,
  "pilot_schedule_count": 0,
  "current_active_model": "ridge_weather_lag",
  "credential_values_included": false
}
```

The preflight re-verifies the approved candidate, evidence bundle, and recovered
bundle. It also verifies the exact candidate commit/tree, hashes every planned
notebook and contract file, checks plan and snapshot freshness, confirms there
are no pilot schedules or conflicting active jobs, verifies capacity headroom,
confirms the control model remains active, and checks that planned credential
reference names are available without reading values.

## Run preflight

```bash
python3 -m forecasting.run_fabric_pilot_authorization preflight \
  --plan data/fabric-pilots/<pilot-id>/pilot_plan_v001.json \
  --candidate-dir data/model-registry/<candidate-id> \
  --evidence-bundle data/bundles/<candidate-id>/evidence_bundle_<bundle-id>.tar \
  --recovered-bundle-dir recovered/<bundle-id> \
  --repository-root . \
  --provider-health-summary data/monitoring/forecast_provider/forecast_provider_health_summary_<id>.parquet \
  --environment-snapshot environment_snapshot.json \
  --current-code-commit-sha <candidate-commit-sha> \
  --current-code-tree-sha <candidate-tree-sha> \
  --available-credential-reference OPENWEATHER_API_KEY \
  --as-of-utc 2026-08-23T12:00:00Z \
  --require-eligible
```

Output:

```text
data/fabric-pilots/<pilot-id>/pilot_preflight_<preflight-id>.json
```

## Human authorization

Only an eligible, zero-failure preflight can be authorized. Authorization:

- requires exact pilot and preflight confirmation IDs;
- rejects automation identities;
- records named authorizer and operator;
- is single-use and revocable;
- has a bounded start/end window;
- must cover the planned duration plus a 15-minute safety margin;
- expires if not used; and
- authorizes only manual pilot execution, never deployment or model activation.

```bash
python3 -m forecasting.run_fabric_pilot_authorization authorize \
  --plan data/fabric-pilots/<pilot-id>/pilot_plan_v001.json \
  --preflight data/fabric-pilots/<pilot-id>/pilot_preflight_<preflight-id>.json \
  --confirm-pilot-id <pilot-id> \
  --confirm-preflight-id <preflight-id> \
  --authorizer "Named human reviewer" \
  --operator "Named human operator" \
  --review-ticket PILOT-AUTH-001 \
  --reason "Authorize one bounded manual non-production pilot run" \
  --valid-from-utc 2026-08-23T13:00:00Z \
  --valid-until-utc 2026-08-23T15:00:00Z
```

Output:

```text
data/fabric-pilots/<pilot-id>/pilot_authorization_<authorization-id>.json
```

Verification can require that the authorization is current at a specific time:

```bash
python3 -m forecasting.run_fabric_pilot_authorization verify-authorization \
  --authorization data/fabric-pilots/<pilot-id>/pilot_authorization_<authorization-id>.json \
  --as-of-utc 2026-08-23T13:30:00Z \
  --require-current
```

Authorization records hard-code:

```text
single_use=true
authorization_consumed=false
execution_authorized=true
execution_performed=false
manual_execution_required=true
automatic_execution_allowed=false
schedule_activation_allowed=false
deployment_authorized=false
model_activation_authorized=false
active_model_expected_after=ridge_weather_lag
```

Creating the record is not a Fabric operation. The next layer must consume it
once in an immutable operator receipt and assess the actual run against every
plan limit.

## Additional contracts

- `data-contracts/fabric_pilot_environment_snapshot_schema.json`
- `data-contracts/fabric_pilot_preflight_schema.json`
- `data-contracts/fabric_pilot_authorization_schema.json`
