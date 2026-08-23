# Controlled Fabric pilot run receipt and assessment

## Purpose

This layer records and assesses one pilot that was performed manually outside the repository. It does not connect to Fabric, invoke a notebook, read a credential, create a schedule, deploy code, or activate a model.

The evidence chain is:

```text
verified draft plan
    -> eligible preflight
    -> current single-use human authorization
    -> operator-supplied run report and evidence files
    -> immutable run receipt
    -> immutable post-run assessment
    -> separate human decision
```

## Operator run report

The operator supplies one JSON object after the external run. It records:

- external run ID and named operator;
- completed, failed, aborted, or rolled-back status;
- start and end timestamps;
- environment, workspace, Lakehouse, and capacity identity;
- repository commit and Git tree SHA;
- executed notebook paths and written tables;
- credential **reference names** used, never values;
- forecast, prediction, and metric row counts;
- blocking quality-check failures and notebook retries;
- peak capacity units;
- whether a schedule, deployment, or model activation occurred;
- active model before and after; and
- rollback status and reason.

## Receipt contract

Receipt creation re-verifies the plan, preflight, and authorization chain. It requires the authorization to have been current at run start and the operator to match the named authorized operator.

Each supplied evidence file must be a regular non-symlink file below one evidence root. The receipt stores a role, relative path, byte size, and SHA-256 digest for every file.

The receipt is single-use by construction: its immutable output filename is based on the authorization ID, so a second receipt cannot overwrite or coexist under the same pilot output root without explicit manual intervention.

Every receipt records:

```text
authorization_consumed=true
execution_authorized=true
execution_performed=true
manual_execution_confirmed=true
automatic_execution_used=false
schedule_activation_allowed=false
deployment_authorized=false
model_activation_authorized=false
credential_values_recorded=false
source_evidence_mutated=false
```

## Assessment contract

The assessment compares the receipt against every plan limit and safety condition:

- authorization window, operator, repository, commit, and tree identity;
- duration, peak capacity, source rows, prediction rows, metric rows;
- failed blocking quality checks and notebook retries;
- exact planned notebook order for a completed run, or a valid prefix for an interrupted run;
- allowlisted tables and credential references;
- no schedule, deployment, model activation, or credential values;
- `ridge_weather_lag` active before and after;
- required evidence roles; and
- rollback evidence whenever the run failed, was aborted, rolled back, or exceeded a bound.

A completed, fully compliant run receives:

```text
eligible_for_post_pilot_review
```

Every other result receives:

```text
pilot_failed
```

Both outcomes still require a separate human decision. The assessment hard-codes:

```text
post_pilot_human_decision_required=true
automatic_model_activation_allowed=false
deployment_authorized=false
source_evidence_mutated=false
```

## Record a run

```bash
python3 -m forecasting.run_fabric_pilot_receipt record \
  --plan data/fabric-pilots/<pilot-id>/pilot_plan_v001.json \
  --preflight data/fabric-pilots/<pilot-id>/pilot_preflight_<id>.json \
  --authorization data/fabric-pilots/<pilot-id>/pilot_authorization_<id>.json \
  --run-report operator_run_report.json \
  --evidence-root pilot-output \
  --evidence-map evidence_roles.json \
  --confirm-authorization-id <authorization-id>
```

Example evidence-role map:

```json
{
  "run_log": "logs/pilot.log",
  "forecast_weather_output": "outputs/forecast_weather.parquet",
  "comparison_predictions": "outputs/comparison_predictions.parquet",
  "comparison_metrics": "outputs/comparison_metrics.parquet",
  "quality_results": "outputs/quality_results.parquet"
}
```

Default receipt output:

```text
data/fabric-pilots/<pilot-id>/pilot_run_receipt_<authorization-id>.json
```

## Assess a run

```bash
python3 -m forecasting.run_fabric_pilot_receipt assess \
  --plan data/fabric-pilots/<pilot-id>/pilot_plan_v001.json \
  --authorization data/fabric-pilots/<pilot-id>/pilot_authorization_<id>.json \
  --receipt data/fabric-pilots/<pilot-id>/pilot_run_receipt_<authorization-id>.json \
  --require-eligible
```

Default assessment output:

```text
data/fabric-pilots/<pilot-id>/pilot_run_assessment_<receipt-id>.json
```

`--require-eligible` returns exit code 2 after writing evidence when the outcome is `pilot_failed`.

## Contracts

- `data-contracts/fabric_pilot_run_report_schema.json`
- `data-contracts/fabric_pilot_run_receipt_schema.json`
- `data-contracts/fabric_pilot_run_assessment_schema.json`

## Boundary

This increment cannot perform the live pilot. The next dependency is an immutable, named human post-pilot decision to continue evidence collection, revise the candidate, or retire it. That decision must not automatically mutate the model registry or active model.
