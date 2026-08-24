# Fabric monitoring runbook

## Daily checks

- Review the latest ordinary Data Factory pipeline status.
- Query `dq_run_results` for errors and warnings.
- Confirm expected `source_area` values in silver and gold.
- Confirm no cross-area or future-weather match failures.
- Confirm no forecast training-boundary, time-horizon, tolerance, target-coverage, evaluation-contract, or origin-sequence failures.
- Compare validation and test MAE/RMSE by `requested_horizon_minutes` for persistence and ridge.
- For rolling-origin runs, compare metrics by `origin_fold` and confirm the final fold is the only test origin.
- Confirm `origin_cutoff_utc` increases and `training_observation_count` does not decrease.
- Review actual target delay and matched/eligible coverage by source area.
- Review unmatched-weather and freshness warnings.
- Confirm complete-snapshot pagination metadata on recent energy raw files.

## Manual interval checks

After a reviewed interval subflow:

- confirm one `point_prediction_run_id` and one `seasonal_comparison_run_id`;
- confirm every area/resource/city/horizon/model slice has the intended coverage levels;
- confirm `calibration_label_available_through_utc < evaluation_feature_start_utc`;
- compare empirical coverage with the nominal target without treating it as a guarantee;
- compare average, median, minimum, and maximum interval width by area, horizon, model, and coverage level;
- inspect calibration row counts and radii before comparing runs; and
- confirm `06e_prediction_interval_quality_checks` passed before retaining the evidence.

## Manual interval-health checks

After several independently validated interval runs exist:

1. Run `prediction_interval_monitoring_pipeline` manually.
2. Confirm `05f_prediction_interval_monitoring` wrote one
   `prediction-interval-monitoring-v1` summary.
3. Confirm `06f_prediction_interval_monitoring_quality_checks` passed.
4. Review failed hard checks before warning-only checks.
5. Triage each exact source-area/resource/city/horizon/model/feature-contract/
   coverage-level/interval-contract slice independently.
6. Confirm all five automatic-action fields remain `false`.

Interpret status as:

```text
failed
    -> one or more hard checks failed

warning
    -> hard checks passed, but reference history is insufficient or drift
       crossed an advisory threshold

healthy
    -> every emitted check passed
```

The default hard checks cover recent run count, latest interval-run freshness,
latest evaluation freshness, minimum causal calibration history, and
nominal-minus-empirical coverage shortfall.

When at least three reference runs exist, advisory checks compare recent and
immediately preceding history for:

```text
empirical coverage drop
weighted average interval-width increase
mean calibration-history decline
```

A monitoring failure does not authorize recalibration, refitting, model
selection, scheduling, registration, promotion, deployment, or alert delivery.

## Useful SQL

```sql
SELECT TOP (100)
    run_timestamp_utc,
    check_name,
    severity,
    failed_rows,
    status
FROM dbo.dq_run_results
ORDER BY run_timestamp_utc DESC, severity, check_name;
```

```sql
SELECT TOP (200)
    run_timestamp_utc,
    source_area,
    requested_horizon_minutes,
    target_tolerance_minutes,
    evaluation_contract_version,
    origin_fold,
    origin_count,
    origin_cutoff_utc,
    training_observation_count,
    split,
    model_name,
    observation_count,
    eligible_target_count,
    matched_target_count,
    target_coverage_pct,
    minimum_target_coverage_pct,
    horizon_minutes_avg,
    target_delay_minutes_avg,
    target_delay_minutes_max,
    mae_mw,
    rmse_mw,
    mape_pct,
    bias_mw,
    trained_through_utc,
    evaluation_feature_start_utc,
    evaluation_feature_end_utc,
    evaluation_start_utc,
    evaluation_end_utc
FROM dbo.forecast_baseline_metrics
ORDER BY
    run_timestamp_utc DESC,
    source_area,
    requested_horizon_minutes,
    origin_fold,
    split,
    model_name;
```

```sql
SELECT TOP (200)
    interval_run_timestamp_utc,
    interval_run_id,
    point_prediction_run_id,
    seasonal_comparison_run_id,
    source_area,
    requested_horizon_minutes,
    model_name,
    target_coverage_level,
    calibration_observation_count,
    calibration_quantile_rank,
    calibration_radius_mw,
    calibration_label_available_through_utc,
    evaluation_observation_count,
    empirical_coverage_pct,
    average_interval_width_mw,
    median_interval_width_mw,
    minimum_interval_width_mw,
    maximum_interval_width_mw
FROM dbo.forecast_prediction_interval_metrics
ORDER BY
    interval_run_timestamp_utc DESC,
    source_area,
    requested_horizon_minutes,
    target_coverage_level,
    model_name;
```

```sql
SELECT TOP (200)
    interval_run_timestamp_utc,
    source_area,
    feature_timestamp_utc,
    event_timestamp_utc AS target_timestamp_utc,
    requested_horizon_minutes,
    model_name,
    target_coverage_level,
    actual_demand_mw,
    point_prediction_mw,
    lower_prediction_mw,
    upper_prediction_mw,
    interval_width_mw,
    interval_covered,
    calibration_label_available_through_utc
FROM dbo.forecast_prediction_intervals
ORDER BY
    interval_run_timestamp_utc DESC,
    source_area,
    requested_horizon_minutes,
    target_coverage_level,
    model_name,
    event_timestamp_utc;
```

```sql
SELECT TOP (300)
    monitor_timestamp_utc,
    monitor_run_id,
    monitor_status,
    source_area,
    resource_id,
    city,
    requested_horizon_minutes,
    model_name,
    feature_contract_version,
    target_coverage_level,
    interval_contract_version,
    latest_interval_run_id,
    check_scope,
    severity,
    check_name,
    observed_value,
    threshold_value,
    comparator,
    passed,
    details
FROM dbo.forecast_prediction_interval_health_checks
ORDER BY
    monitor_timestamp_utc DESC,
    severity,
    passed,
    source_area,
    requested_horizon_minutes,
    target_coverage_level,
    model_name,
    check_name;
```

```sql
SELECT TOP (50)
    monitor_timestamp_utc,
    monitor_run_id,
    monitor_as_of_utc,
    monitor_status,
    check_count,
    passed_check_count,
    failed_error_check_count,
    failed_warning_check_count,
    monitored_interval_slice_count,
    retained_interval_run_count,
    automatic_remediation_allowed,
    automatic_recalibration_allowed,
    automatic_model_change_allowed,
    automatic_schedule_change_allowed,
    automatic_promotion_allowed,
    policy_version,
    monitoring_contract_version
FROM dbo.forecast_prediction_interval_health_summary
ORDER BY monitor_timestamp_utc DESC, monitor_run_id DESC;
```

## Rolling-origin evidence

A healthy rolling-origin run has one complete sequence per source area, resource, city, requested horizon, and model:

```text
origin_fold = 1..origin_count
```

All folds before `origin_count` use `split=validation`; the final fold uses `split=test`. The following must hold:

```text
trained_through_utc
    <
origin_cutoff_utc
    <=
evaluation_feature_start_utc
```

Cutoffs must increase, post-purge training history must not decrease, and no model may score the same feature timestamp in more than one origin.

Holdout runs use `evaluation_contract_version=fixed-holdout-v1` and null origin fields. Rolling runs use `rolling-origin-v1` and complete origin fields.

## Triage

- Source-binding failures mean `SOURCE_AREA`, `WEATHER_CITY`, and the NGED resource disagree.
- Ingestion failures usually indicate credentials, quota, source availability, contract drift, or incomplete CKAN pagination.
- Unscoped-source warnings indicate legacy raw files without `_pipeline_metadata`; retain them for lineage, but do not use them in scoped gold features.
- Unsupported-horizon, delay-tolerance, low-coverage, or label-availability failures are blocking correctness defects.
- An evaluation-contract failure usually means the run mixed holdout and rolling fields, omitted origin evidence, or placed validation/test rows in the wrong fold.
- An origin-sequence failure means folds are missing, cutoffs or training history moved backwards, or an evaluation timestamp was reused.
- A calibration-causality failure means an overlapping validation label or test-period label influenced interval width.
- A finite-sample-rank failure means the retained radius does not match the configured coverage level and calibration count.
- A fixed-radius or metric-consistency failure means row evidence and summary evidence diverged.
- A recent interval-history failure means fewer than the required number of retained runs exist for one exact monitoring slice.
- An interval-run or evaluation freshness failure means the newest retained evidence for a slice is older than the reviewed threshold.
- A minimum-calibration failure means at least one recent interval run used too little causal calibration history.
- A recent coverage-shortfall failure means weighted empirical coverage is materially below its nominal level in the recent window.
- An interval coverage-drop warning means recent empirical coverage deteriorated against the immediately preceding reference window.
- An interval-width warning means recent weighted average width increased; investigate uncertainty and data/model change rather than narrowing intervals automatically.
- A calibration-history warning means recent mean causal calibration evidence declined against reference history.
- Insufficient reference history is a warning and must not be interpreted as stable drift.
- Low empirical interval coverage or wider intervals are product evidence to investigate, not permission to recalibrate automatically.
- Zero eligible targets usually mean retained history is shorter than the configured horizon for a source group.
- Low matched/eligible coverage usually means source gaps exceeded `TARGET_TOLERANCE_MINUTES`; investigate source cadence before relaxing the control.
- Insufficient-history failures may occur after target matching and overlap purging; reduce rolling fold count only when the required evaluation resolution is genuinely lower.
- A ridge model underperforming persistence is not a pipeline failure, but it is evidence not to promote the model.
