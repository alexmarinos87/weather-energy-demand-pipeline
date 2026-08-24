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
- Low empirical interval coverage or wider intervals are product evidence to investigate, not permission to recalibrate automatically.
- Zero eligible targets usually mean retained history is shorter than the configured horizon for a source group.
- Low matched/eligible coverage usually means source gaps exceeded `TARGET_TOLERANCE_MINUTES`; investigate source cadence before relaxing the control.
- Insufficient-history failures may occur after target matching and overlap purging; reduce rolling fold count only when the required evaluation resolution is genuinely lower.
- A ridge model underperforming persistence is not a pipeline failure, but it is evidence not to promote the model.
