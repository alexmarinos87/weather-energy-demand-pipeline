-- SQL endpoint checks for the latest fixed-holdout or rolling-origin run.

WITH latest_run AS (
    SELECT TOP (1) run_id
    FROM dbo.forecast_baseline_predictions
    ORDER BY run_timestamp_utc DESC, run_id DESC
),
latest_predictions AS (
    SELECT *
    FROM dbo.forecast_baseline_predictions
    WHERE run_id = (SELECT run_id FROM latest_run)
),
latest_metrics AS (
    SELECT *
    FROM dbo.forecast_baseline_metrics
    WHERE run_id = (SELECT run_id FROM latest_run)
),
rolling_origin_order AS (
    SELECT
        source_area,
        resource_id,
        city,
        requested_horizon_minutes,
        model_name,
        origin_fold,
        origin_count,
        origin_cutoff_utc,
        training_observation_count,
        LAG(origin_cutoff_utc) OVER (
            PARTITION BY
                source_area,
                resource_id,
                city,
                requested_horizon_minutes,
                model_name
            ORDER BY origin_fold
        ) AS previous_origin_cutoff_utc,
        LAG(training_observation_count) OVER (
            PARTITION BY
                source_area,
                resource_id,
                city,
                requested_horizon_minutes,
                model_name
            ORDER BY origin_fold
        ) AS previous_training_observation_count
    FROM latest_metrics
    WHERE evaluation_contract_version = 'rolling-origin-v1'
),
rolling_origin_groups AS (
    SELECT
        source_area,
        resource_id,
        city,
        requested_horizon_minutes,
        model_name,
        COUNT(DISTINCT origin_fold) AS fold_count,
        MIN(origin_fold) AS minimum_fold,
        MAX(origin_fold) AS maximum_fold,
        MIN(origin_count) AS minimum_origin_count,
        MAX(origin_count) AS maximum_origin_count
    FROM latest_metrics
    WHERE evaluation_contract_version = 'rolling-origin-v1'
    GROUP BY
        source_area,
        resource_id,
        city,
        requested_horizon_minutes,
        model_name
),
rolling_reused_evaluations AS (
    SELECT
        source_area,
        resource_id,
        city,
        requested_horizon_minutes,
        model_name,
        feature_timestamp_utc,
        COUNT(DISTINCT origin_fold) AS origin_uses
    FROM latest_predictions
    WHERE evaluation_contract_version = 'rolling-origin-v1'
    GROUP BY
        source_area,
        resource_id,
        city,
        requested_horizon_minutes,
        model_name,
        feature_timestamp_utc
)
SELECT
    'forecast_prediction_training_boundary' AS check_name,
    COUNT_BIG(*) AS failed_rows
FROM latest_predictions
WHERE feature_timestamp_utc <= trained_through_utc

UNION ALL
SELECT 'forecast_prediction_time_horizon_valid', COUNT_BIG(*)
FROM latest_predictions
WHERE
       event_timestamp_utc <= feature_timestamp_utc
    OR requested_horizon_minutes NOT IN (30, 60)
    OR target_tolerance_minutes < 0
    OR horizon_steps < 1
    OR horizon_minutes < requested_horizon_minutes
    OR target_delay_minutes < 0
    OR target_delay_minutes > target_tolerance_minutes
    OR ABS(
        horizon_minutes
        - requested_horizon_minutes
        - target_delay_minutes
    ) > 0.000001
    OR feature_contract_version <> 'time-horizon-v1'

UNION ALL
SELECT 'forecast_prediction_required_fields', COUNT_BIG(*)
FROM latest_predictions
WHERE
       source_area IS NULL
    OR resource_id IS NULL
    OR city IS NULL
    OR feature_timestamp_utc IS NULL
    OR event_timestamp_utc IS NULL
    OR requested_horizon_minutes IS NULL
    OR target_tolerance_minutes IS NULL
    OR horizon_steps IS NULL
    OR horizon_minutes IS NULL
    OR target_delay_minutes IS NULL
    OR split IS NULL
    OR model_name IS NULL
    OR current_demand_mw IS NULL
    OR actual_demand_mw IS NULL
    OR predicted_demand_mw IS NULL
    OR trained_through_utc IS NULL
    OR feature_contract_version IS NULL
    OR evaluation_contract_version IS NULL

UNION ALL
SELECT 'forecast_target_coverage_valid', COUNT_BIG(*)
FROM latest_metrics
WHERE
       eligible_target_count <= 0
    OR matched_target_count <= 0
    OR matched_target_count > eligible_target_count
    OR target_coverage_pct < minimum_target_coverage_pct
    OR target_coverage_pct < 0
    OR target_coverage_pct > 100
    OR target_delay_minutes_max > target_tolerance_minutes

UNION ALL
SELECT
    'forecast_evaluation_contract_valid',
    (
        SELECT COUNT_BIG(*)
        FROM latest_predictions
        WHERE
            evaluation_contract_version NOT IN (
                'fixed-holdout-v1',
                'rolling-origin-v1'
            )
            OR (
                evaluation_contract_version = 'fixed-holdout-v1'
                AND (
                    origin_fold IS NOT NULL
                    OR origin_count IS NOT NULL
                    OR origin_cutoff_utc IS NOT NULL
                    OR training_observation_count IS NOT NULL
                    OR split NOT IN ('validation', 'test')
                )
            )
            OR (
                evaluation_contract_version = 'rolling-origin-v1'
                AND (
                    origin_fold IS NULL
                    OR origin_count IS NULL
                    OR origin_cutoff_utc IS NULL
                    OR training_observation_count IS NULL
                    OR origin_count < 2
                    OR origin_fold < 1
                    OR origin_fold > origin_count
                    OR training_observation_count <= 0
                    OR trained_through_utc >= origin_cutoff_utc
                    OR origin_cutoff_utc > feature_timestamp_utc
                    OR (
                        origin_fold < origin_count
                        AND split <> 'validation'
                    )
                    OR (
                        origin_fold = origin_count
                        AND split <> 'test'
                    )
                )
            )
    )
    +
    (
        SELECT COUNT_BIG(*)
        FROM latest_metrics
        WHERE
            evaluation_contract_version NOT IN (
                'fixed-holdout-v1',
                'rolling-origin-v1'
            )
            OR (
                evaluation_contract_version = 'fixed-holdout-v1'
                AND (
                    origin_fold IS NOT NULL
                    OR origin_count IS NOT NULL
                    OR origin_cutoff_utc IS NOT NULL
                    OR training_observation_count IS NOT NULL
                )
            )
            OR (
                evaluation_contract_version = 'rolling-origin-v1'
                AND (
                    origin_fold IS NULL
                    OR origin_count IS NULL
                    OR origin_cutoff_utc IS NULL
                    OR training_observation_count IS NULL
                    OR origin_count < 2
                    OR origin_fold < 1
                    OR origin_fold > origin_count
                    OR training_observation_count <= 0
                    OR trained_through_utc >= origin_cutoff_utc
                    OR origin_cutoff_utc > evaluation_feature_start_utc
                    OR (
                        origin_fold < origin_count
                        AND split <> 'validation'
                    )
                    OR (
                        origin_fold = origin_count
                        AND split <> 'test'
                    )
                )
            )
    )

UNION ALL
SELECT
    'forecast_rolling_origin_sequence_valid',
    (
        SELECT COUNT_BIG(*)
        FROM rolling_origin_groups
        WHERE
               minimum_fold <> 1
            OR maximum_fold <> maximum_origin_count
            OR fold_count <> maximum_origin_count
            OR minimum_origin_count <> maximum_origin_count
    )
    +
    (
        SELECT COUNT_BIG(*)
        FROM rolling_origin_order
        WHERE
            origin_fold > 1
            AND (
                origin_cutoff_utc <= previous_origin_cutoff_utc
                OR training_observation_count
                    < previous_training_observation_count
            )
    )
    +
    (
        SELECT COUNT_BIG(*)
        FROM rolling_reused_evaluations
        WHERE origin_uses > 1
    )

UNION ALL
SELECT 'forecast_metrics_valid', COUNT_BIG(*)
FROM latest_metrics
WHERE
       observation_count <= 0
    OR requested_horizon_minutes NOT IN (30, 60)
    OR target_tolerance_minutes < 0
    OR horizon_steps_avg < 1
    OR horizon_minutes_avg < requested_horizon_minutes
    OR target_delay_minutes_avg < 0
    OR mae_mw < 0
    OR rmse_mw < 0
    OR evaluation_feature_start_utc <= trained_through_utc
    OR evaluation_feature_end_utc < evaluation_feature_start_utc
    OR evaluation_start_utc <= evaluation_feature_start_utc
    OR evaluation_end_utc < evaluation_start_utc
    OR feature_contract_version <> 'time-horizon-v1'
    OR evaluation_contract_version NOT IN (
        'fixed-holdout-v1',
        'rolling-origin-v1'
    );
