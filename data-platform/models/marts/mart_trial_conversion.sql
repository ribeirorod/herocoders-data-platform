{{
  config(
    materialized = 'table'
  )
}}

-- ---------------------------------------------------------------------------
-- mart_trial_conversion
-- Business question: What is the trial-to-paid conversion rate over time?
--
-- Grain: one row per eval license (trial)
-- Cohort by: eval_start_date month
-- Outcome: converted / expired_no_convert / active_trial
--
-- Conversion rate = converted / (converted + expired_no_convert)
-- Active trials excluded from the denominator — outcome not yet known.
-- ---------------------------------------------------------------------------

WITH lifecycle AS (
    SELECT
        eval_license_id,
        addon_key,
        instance_id,
        company,
        country,
        hosting,
        eval_tier,
        eval_start_date,
        eval_end_date,
        commercial_license_id,
        commercial_start_date,
        commercial_tier,
        days_to_convert_eval,
        first_sale_date,
        converted_arr,
        trial_outcome,
        is_converted,
        is_expired_no_convert,
        DATE_TRUNC('month', eval_start_date)                            AS eval_cohort_month
    FROM {{ ref('int_trial_lifecycle') }}
),

cohort_summary AS (
    SELECT
        eval_cohort_month,
        addon_key,
        COUNT(*)                                                        AS total_trials,
        COUNT(CASE WHEN trial_outcome = 'converted' THEN 1 END)         AS converted_count,
        COUNT(CASE WHEN trial_outcome = 'expired_no_convert' THEN 1 END) AS expired_count,
        COUNT(CASE WHEN trial_outcome = 'active_trial' THEN 1 END)      AS active_trial_count,
        AVG(CASE WHEN is_converted THEN days_to_convert_eval END)       AS avg_days_to_convert,
        MEDIAN(CASE WHEN is_converted THEN days_to_convert_eval END)    AS median_days_to_convert,
        SUM(CASE WHEN is_converted THEN converted_arr ELSE 0 END)       AS cohort_converted_arr
    FROM lifecycle
    GROUP BY 1, 2
)

SELECT
    eval_cohort_month,
    addon_key,
    total_trials,
    converted_count,
    expired_count,
    active_trial_count,
    -- Conversion rate excludes still-active trials (outcome unknown)
    CASE
        WHEN (converted_count + expired_count) = 0 THEN NULL
        ELSE ROUND(converted_count / (converted_count + expired_count)::FLOAT, 4)
    END                                                                 AS conversion_rate,
    avg_days_to_convert,
    median_days_to_convert,
    cohort_converted_arr
FROM cohort_summary
ORDER BY eval_cohort_month DESC, addon_key
