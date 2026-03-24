{{
  config(
    materialized = 'table'
  )
}}

-- ---------------------------------------------------------------------------
-- mart_feature_adoption
-- Business question: Which features drive trial conversion vs. churn?
--
-- Grain: event_month + product + event_type + trial_outcome
-- Joins int_feature_usage to int_trial_lifecycle to compare feature usage
-- between workspaces that converted and those that didn't.
--
-- Bot and test events excluded upstream in int_feature_usage.
-- customer_cohort values: 'converted', 'expired', 'active_trial', 'non_trial'
-- Unmatched amplitude events (is_matched = FALSE) included in 'non_trial' cohort.
-- ---------------------------------------------------------------------------

WITH usage AS (
    SELECT
        event_month,
        product,
        event_type,
        instance_id,
        is_matched,
        event_count,
        unique_users,
        unique_sessions
    FROM {{ ref('int_feature_usage') }}
),

lifecycle AS (
    SELECT
        instance_id,
        addon_key,
        trial_outcome,
        eval_cohort_month,
        days_to_convert_eval
    FROM {{ ref('int_trial_lifecycle') }}
),

joined AS (
    SELECT
        u.event_month,
        u.product,
        u.event_type,
        u.instance_id,
        u.is_matched,
        -- Map to trial cohort if instance had a trial for this product
        l.trial_outcome,
        l.eval_cohort_month,
        l.days_to_convert_eval,
        u.event_count,
        u.unique_users,
        u.unique_sessions
    FROM usage u
    LEFT JOIN lifecycle l
        ON u.instance_id = l.instance_id
        AND u.product = REPLACE(l.addon_key, 'com.herocoders.', '')
)

SELECT
    event_month,
    product,
    event_type,
    COALESCE(trial_outcome, 'non_trial')                                AS customer_cohort,
    COUNT(DISTINCT instance_id)                                         AS workspace_count,
    SUM(event_count)                                                    AS total_events,
    SUM(unique_users)                                                   AS total_unique_users,
    SUM(unique_sessions)                                                AS total_unique_sessions,
    AVG(event_count)                                                    AS avg_events_per_workspace,
    AVG(unique_users)                                                   AS avg_users_per_workspace,
    COUNT(DISTINCT CASE WHEN event_count > 0 THEN instance_id END)
        / NULLIF(COUNT(DISTINCT instance_id), 0)                        AS feature_adoption_rate
FROM joined
WHERE instance_id IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY event_month DESC, product, event_type, customer_cohort
