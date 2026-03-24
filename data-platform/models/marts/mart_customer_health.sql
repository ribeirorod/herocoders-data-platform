{{
  config(
    materialized = 'table'
  )
}}

-- ---------------------------------------------------------------------------
-- mart_customer_health
-- Business question: What is the health of each customer workspace?
--
-- Grain: one row per workspace instance
-- Health score (0–100) is a weighted composite of three signals:
--   40% — License status  (active vs expired/cancelled)
--   30% — Revenue         (current MRR relative to cohort median)
--   30% — Engagement      (distinct features used in last 90 days)
--
-- Each component score is exposed for re-weighting without model changes.
-- Open questions for the team: exact weights, engagement window (90d vs 60d),
-- whether $0 logos should count in revenue signal.
-- ---------------------------------------------------------------------------

WITH license_signal AS (
    SELECT
        host_license_id                                                 AS instance_id,
        company,
        country,
        BOOLOR_AGG(is_active)                                           AS has_active_license,
        COUNT(DISTINCT addon_key)                                       AS product_count,
        MAX(maintenance_end_date)                                       AS latest_maintenance_end,
        DATEDIFF('day', MAX(maintenance_end_date), CURRENT_DATE)        AS days_since_maintenance_end
    FROM {{ ref('stg_marketplace_licenses') }}
    GROUP BY 1, 2, 3
),

revenue_signal AS (
    SELECT
        instance_id,
        SUM(mrr)                                                        AS current_mrr,
        MAX(sale_month)                                                 AS last_transaction_month
    FROM {{ ref('int_monthly_mrr') }}
    WHERE sale_month = (SELECT MAX(sale_month) FROM {{ ref('int_monthly_mrr') }})
      AND NOT is_refund
      AND instance_id IS NOT NULL
    GROUP BY 1
),

median_mrr AS (
    SELECT MEDIAN(current_mrr) AS med_mrr
    FROM revenue_signal
    WHERE current_mrr > 0
),

engagement_signal AS (
    SELECT
        instance_id,
        SUM(event_count)                                                AS events_90d,
        COUNT(DISTINCT event_type)                                      AS distinct_features_90d,
        SUM(unique_users)                                               AS unique_users_90d,
        MAX(event_month)                                                AS last_active_month
    FROM {{ ref('int_feature_usage') }}
    WHERE event_month >= DATE_TRUNC('month', DATEADD('day', -90, CURRENT_DATE))
      AND is_matched
      AND instance_id IS NOT NULL
    GROUP BY 1
),

scored AS (
    SELECT
        ls.instance_id,
        ls.company,
        ls.country,
        ls.product_count,
        ls.has_active_license,
        ls.latest_maintenance_end,
        ls.days_since_maintenance_end,
        COALESCE(rs.current_mrr, 0)                                     AS current_mrr,
        rs.last_transaction_month,
        COALESCE(es.events_90d, 0)                                      AS events_90d,
        COALESCE(es.distinct_features_90d, 0)                           AS distinct_features_90d,
        COALESCE(es.unique_users_90d, 0)                                AS unique_users_90d,
        es.last_active_month,
        -- Component: license (40%)
        CASE
            WHEN ls.has_active_license              THEN 100
            WHEN ls.days_since_maintenance_end <= 30 THEN 50
            ELSE 0
        END                                                             AS license_score,
        -- Component: revenue (30%)
        CASE
            WHEN COALESCE(rs.current_mrr, 0) = 0    THEN 0
            WHEN rs.current_mrr >= mm.med_mrr * 2    THEN 100
            WHEN rs.current_mrr >= mm.med_mrr         THEN 75
            WHEN rs.current_mrr >= mm.med_mrr * 0.5   THEN 50
            ELSE 25
        END                                                             AS revenue_score,
        -- Component: engagement (30%)
        CASE
            WHEN COALESCE(es.distinct_features_90d, 0) >= 5 THEN 100
            WHEN COALESCE(es.distinct_features_90d, 0) >= 3 THEN 75
            WHEN COALESCE(es.distinct_features_90d, 0) >= 1 THEN 50
            ELSE 0
        END                                                             AS engagement_score
    FROM license_signal ls
    CROSS JOIN median_mrr mm
    LEFT JOIN revenue_signal rs USING (instance_id)
    LEFT JOIN engagement_signal es USING (instance_id)
)

SELECT
    instance_id,
    company,
    country,
    product_count,
    has_active_license,
    latest_maintenance_end,
    days_since_maintenance_end,
    current_mrr,
    last_transaction_month,
    events_90d,
    distinct_features_90d,
    unique_users_90d,
    last_active_month,
    license_score,
    revenue_score,
    engagement_score,
    ROUND(0.40 * license_score + 0.30 * revenue_score + 0.30 * engagement_score, 1) AS health_score,
    CASE
        WHEN ROUND(0.40 * license_score + 0.30 * revenue_score + 0.30 * engagement_score, 1) >= 75 THEN 'healthy'
        WHEN ROUND(0.40 * license_score + 0.30 * revenue_score + 0.30 * engagement_score, 1) >= 50 THEN 'at_risk'
        ELSE 'churned_or_inactive'
    END                                                                 AS health_segment
FROM scored
ORDER BY health_score DESC
