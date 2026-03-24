{{
  config(
    materialized = 'table'
  )
}}

-- ---------------------------------------------------------------------------
-- mart_mrr_churn
-- Business question: What is monthly churn rate and Net Revenue Retention?
--
-- Grain: one row per month + addon_key
-- MRR movement types:
--   new          → first purchase (sale_type = 'NEW')
--   renewal      → ongoing renewal (sale_type = 'RENEWAL')
--   expansion    → renewal where MRR > prior period (higher tier/more seats)
--   contraction  → renewal where MRR < prior period
--   churn        → renewal gap detected (is_renewal_gap = TRUE)
--
-- NRR = (Beginning MRR + Expansion - Contraction - Churn) / Beginning MRR
-- Excludes $0 transactions from MRR calculations; retained as a separate count.
-- ---------------------------------------------------------------------------

-- billing_period is carried through as a grain dimension so dashboards can
-- filter to 'Monthly' for a clean trend line, or view 'Annual' separately.
-- Annual MRR is assigned to the sale_month only (not spread across 12 months)
-- — known limitation; production would use a date-spine to distribute evenly.
WITH base AS (
    SELECT
        sale_month,
        addon_key,
        addon_name,
        billing_period,
        instance_id,
        sale_type,
        mrr,
        is_zero_value,
        is_refund,
        is_renewal_gap,
        LAG(mrr) OVER (
            PARTITION BY instance_id, addon_key, billing_period
            ORDER BY sale_month
        )                                                               AS prior_mrr
    FROM {{ ref('int_monthly_mrr') }}
    WHERE NOT is_refund
      AND instance_id IS NOT NULL
),

categorized AS (
    SELECT
        sale_month,
        addon_key,
        addon_name,
        billing_period,
        instance_id,
        mrr,
        is_zero_value,
        is_renewal_gap,
        CASE
            WHEN sale_type = 'NEW'                          THEN 'new'
            WHEN is_renewal_gap                             THEN 'reactivation'
            WHEN sale_type = 'RENEWAL' AND prior_mrr IS NULL THEN 'renewal'
            WHEN sale_type = 'RENEWAL' AND mrr > prior_mrr  THEN 'expansion'
            WHEN sale_type = 'RENEWAL' AND mrr < prior_mrr  THEN 'contraction'
            WHEN sale_type = 'RENEWAL'                      THEN 'renewal'
            ELSE sale_type
        END                                                             AS mrr_movement_type
    FROM base
),

monthly_summary AS (
    SELECT
        sale_month,
        addon_key,
        addon_name,
        billing_period,
        SUM(CASE WHEN mrr_movement_type = 'new'          THEN mrr ELSE 0 END)  AS new_mrr,
        SUM(CASE WHEN mrr_movement_type = 'renewal'      THEN mrr ELSE 0 END)  AS renewal_mrr,
        SUM(CASE WHEN mrr_movement_type = 'expansion'    THEN mrr ELSE 0 END)  AS expansion_mrr,
        SUM(CASE WHEN mrr_movement_type = 'contraction'  THEN mrr ELSE 0 END)  AS contraction_mrr,
        SUM(CASE WHEN mrr_movement_type = 'reactivation' THEN mrr ELSE 0 END)  AS reactivation_mrr,
        COUNT(DISTINCT CASE WHEN mrr_movement_type = 'new'      THEN instance_id END) AS new_logos,
        COUNT(DISTINCT CASE WHEN mrr_movement_type = 'renewal'  THEN instance_id END) AS renewing_logos,
        COUNT(DISTINCT instance_id)                                              AS total_logos,
        SUM(mrr)                                                                 AS total_mrr,
        COUNT(DISTINCT CASE WHEN is_zero_value THEN instance_id END)            AS zero_value_logos
    FROM categorized
    GROUP BY 1, 2, 3, 4
),

with_prior AS (
    SELECT
        *,
        LAG(total_mrr) OVER (PARTITION BY addon_key, billing_period ORDER BY sale_month)   AS beginning_mrr,
        LAG(total_logos) OVER (PARTITION BY addon_key, billing_period ORDER BY sale_month) AS beginning_logos
    FROM monthly_summary
)

SELECT
    sale_month,
    addon_key,
    addon_name,
    billing_period,
    new_mrr,
    renewal_mrr,
    expansion_mrr,
    contraction_mrr,
    reactivation_mrr,
    total_mrr,
    new_logos,
    renewing_logos,
    total_logos,
    zero_value_logos,
    beginning_mrr,
    beginning_logos,
    -- Net Revenue Retention: how well we retain + grow existing revenue
    CASE
        WHEN beginning_mrr IS NULL OR beginning_mrr = 0 THEN NULL
        ELSE ROUND((renewal_mrr + expansion_mrr - contraction_mrr) / beginning_mrr, 4)
    END                                                                          AS nrr,
    -- Logo churn rate (note: open question — expired only? cancelled only? both?)
    CASE
        WHEN beginning_logos IS NULL OR beginning_logos = 0 THEN NULL
        ELSE ROUND(1.0 - (renewing_logos + 0.0) / beginning_logos, 4)
    END                                                                          AS logo_churn_rate
FROM with_prior
ORDER BY sale_month DESC, addon_key, billing_period
