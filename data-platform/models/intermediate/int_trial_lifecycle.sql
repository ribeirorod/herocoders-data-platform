-- ---------------------------------------------------------------------------
-- int_trial_lifecycle
-- Grain: one row per evaluation license
--
-- Conversion bridge: evaluation_license on commercial records is a direct FK
-- to addon_license_id of the originating eval. No fuzzy matching needed.
-- ---------------------------------------------------------------------------

WITH evals AS (
    SELECT
        addon_license_id                                                AS eval_license_id,
        addon_key,
        host_license_id                                                AS instance_id,
        company,
        country,
        hosting,
        tier,
        maintenance_start_date                                          AS eval_start_date,
        maintenance_end_date                                            AS eval_end_date,
        last_updated
    FROM {{ ref('stg_marketplace_licenses') }}
    WHERE is_evaluation
),

conversions AS (
    -- Commercial licenses that reference an eval — one eval can only convert once
    SELECT
        evaluation_license                                              AS eval_license_id,
        addon_license_id                                                AS commercial_license_id,
        maintenance_start_date                                          AS commercial_start_date,
        tier                                                            AS commercial_tier,
        days_to_convert_eval
    FROM {{ ref('stg_marketplace_licenses') }}
    WHERE is_converted
),

-- First transaction for the converted commercial license (true sale date)
first_transaction AS (
    SELECT
        addon_license_id,
        MIN(sale_date)                                                  AS first_sale_date,
        SUM(purchase_price)                                             AS total_arr
    FROM {{ ref('stg_marketplace_transactions') }}
    WHERE sale_type = 'NEW'
      AND NOT is_refund
    GROUP BY 1
)

SELECT
    e.eval_license_id,
    e.addon_key,
    e.instance_id,
    e.company,
    e.country,
    e.hosting,
    e.tier                                                              AS eval_tier,
    e.eval_start_date,
    e.eval_end_date,
    -- Conversion outcome
    c.commercial_license_id,
    c.commercial_start_date,
    c.commercial_tier,
    c.days_to_convert_eval,
    ft.first_sale_date,
    ft.total_arr                                                        AS converted_arr,
    -- Lifecycle classification
    CASE
        WHEN c.commercial_license_id IS NOT NULL    THEN 'converted'
        WHEN e.eval_end_date < CURRENT_DATE         THEN 'expired'
        ELSE                                             'active_trial'
    END                                                                 AS trial_outcome,
    c.commercial_license_id IS NOT NULL                                 AS is_converted,
    e.eval_end_date < CURRENT_DATE AND c.commercial_license_id IS NULL  AS is_expired_no_convert,
    DATE_TRUNC('month', e.eval_start_date)                              AS eval_cohort_month
FROM evals e
LEFT JOIN conversions c
    ON e.eval_license_id = c.eval_license_id
LEFT JOIN first_transaction ft
    ON c.commercial_license_id = ft.addon_license_id
