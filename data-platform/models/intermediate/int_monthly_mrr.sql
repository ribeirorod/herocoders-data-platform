-- ---------------------------------------------------------------------------
-- int_monthly_mrr
-- Grain: one row per transaction, enriched with MRR and month bucket
--
-- MRR calculation:
--   Annual billing  → purchase_price / 12
--   Monthly billing → purchase_price as-is
--   $0 transactions → MRR = 0 (is_zero_value flag retained for analysis)
--   Refunds         → negative MRR contribution (is_refund flag)
--
-- Churn detection:
--   Grain: license = workspace (host_license_id) + product (addon_key)
--   A license is considered churned when no renewal transaction follows the
--   prior renewal within the expected billing window:
--     Annual  → no renewal within 13 months of the previous one
--     Monthly → no renewal within 1 month of the previous one
--   A workspace can partially churn (e.g. Checklist churns, Clockwork renews).
--   This renewal-gap pattern is more reliable than licenses.status, which only
--   reflects current state and can lag the true churn event.
-- ---------------------------------------------------------------------------

WITH transactions AS (
    SELECT
        t.transaction_id,
        t.addon_license_id,
        t.addon_key,
        t.addon_name,
        t.hosting,
        t.sale_date,
        t.sale_type,
        t.purchase_price,
        t.vendor_amount,
        t.billing_period,
        t.tier,
        t.maintenance_start_date,
        t.maintenance_end_date,
        t.sale_channel,
        t.is_zero_value,
        t.is_refund,
        CASE
            WHEN UPPER(t.billing_period) = 'ANNUAL'  THEN t.purchase_price / 12.0 -- needs a proper revenue recognition logic
            WHEN UPPER(t.billing_period) = 'MONTHLY' THEN t.purchase_price
            ELSE t.purchase_price / 12.0
        END                                                             AS mrr,
        DATE_TRUNC('month', t.sale_date)                               AS sale_month,
        l.cloud_id,
        l.host_license_id,
        l.host_license_id                                               AS instance_id,
        l.company,
        l.country
    FROM {{ ref('stg_marketplace_transactions') }} t
    LEFT JOIN {{ ref('stg_marketplace_licenses') }} l
        ON t.addon_license_id = l.addon_license_id
),

-- Renewal gap: flag months where a workspace had no renewal but maintenance lapsed
with_churn_flag AS (
    SELECT
        *,
        LAG(sale_month) OVER (
            PARTITION BY instance_id, addon_key
            ORDER BY sale_month
        )                                                               AS prev_renewal_month,
        DATE_TRUNC('month', DATEADD('day', 1, maintenance_end_date))   AS expected_next_renewal_month
    FROM transactions
    WHERE sale_type IN ('RENEWAL', 'NEW')
      AND NOT is_refund
)

SELECT
    transaction_id,
    addon_license_id,
    instance_id,
    cloud_id,
    host_license_id,
    addon_key,
    addon_name,
    hosting,
    company,
    country,
    sale_date,
    sale_month,
    sale_type,
    billing_period,
    purchase_price,
    vendor_amount,
    mrr,
    tier,
    maintenance_start_date,
    maintenance_end_date,
    sale_channel,
    is_zero_value,
    is_refund,
    prev_renewal_month,
    expected_next_renewal_month,
    CASE
        WHEN sale_type = 'RENEWAL'
         AND prev_renewal_month IS NOT NULL
         AND (
            (UPPER(billing_period) = 'ANNUAL'  AND DATEDIFF('month', prev_renewal_month, sale_month) > 13)
            OR
            (UPPER(billing_period) = 'MONTHLY' AND DATEDIFF('month', prev_renewal_month, sale_month) > 1)
         )
        THEN TRUE
        ELSE FALSE
    END                                                                 AS is_renewal_gap
FROM with_churn_flag
