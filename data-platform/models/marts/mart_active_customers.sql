{{
  config(
    materialized = 'table'
  )
}}

-- ---------------------------------------------------------------------------
-- mart_active_customers
-- Business question: How many active customer workspaces per product per month?
--
-- "Active" = workspace had a renewal or new purchase in that month, or
-- had an active maintenance window covering that month.
-- Uses transactions as the event log (license table = current state only).
-- ---------------------------------------------------------------------------

WITH monthly_activity AS (
    SELECT
        sale_month,
        addon_key,
        addon_name,
        hosting,
        instance_id,
        company,
        country,
        SUM(mrr)                                                        AS monthly_mrr,
        MAX(is_zero_value::INT)                                         AS has_zero_value_tx,
        COUNT(transaction_id)                                           AS transaction_count
    FROM {{ ref('int_monthly_mrr') }}
    WHERE sale_type IN ('NEW', 'RENEWAL')
      AND NOT is_refund
      AND instance_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    sale_month,
    addon_key,
    addon_name,
    hosting,
    -- Workspace-level detail
    instance_id,
    company,
    country,
    monthly_mrr,
    transaction_count,
    has_zero_value_tx::BOOLEAN                                          AS has_zero_value_tx,
    -- Rollup metrics (useful for BI tools that don't aggregate on the fly)
    COUNT(*) OVER (PARTITION BY sale_month, addon_key)                  AS active_workspaces_per_product_month,
    COUNT(DISTINCT instance_id) OVER (PARTITION BY sale_month)          AS total_active_workspaces_month
FROM monthly_activity
ORDER BY sale_month DESC, addon_key, instance_id
