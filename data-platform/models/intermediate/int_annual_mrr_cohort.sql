{{
  config(
    materialized = 'table'
  )
}}

{%- set start_year  = 2024 -%}
{%- set start_month = 7    -%}
{%- set n_periods   = 24   -%}

{%- set cal_months = [] -%}
{%- for i in range(n_periods) -%}
    {%- set total  = start_month + i - 1 -%}
    {%- set yr     = start_year + (total // 12) -%}
    {%- set mo     = (total % 12) + 1 -%}
    {%- set mo_str = ('0' ~ mo) if mo < 10 else (mo | string) -%}
    {%- do cal_months.append({'col': 'm_' ~ yr ~ '_' ~ mo_str, 'dt': yr ~ '-' ~ mo_str ~ '-01'}) -%}
{%- endfor -%}

-- Grain: cohort_month + addon_key
-- Cohort revenue retention matrix for annual subscriptions.
-- Columns are calendar months; rows are acquisition cohorts.
-- The drop at the 12th column after each cohort's start = true renewal retention.

WITH acquisitions AS (
    SELECT
        l.host_license_id                                           AS instance_id,
        t.addon_key,
        DATE_TRUNC('month', MIN(t.sale_date))                       AS cohort_month
    FROM {{ ref('stg_marketplace_transactions') }} t
    JOIN {{ ref('stg_marketplace_licenses') }}    l USING (addon_license_id)
    WHERE UPPER(t.billing_period) = 'ANNUAL'
      AND t.sale_type     = 'NEW'
      AND NOT t.is_refund
      AND l.host_license_id IS NOT NULL
    GROUP BY 1, 2
),

annual_txns AS (
    SELECT
        a.cohort_month,
        a.instance_id,
        t.addon_key,
        DATE_TRUNC('month', t.sale_date)                            AS txn_month,
        t.purchase_price / 12.0                                     AS monthly_mrr
    FROM {{ ref('stg_marketplace_transactions') }} t
    JOIN {{ ref('stg_marketplace_licenses') }}    l USING (addon_license_id)
    JOIN acquisitions                              a
        ON  l.host_license_id = a.instance_id
        AND t.addon_key       = a.addon_key
    WHERE UPPER(t.billing_period) = 'ANNUAL'
      AND t.sale_type IN ('NEW', 'RENEWAL')
      AND NOT t.is_refund
      AND l.host_license_id IS NOT NULL
),

periods AS (
    {% for i in range(n_periods) %}
    SELECT {{ i }} AS period_offset{% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),

expanded AS (
    SELECT
        t.cohort_month,
        t.addon_key,
        t.instance_id,
        DATEADD('month', p.period_offset, t.cohort_month)           AS revenue_month,
        t.monthly_mrr
    FROM annual_txns t
    CROSS JOIN periods p
    WHERE p.period_offset >= DATEDIFF('month', t.cohort_month, t.txn_month)
      AND p.period_offset <  DATEDIFF('month', t.cohort_month, t.txn_month) + 12
)

SELECT
    cohort_month,
    addon_key,
    COUNT(DISTINCT instance_id)                                     AS cohort_size,
    {% for m in cal_months %}
    ROUND(SUM(CASE WHEN revenue_month = '{{ m.dt }}'::DATE THEN monthly_mrr END), 2) AS {{ m.col }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM expanded
GROUP BY 1, 2
ORDER BY 1, 2
