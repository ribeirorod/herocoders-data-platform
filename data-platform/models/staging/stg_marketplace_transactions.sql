{{
  config(
    materialized = 'incremental',
    unique_key   = 'transaction_id',
    pre_hook     = "{{ load_raw_source('marketplace_transactions') }}"
  )
}}

WITH source AS (
    SELECT
        raw_data:payload:transaction_id::VARCHAR                    AS transaction_id,
        raw_data:payload:addon_license_id::INTEGER                  AS addon_license_id,
        raw_data:payload:license_id::VARCHAR                        AS license_id,
        raw_data:payload:app_entitlement_number::VARCHAR            AS app_entitlement_number,
        raw_data:payload:addon_key::VARCHAR                         AS addon_key,
        raw_data:payload:addon_name::VARCHAR                        AS addon_name,
        raw_data:payload:hosting::VARCHAR                           AS hosting,
        raw_data:payload:sale_date::DATE                            AS sale_date,
        raw_data:payload:sale_type::VARCHAR                         AS sale_type,
        raw_data:payload:purchase_price::FLOAT                      AS purchase_price,
        raw_data:payload:vendor_amount::FLOAT                       AS vendor_amount,
        raw_data:payload:billing_period::VARCHAR                    AS billing_period,
        raw_data:payload:tier::VARCHAR                              AS tier,
        raw_data:payload:maintenance_start_date::DATE               AS maintenance_start_date,
        raw_data:payload:maintenance_end_date::DATE                 AS maintenance_end_date,
        raw_data:payload:sale_channel::VARCHAR                      AS sale_channel,
        raw_data:payload:parent_product_name::VARCHAR               AS parent_product_name,
        raw_data:payload:parent_product_edition::VARCHAR            AS parent_product_edition,
        raw_data:payload:loyalty_discount::FLOAT                    AS loyalty_discount,
        raw_data:payload:marketplace_promotion_discount::FLOAT      AS marketplace_promotion_discount,
        raw_data:payload:expert_discount::FLOAT                     AS expert_discount,
        -- manual_discount excluded: constant 0 across all rows
        raw_data:metadata:extracted_at::TIMESTAMP_LTZ               AS extracted_at,
        loaded_at
    FROM {{ source('raw', 'marketplace_transactions_raw') }}

    {% if is_incremental() %}
        WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    {{ deduplicate('source', 'transaction_id', 'extracted_at DESC') }}
)

SELECT
    transaction_id,
    addon_license_id,
    license_id,
    app_entitlement_number,
    addon_key,
    addon_name,
    REPLACE(LOWER(TRIM(hosting)), ' ', '')                  AS hosting,
    sale_date,
    UPPER(sale_type)                                        AS sale_type,
    purchase_price,
    vendor_amount,
    billing_period,
    tier,
    maintenance_start_date,
    maintenance_end_date,
    sale_channel,
    parent_product_name,
    parent_product_edition,
    COALESCE(loyalty_discount, 0)                           AS loyalty_discount,
    COALESCE(marketplace_promotion_discount, 0)             AS marketplace_promotion_discount,
    COALESCE(expert_discount, 0)                            AS expert_discount,
    purchase_price = 0                                      AS is_zero_value,
    purchase_price < 0                                      AS is_refund,
    extracted_at,
    loaded_at
FROM deduplicated
