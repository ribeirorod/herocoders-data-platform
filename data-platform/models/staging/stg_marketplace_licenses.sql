{{
  config(
    materialized = 'incremental',
    unique_key   = 'addon_license_id',
    pre_hook     = "{{ load_raw_source('marketplace_licenses') }}"
  )
}}

WITH source AS (
    SELECT
        raw_data:payload:addon_license_id::INTEGER              AS addon_license_id,
        raw_data:payload:license_id::VARCHAR                    AS license_id,
        raw_data:payload:app_entitlement_number::VARCHAR        AS app_entitlement_number,
        raw_data:payload:host_license_id::VARCHAR               AS host_license_id,
        raw_data:payload:host_entitlement_number::VARCHAR       AS host_entitlement_number,
        raw_data:payload:cloud_id::VARCHAR                      AS cloud_id,
        raw_data:payload:cloud_site_hostname::VARCHAR           AS cloud_site_hostname,
        raw_data:payload:addon_key::VARCHAR                     AS addon_key,
        raw_data:payload:addon_name::VARCHAR                    AS addon_name,
        raw_data:payload:hosting::VARCHAR                       AS hosting,
        raw_data:payload:last_updated::TIMESTAMP_LTZ            AS last_updated,
        raw_data:payload:license_type::VARCHAR                  AS license_type,
        raw_data:payload:maintenance_start_date::DATE           AS maintenance_start_date,
        raw_data:payload:maintenance_end_date::DATE             AS maintenance_end_date,
        raw_data:payload:status::VARCHAR                        AS status,
        raw_data:payload:tier::VARCHAR                          AS tier,
        raw_data:payload:company::VARCHAR                       AS company,
        raw_data:payload:country::VARCHAR                       AS country,
        raw_data:payload:region::VARCHAR                        AS region,
        raw_data:payload:tech_contact_email::VARCHAR            AS tech_contact_email,
        raw_data:payload:tech_contact_name::VARCHAR             AS tech_contact_name,
        -- Evaluation / conversion fields
        raw_data:payload:evaluation_opportunity_size::VARCHAR   AS evaluation_opportunity_size,
        raw_data:payload:evaluation_license::INTEGER            AS evaluation_license,
        raw_data:payload:days_to_convert_eval::FLOAT            AS days_to_convert_eval,
        raw_data:payload:evaluation_start_date::DATE            AS evaluation_start_date,
        raw_data:payload:evaluation_end_date::DATE              AS evaluation_end_date,
        raw_data:payload:evaluation_sale_date::DATE             AS evaluation_sale_date,
        raw_data:metadata:extracted_at::TIMESTAMP_LTZ           AS extracted_at,
        loaded_at
    FROM {{ source('raw', 'marketplace_licenses_raw') }}

    {% if is_incremental() %}
        WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    {{ deduplicate('source', 'addon_license_id', 'last_updated DESC') }}
),

normalized AS (
    SELECT
        addon_license_id,
        license_id,
        app_entitlement_number,
        host_license_id,
        host_entitlement_number,
        cloud_id,
        cloud_site_hostname,
        addon_key,
        addon_name,
        REPLACE(LOWER(TRIM(hosting)), ' ', '')                          AS hosting,
        last_updated,
        LOWER(license_type)                                             AS license_type,
        maintenance_start_date,
        maintenance_end_date,

        -- Normalize status: 5 raw variants → 3 logical states
        CASE UPPER(TRIM(status))
            WHEN 'ACTIVE'    THEN 'active'
            WHEN 'ACTV'      THEN 'active'
            WHEN 'EXPIRED'   THEN 'expired'
            WHEN 'CANCELLED' THEN 'cancelled'
            ELSE LOWER(TRIM(status))  -- fallback: surface unknowns for investigation
        END                                                             AS status,

        tier, 
        company,
        country,
        region,
        tech_contact_email,
        tech_contact_name,
        evaluation_opportunity_size,
        evaluation_license,
        days_to_convert_eval,
        evaluation_start_date,
        evaluation_end_date,
        evaluation_sale_date,

        -- Convenience flags
        LOWER(license_type) = 'evaluation'                              AS is_evaluation,
        CASE UPPER(TRIM(status))
            WHEN 'ACTIVE' THEN TRUE
            WHEN 'ACTV'   THEN TRUE
            ELSE FALSE
        END                                                             AS is_active,
        evaluation_license IS NOT NULL                                  AS is_converted,

        extracted_at,
        loaded_at
    FROM deduplicated
)

SELECT * FROM normalized
