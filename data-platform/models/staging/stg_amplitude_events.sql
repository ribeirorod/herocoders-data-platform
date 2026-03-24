{{
  config(
    materialized = 'incremental',
    unique_key   = 'event_id',
    pre_hook     = "{{ load_raw_source('amplitude_events') }}"
  )
}}

WITH source AS (
    SELECT
        raw_data:payload:event_id::VARCHAR                  AS event_id,
        raw_data:payload:event_type::VARCHAR                AS event_type,
        raw_data:payload:event_time::TIMESTAMP_LTZ          AS event_time,
        raw_data:payload:user_id::VARCHAR                   AS user_id,
        raw_data:payload:device_id::VARCHAR                 AS device_id,
        raw_data:payload:platform::VARCHAR                  AS platform,
        raw_data:payload:event_source::VARCHAR              AS event_source,
        raw_data:payload:product::VARCHAR                   AS product,
        raw_data:payload:event_properties::VARIANT          AS event_properties,
        raw_data:payload:user_property_company::VARCHAR     AS user_property_company,
        raw_data:payload:user_property_plan::VARCHAR        AS user_property_plan,
        raw_data:payload:user_property_tier::VARCHAR        AS user_property_tier,
        raw_data:payload:session_id::INTEGER                AS session_id,
        raw_data:payload:country::VARCHAR                   AS country,
        raw_data:payload:region::VARCHAR                    AS region,
        raw_data:metadata:extracted_at::TIMESTAMP_LTZ       AS extracted_at,
        loaded_at
    FROM {{ source('raw', 'amplitude_events_raw') }}

    {% if is_incremental() %}
        WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    {{ deduplicate('source', 'event_id', 'event_time DESC') }}
)

SELECT
    event_id,
    event_type,
    event_time,
    user_id,
    device_id,
    LOWER(platform)                                         AS platform,
    event_source,
    LOWER(product)                                          AS product,
    event_properties,
    -- Raw company name preserved — normalization and cross-source join in intermediate
    user_property_company,
    --blank strings and "None" → NULL
    NULLIF(NULLIF(TRIM(user_property_plan), ''), 'None')    AS user_property_plan,
    NULLIF(NULLIF(TRIM(user_property_tier), ''), 'None')    AS user_property_tier,
    session_id,
    LOWER(country)                                          AS country,
    LOWER(region)                                           AS region,

    user_id IS NULL                                         AS is_anonymous,
    device_id = 'bot-automation-001'                        AS is_bot,
    event_type = 'automated_test_run'                       AS is_test_event,
    extracted_at,
    loaded_at
FROM deduplicated
