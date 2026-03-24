-- depends_on: {{ ref('dim_legal_suffixes') }}
-- Grain: company_key + product + event_type + month
-- Bot and test events excluded here — first point where product analytics intent justifies filtering.

WITH events_with_iso AS (
    SELECT
        e.*,
        LOWER(COALESCE(c.iso_code, LOWER(TRIM(e.country))))            AS country_iso
    FROM {{ ref('stg_amplitude_events') }} e
    LEFT JOIN {{ ref('dim_country_codes') }} c
        ON LOWER(TRIM(e.country)) = LOWER(TRIM(c.country_name))
    WHERE NOT e.is_bot
      AND NOT e.is_test_event
),

events_keyed AS (
    SELECT
        event_id,
        event_type,
        event_time,
        DATE_TRUNC('month', event_time)                                AS event_month,
        user_id,
        device_id,
        session_id,
        product,
        user_property_company,
        user_property_plan,
        user_property_tier,
        country_iso                                                    AS country,
        is_anonymous,
        CASE
            WHEN user_property_company IS NULL THEN NULL
            ELSE {{ normalize_company_key('user_property_company', 'country_iso') }}
        END                                                            AS company_key
    FROM events_with_iso
),

joined AS (
    SELECT
        ev.event_id,
        ev.event_type,
        ev.event_time,
        ev.event_month,
        ev.user_id,
        ev.device_id,
        ev.session_id,
        ev.product,
        ev.company_key,
        ev.user_property_company,
        ev.user_property_plan,
        ev.user_property_tier,
        ev.country,
        ev.is_anonymous,
        c.instance_id,
        c.instance_id IS NOT NULL                                      AS is_matched -- amplitude event's company_key successfully matched to int_customers
    FROM events_keyed ev
    LEFT JOIN {{ ref('int_customers') }} c
        USING (company_key)
)

SELECT
    event_month,
    product,
    event_type,
    company_key,
    instance_id,
    is_matched,
    COUNT(*)                                                            AS event_count,
    COUNT(DISTINCT user_id)                                            AS unique_users,
    COUNT(DISTINCT session_id)                                         AS unique_sessions,
    COUNT(DISTINCT CASE WHEN is_anonymous THEN device_id END)          AS anonymous_device_count
FROM joined
GROUP BY 1, 2, 3, 4, 5, 6
