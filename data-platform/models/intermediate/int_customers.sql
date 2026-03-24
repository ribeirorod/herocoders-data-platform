-- depends_on: {{ ref('dim_legal_suffixes') }}
-- ---------------------------------------------------------------------------
-- int_customers
-- Grain: one row per customer (host_license_id)
-- Purpose: central customer hub for all downstream joins
--
-- Assumption: host_license_id is the Atlassian instance identifier across all
-- hosting types (Cloud, Server, DC). One customer can hold multiple products.
--
-- company_key is derived via the normalize_company_key() macro.
-- Country is resolved to ISO 2-letter code via dim_country_codes before the
-- key is built — case-insensitive join, falls back to raw country if unmatched.
-- ---------------------------------------------------------------------------

WITH licenses_with_iso AS (
    SELECT
        l.*,
        LOWER(COALESCE(c.iso_code, LOWER(TRIM(l.country))))            AS country_iso
    FROM {{ ref('stg_marketplace_licenses') }} l
    LEFT JOIN {{ ref('dim_country_codes') }} c
        ON LOWER(TRIM(l.country)) = LOWER(TRIM(c.country_name))
),

licenses_grouped AS (
    SELECT
        host_license_id                                                 AS instance_id,
        host_license_id,
        MAX(cloud_id)                                                   AS cloud_id,
        MAX(hosting)                                                    AS hosting,
        LOWER(TRIM(MAX(company)))                                       AS company,
        MAX(country_iso)                                                AS country,
        BOOLOR_AGG(is_active)                                           AS has_active_license,
        COUNT(DISTINCT addon_key)                                       AS product_count,
        COUNT(DISTINCT CASE WHEN is_active THEN addon_key END)          AS product_count_active,
        ARRAY_AGG(DISTINCT addon_key)                                   AS products,
        MAX(last_updated)                                               AS last_updated
    FROM licenses_with_iso
    GROUP BY host_license_id
)

SELECT
    instance_id,
    cloud_id,
    host_license_id,
    hosting,
    country                                                             AS country_iso,
    {{ normalize_company_key('company', 'country') }}                   AS company_key,
    has_active_license,
    product_count,
    product_count_active,
    products,
    last_updated
FROM licenses_grouped
