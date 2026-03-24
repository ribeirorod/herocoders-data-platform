{% macro legal_suffix_pattern() %}
{#-
  Builds a regex alternation pattern from the dim_legal_suffixes seed.
  Suffixes ordered by LENGTH DESC so longer variants match before shorter ones
  (e.g. "limited liability company" before "limited").
  run_query() resolves the list at compile time — Snowflake requires the regex
  pattern to be a string literal, not a runtime subquery.
-#}

{%- if execute -%}
    {%- set query -%}
        SELECT suffix
        FROM {{ ref('dim_legal_suffixes') }}
        ORDER BY LENGTH(suffix) DESC
    {%- endset -%}

    {%- set results = run_query(query) -%}
    {%- set suffixes = results.columns[0].values() -%}

    \\s+({{ suffixes | join("|") }})\\.?\\s*$

{%- endif -%}
{% endmacro %}


{% macro normalize_company_key(company_col, country_col) %}
{#-
  Stable company_key for cross-source joins (marketplace ↔ Amplitude):
  lowercase → strip legal suffix → collapse non-alphanumeric → append country
  ("acme|de" ≠ "acme|us" — avoids cross-country collisions).
-#}
TRIM(
    REGEXP_REPLACE(
        TRIM(
            REGEXP_REPLACE(
                LOWER(TRIM({{ company_col }})),
                '{{ legal_suffix_pattern() | trim }}',
                '',
                1, 0, 'i'
            )
        ),
        '[^a-z0-9]+', '_'
    ), '_'
) || '|' || LOWER(COALESCE({{ country_col }}, ''))
{% endmacro %}
