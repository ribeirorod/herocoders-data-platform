{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
      Override dbt's default schema naming which concatenates the profile schema
      with the custom schema (e.g. STAGING + staging = STAGING_STAGING).
      This macro uses the custom schema name directly when provided.
    #}
    {%- if custom_schema_name is none -%}
        {{ default_schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
