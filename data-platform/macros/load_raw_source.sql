{% macro load_raw_source(source_name, stage_name=none) %}
  {#
    Creates a raw landing table (VARIANT) and COPYs JSONL.gz files from S3.
    Envelope: {"metadata": {...}, "payload": {...}} — staging models flatten payload.
    Incremental: Snowflake load history skips already-processed files (FORCE = FALSE).
    Full-refresh: drops and recreates the raw table, reloads all files (FORCE = TRUE).
  #}

  {% set resolved_stage = stage_name if stage_name is not none else (target.database ~ '.RAW.' ~ var('s3_stage')) %}
  {% set raw_table = target.database ~ '.RAW.' ~ source_name | upper ~ '_RAW' %}

  {% if not is_incremental() %}
    DROP TABLE IF EXISTS {{ raw_table }};
  {% endif %}

  CREATE TABLE IF NOT EXISTS {{ raw_table }} (
      raw_data    VARIANT,
      file_name   VARCHAR,
      loaded_at   TIMESTAMP_LTZ DEFAULT SYSDATE()
  );

  COPY INTO {{ raw_table }} (raw_data, file_name)
  FROM (
      SELECT
          $1                  AS raw_data,
          METADATA$FILENAME   AS file_name
      FROM @{{ resolved_stage }}/raw/{{ source_name }}/
  )
  FILE_FORMAT = (
      TYPE              = 'JSON'
      COMPRESSION       = 'GZIP'
      STRIP_OUTER_ARRAY = FALSE
  )
  FORCE      = {{ 'TRUE' if not is_incremental() else 'FALSE' }}
  PURGE      = FALSE
  ON_ERROR   = 'CONTINUE';

{% endmacro %}
