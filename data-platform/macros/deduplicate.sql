{% macro deduplicate(relation, partition_by, order_by) %}
  {#
    Returns a deduplicated SELECT from relation using ROW_NUMBER() + QUALIFY.
    Keeps the most recent record per partition.

    Args:
        relation:     CTE name or table reference to deduplicate
        partition_by: column(s) defining the unique grain, e.g. 'addon_license_id'
        order_by:     column to pick the winner, e.g. 'last_updated DESC'

    Usage:
        {{ deduplicate('source', 'addon_license_id', 'last_updated DESC') }}
  #}
  SELECT *
  FROM {{ relation }}
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY {{ partition_by }}
      ORDER BY {{ order_by }}
  ) = 1
{% endmacro %}
