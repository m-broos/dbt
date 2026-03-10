{% materialization scd1_cdc_hard_delete_strict, adapter='snowflake' -%}

  {# ------------------------------
     Required config
     ------------------------------ #}
  {% set unique_key = config.require('unique_key') -%}
  {% set key_columns = (
      unique_key if unique_key is iterable and unique_key is not string
      else [unique_key]
  ) -%}

  {# Custom config via meta (avoids dbt warnings) #}
  {% set meta_cfg = config.get('meta', {}) -%}
  {% set cdc_flag_column = meta_cfg.get('cdc_flag_column', 'GL_CDC_OPERATION_FLAG') -%}
  {% set source_model_name = meta_cfg.get('source_model', none) -%}

  {% if source_model_name is none -%}
    {{ exceptions.raise_compiler_error(
      "scd1_cdc_hard_delete_strict requires meta.source_model (string). "
      ~ "Example: meta={'source_model':'stag_z_glue_t001','cdc_flag_column':'gldelflag'}"
    ) }}
  {% endif -%}

  {# Optional exclude_columns #}
  {% set exclude_columns = config.get('exclude_columns', default=[]) -%}
  {% set exclude_columns = (
      exclude_columns if exclude_columns is iterable and exclude_columns is not string
      else [exclude_columns]
  ) -%}

  {{ run_hooks(pre_hooks) }}

  {# ------------------------------
     Normalize delta: trim + empty/null -> 'I'
     ------------------------------ #}
  {% set normalized_delta_sql -%}
WITH DELTA_RAW AS (
  {{ sql }}
),
DELTA AS (
  SELECT
    *,
    COALESCE(NULLIF(TRIM({{ cdc_flag_column }}), ''), 'I') AS CDC_OP_NORM
  FROM DELTA_RAW
)
SELECT * FROM DELTA
  {%- endset -%}

  {# ------------------------------
     Auto-derive columns from source model relation metadata
     (no temp objects, no drop)
     ------------------------------ #}
  {% set source_relation = ref(source_model_name) -%}
  {% set source_cols = adapter.get_columns_in_relation(source_relation) -%}

  {% if source_cols is none or (source_cols | length) == 0 -%}
    {{ exceptions.raise_compiler_error(
      "Could not read columns from meta.source_model='" ~ source_model_name ~ "'. "
      ~ "Ensure the model exists and privileges allow DESCRIBE/SHOW columns."
    ) }}
  {% endif -%}

  {% set all_col_names = [] -%}
  {% for col in source_cols -%}
    {% do all_col_names.append(col.name) -%}
  {% endfor -%}

  {# Exclusions: keys + cdc flag + helper col + optional excludes #}
  {% set exclude_auto = key_columns + [cdc_flag_column, 'CDC_OP_NORM'] + exclude_columns -%}

  {% set data_columns = [] -%}
  {% for c in all_col_names -%}
    {% if (c | upper) not in (exclude_auto | map('upper') | list) -%}
      {% do data_columns.append(c) -%}
    {% endif -%}
  {% endfor -%}

  {% if (data_columns | length) == 0 -%}
    {{ exceptions.raise_compiler_error(
      "Derived data_columns is empty. Check unique_key/cdc_flag_column/exclude_columns vs columns in " ~ source_model_name
    ) }}
  {% endif -%}

  {# ------------------------------
     Target existence
     ------------------------------ #}
  {% set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) -%}

  {# ------------------------------
     Full refresh: CREATE OR REPLACE (no DROP/CASCADE)
     ------------------------------ #}
  {% if should_full_refresh() -%}
    {% call statement('main', fetch_result=False) -%}
CREATE OR REPLACE TABLE {{ this }} AS
SELECT
  {% for c in key_columns + data_columns -%}
    {{ c }}{% if not loop.last %}, {% endif %}
  {%- endfor -%}
  , CDC_OP_NORM AS {{ cdc_flag_column }}
FROM (
  {{ normalized_delta_sql }}
)
WHERE CDC_OP_NORM = 'I';
    {%- endcall %}

    {{ run_hooks(post_hooks) }}
    {{ return({'relations': [this]}) }}
  {% endif -%}

  {# ------------------------------
     First run: create table from inserts only
     ------------------------------ #}
  {% if not target_relation -%}

    {% call statement('main', fetch_result=False) -%}
CREATE TABLE {{ this }} AS
SELECT
  {% for c in key_columns + data_columns -%}
    {{ c }}{% if not loop.last %}, {% endif %}
  {%- endfor -%}
  , CDC_OP_NORM AS {{ cdc_flag_column }}
FROM (
  {{ normalized_delta_sql }}
)
WHERE CDC_OP_NORM = 'I';
    {%- endcall %}

  {% elif flags.EMPTY -%}

    {{ log("No data → skipping merge", info=True) }}
    {% call statement('main', fetch_result=False) -%}
SELECT 1;
    {%- endcall %}

  {% else -%}

    {% call statement('main', fetch_result=False) -%}
MERGE INTO {{ this }} AS BASE_TABLE
USING (
  {{ normalized_delta_sql }}
) AS DELTA_TABLE
ON
  {% for col in key_columns -%}
    BASE_TABLE.{{ col }} = DELTA_TABLE.{{ col }}{% if not loop.last %} AND {% endif %}
  {%- endfor %}

WHEN NOT MATCHED AND DELTA_TABLE.CDC_OP_NORM = 'I'
THEN INSERT (
  {% for c in key_columns + data_columns -%}
    {{ c }}{% if not loop.last %}, {% endif %}
  {%- endfor -%}
  , {{ cdc_flag_column }}
)
VALUES (
  {% for c in key_columns + data_columns -%}
    DELTA_TABLE.{{ c }}{% if not loop.last %}, {% endif %}
  {%- endfor -%}
  , DELTA_TABLE.CDC_OP_NORM
)

WHEN MATCHED AND DELTA_TABLE.CDC_OP_NORM = 'D'
THEN DELETE

WHEN MATCHED AND DELTA_TABLE.CDC_OP_NORM = 'U'
THEN UPDATE SET
  {% for c in data_columns -%}
    BASE_TABLE.{{ c }} = DELTA_TABLE.{{ c }}{% if not loop.last %}, {% endif %}
  {%- endfor -%}
  , BASE_TABLE.{{ cdc_flag_column }} = DELTA_TABLE.CDC_OP_NORM
;
    {%- endcall %}

  {% endif -%}

  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [this]}) }}

{%- endmaterialization %}
