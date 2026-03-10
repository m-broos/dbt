{%- materialization stored_procedure, adapter='snowflake' -%}
  {%- set replace_procedure = config.meta_get('replace_procedure', default = false) -%}
  {%- set preferred_language = config.meta_get('preferred_language', default=SQL) -%}
  {%- set runtime_version = config.meta_get('runtime_version') -%}
  {%- set load_packages = config.meta_get('load_packages') -%}
  {%- set comment = config.meta_get('comment') -%}
  {%- set parameters = config.meta_get('parameters', default= '') -%}
  {%- set return_type = config.meta_get('return_type', default='varchar' ) -%}
  {%- set handler = config.meta_get('handler') -%}
  {%- set comment = config.meta_get('comment') -%}
  {%- set execute_as = config.meta_get('execute_as', default = 'OWNER') -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}

  {%- set has_transactional_hooks = (hooks | selectattr('transaction', 'equalto', True) | list | length) > 0 %}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- BEGIN happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

      --------------------------------------------------------------------------------------------------------------------
  -- build model
  {%- if replace_procedure -%}
  {% call statement('main') -%}
    {{ create_or_replace_stored_procedure(target_relation, preferred_language, runtime_version, load_packages, parameters, return_type, handler, comment, execute_as, sql) }}
  {%- endcall %}
  {%- else -%}
  {% call statement('main') -%}
    {{ create_if_not_exists_stored_procedure(target_relation, preferred_language, runtime_version, load_packages, parameters, return_type, handler, comment, execute_as, sql) }}
  {%- endcall %}
  {% endif %}

      --------------------------------------------------------------------------------------------------------------------
  -- build model
  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do persist_docs(target_relation, model) %}
  -- `COMMIT` happens here
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  -- return
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}