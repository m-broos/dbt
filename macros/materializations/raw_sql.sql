
{% materialization raw_sql, default -%}
  
  --run pre_hooks if needed outside transaction
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  --run pre_hooks if needed inside transaction
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  
 --write logging informations for executet object
  {{ log('execute custom raw_sql query for model: ' ~ this , info=True) }}

  --execute main sql query
  {% call statement("main") %}
      {{ sql }}
  {% endcall %}

  --call of the logging macro to output the query_id in the logs
  {{ log('execute custom raw_sql query_id: ' ~ this , info=True) }}
  {{ snowflake_logging_query_id() }}
  
  --run pre_hooks if needed inside transaction
  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}
  
  --run pre_hooks if needed outside transaction
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': []}) }}

{%- endmaterialization %}