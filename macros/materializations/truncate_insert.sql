{% materialization truncate_insert, adapter='snowflake' %}

    {% set original_query_tag = set_query_tag() %}

    {%- set identifier = model['alias'] -%}

    {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change')) -%}


    --Recording of all information about the desired objects and their relationships in the snowflake database.
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier, type='table') -%}

    --Run pre_hooks if needed.
    {{ run_hooks(pre_hooks) }}

    --If the table to be copied doe not exist yet or if full-refresh is required, create the table.
    --Otherwise, check if schema has changed.
    {% if old_relation is none or not old_relation.is_table or should_full_refresh() %}
        {{ log("Replacing existing relation " ~ old_relation) }}

        {%- call statement('create_table') -%}
            {{ create_table_as(false, target_relation, sql ~ '\nlimit 0') }}
        {%- endcall -%}
    {% elif on_schema_change != 'ignore'  %}
        {{ log("Creating temporary view") }}
        {%- set tmp_relation = make_temp_relation(target_relation).incorporate(type='view') -%}

        {%- call statement('create_tmp_view') -%}
          {{ snowflake__create_view_as_with_temp_flag(tmp_relation, sql, true) }}
        {%- endcall -%}

        {%- do process_schema_changes(on_schema_change, tmp_relation, target_relation) -%}
    {% endif %}

    --Execute insert statement with truncate.
    {%- call statement('main') -%}
        {% set column_list = get_columns_in_query(sql) %}
        INSERT OVERWRITE INTO {{target_relation}}
        (
            {%- for column in column_list -%}
                {{column}}{% if not loop.last %},{% endif %}
            {%- endfor -%}
        )
        {{sql}}
    {%- endcall -%}

    --Run post_hooks if needed.
    {{ run_hooks(post_hooks) }}

    {% do persist_docs(target_relation, model) %}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
