{% materialization dummy, adapter='snowflake' %}

    {% set original_query_tag = set_query_tag() %}

    {%- set identifier = model['alias'] -%}

    --recording of all information about the desired objects and their relationships in the snowflake database
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database, type='table') -%}

   --run pre_hooks if needed
    {{ run_hooks(pre_hooks) }}

        {{ log('dummy mat for: ' ~ this) }}

        {%- call statement('main') -%}
            {{sql}} limit 1
        {%- endcall -%}   

    --run post_hooks if needed
    {{ run_hooks(post_hooks) }}        

    {% do persist_docs(target_relation, model) %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}