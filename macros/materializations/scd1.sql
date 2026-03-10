{% materialization scd1, adapter='default' %}

    {%- set debug = False -%}

    {# --- Configuration --- #}
    {%- set unique_key       = config.require('unique_key') -%}
    {%- set key_columns      = (unique_key if unique_key is iterable and unique_key is not string else [unique_key]) | map('lower') | list -%}
     
    {%- set soft_delete      = config.get('soft_delete', default=true) -%}
    {%- set hard_delete      = config.get('hard_delete', default=false) -%}
    {%- set im_cdc_ts        = config.get('im_cdc_ts', default='current_timestamp::timestamp_ltz') -%}
    {%- set im_cdc_flag      = config.get('im_cdc_flag', default='\'I\'') -%}
    {%- set im_record_source = config.get('im_record_source', default='\'' ~ this ~ '\'') -%}

    {# --- Validation of configuration --- #}
    {%- if soft_delete and hard_delete -%}
        {%- do exceptions.raise_compiler_error("Configuration error: 'soft_delete' and 'hard_delete' cannot both be true.") -%}
    {%- endif -%}

    {# --- Execute any pre-hooks --- #}
    {{- run_hooks(pre_hooks) -}}

    {# --- Check if target relation exists; Return: https://docs.getdbt.com/reference/dbt-classes#relation --- #}
    {%- set target_relation = adapter.get_relation(
        database=this.database,
        schema=this.schema,
        identifier=this.identifier
    ) -%}
    {# --- Disable Quoting --- #}
    {%- if target_relation -%}
        {%- set target_relation = target_relation.incorporate(quote_policy={'database': false, 'schema': false, 'identifier': false}) -%}
    {%- endif -%}

    {# --- Create temporary staging relation; Return: https://docs.getdbt.com/reference/dbt-classes#relation --- #}
    {%- set intermediate_relation = api.Relation.create(
        schema=this.schema,
        database=this.database,
        identifier=this.identifier ~ '__dbt_tmp',
        type='view'
    ) -%}

    {# --- Generate column information based on the source table --- #}
    {%- set column_dict = get_column_names_with_datatypes_from_sql(sql, debug) -%}
    {%- set column_names = column_dict.keys() | list -%}
    {{- log('Preview Columns: ' ~ column_names, info=debug) -}}
    {%- set hashable_columns = get_hash_columns(column_names, key_columns) -%}
    {{- log('Hashable Columns: ' ~ hashable_columns, info=debug) -}}

    {# --- Handle full refresh if needed --- #}
    {%- if should_full_refresh() and target_relation -%}
        
        {{- log("Full-Refresh! – " ~ target_relation ~ " is being deleted.", info=debug) -}}
        {{- adapter.drop_relation(target_relation) -}}
        {%- set target_relation = none -%}
    {%- endif -%}
    
    {# --- Create staging table from source data with metadata columns --- #}
    {%- call statement('main', fetch_result=False) -%}
        CREATE TEMPORARY VIEW {{ intermediate_relation | lower }} AS
        SELECT
            -- key columns
            {{ comma_seperated_column_select(key_columns, indent=12, datatypes=column_dict) }},
            {% if hashable_columns -%}
                -- hashable columns
                {{ comma_seperated_column_select(hashable_columns, indent=16, datatypes=column_dict) }},
                -- change_hash and metadata columns
                {{ 'hash(' ~ (hashable_columns | join(', ') | lower ) ~ ')' }} AS im_change_hash,
            {%- else -%}
                -- default change_hash because no hashable columns
                {{ 'hash(true)' }} AS im_change_hash,
            {%- endif %}
            -- metadata columns
            {{ im_record_source }}::varchar AS im_record_source,
            {{ im_cdc_flag }}::varchar(1) AS im_cdc_flag,
            '{{ invocation_id }}'::varchar AS im_invocation_id,
            {{ im_cdc_ts }} AS im_cdc_ts
        FROM (
            {{ sql | indent(14) }}
        )
    {%- endcall -%}

    {%- if not target_relation -%}
        
        {{- log("Target table " ~ this | lower ~ " is being created.", info=debug) -}}
        
        {# --- Initial creation of target table with metadata columns --- #}
        {%- call statement('main', fetch_result=False) -%}
            CREATE TABLE {{ this | lower }} 
            AS
            SELECT * 
            FROM {{ intermediate_relation | lower }}
        {%- endcall -%}

    {% elif flags.EMPTY %}
        
        {{- log("Target table " ~ target_relation | lower ~ " exists, but no merge because empty load!", info=true) -}}

    {%- else -%}

        {{- log("Target table " ~ target_relation | lower ~ " exists! Starting merge.", info=debug) -}}

        {# --- Merge: Handles inserts, updates, and soft deletes --- #}
        {%- call statement('merge', fetch_result=True) -%}
            MERGE
            INTO    {{ target_relation | lower }} AS target
            USING   (
                           -- New Records and Changed Records from transformation
                           SELECT     1 AS source_indicator,
                                      src.*
                           FROM       {{ intermediate_relation | lower }} AS src
                           UNION ALL BY NAME
                           -- Deleted Records (soft delete)
                           SELECT     0 AS source_indicator,
                                      tgt.* exclude (im_cdc_ts, im_invocation_id),
                                      current_timestamp::timestamp_ltz AS im_cdc_ts,
                                      '{{ invocation_id }}' AS im_invocation_id
                           FROM       {{ target_relation | lower }} AS tgt
                           LEFT JOIN  {{ intermediate_relation | lower }} AS src
                           ON         -- key_columns match
                                      {% for key in key_columns -%}
                                          tgt.{{ key }} = src.{{ key }}
                                          {%- if not loop.last %}{{ '\nAND ' | indent(34) }}{%- endif -%}
                                      {% endfor %}
                           WHERE      -- key_columns in source are null
                                      {% for key in key_columns -%}
                                          src.{{ key }} IS NULL
                                          {%- if not loop.last %}{{ '\nAND ' | indent(34) }}{%- endif -%}
                                      {% endfor %}
                    ) AS source
            ON      {% for key in key_columns -%}
                        target.{{ key }} = source.{{ key }}
                        {%- if not loop.last %}{{ '\nAND ' | indent(16) }}{%- endif -%}
                    {% endfor %}

            /* -- Insert new records -- */
            WHEN NOT MATCHED THEN
            INSERT 
            (
                -- key_columns
                {{ comma_seperated_column_select(key_columns, indent=16) }},
                -- hashable columns
                {{ comma_seperated_column_select(hashable_columns, indent=16) ~ ',' if hashable_columns else '' }}
                -- change_hash and metadata columns
                im_change_hash,
                im_record_source,
                im_cdc_flag,
                im_invocation_id,
                im_cdc_ts

            ) VALUES (
                -- key_columns
                {{ comma_seperated_column_select(key_columns, prefix='source', indent=16) }},
                -- hashable columns
                {{ comma_seperated_column_select(hashable_columns, prefix='source', indent=16) ~ ',' if hashable_columns else '' }}
                -- change_hash and metadata columns
                source.im_change_hash,
                source.im_record_source,
                source.im_cdc_flag,
                source.im_invocation_id,
                source.im_cdc_ts
            ) 

            {% if hashable_columns %}
                /* -- Update changed and reopen records -- */
                WHEN    MATCHED
                AND     source.source_indicator = 1
                AND     (  target.im_change_hash <> source.im_change_hash 
                        OR target.im_cdc_flag = 'D' ) THEN
                UPDATE
                SET     -- hashable_columns
                        {{ comma_seperated_pattern_select(hashable_columns, pattern='<col> = source.<col>', indent=20) }},
                        -- change_hash and metadata columns
                        im_change_hash = source.im_change_hash,
                        im_cdc_flag = 'U',
                        im_invocation_id = source.im_invocation_id,
                        im_cdc_ts = source.im_cdc_ts
            {% endif %}


            {% if soft_delete %}
                /* -- Mark records as deleted (soft delete) -- */
                WHEN    MATCHED
                AND     source.source_indicator = 0 THEN
                UPDATE
                SET     im_cdc_flag = 'D',
                        im_invocation_id = source.im_invocation_id,
                        im_cdc_ts = source.im_cdc_ts

            {% elif hard_delete %}
                /* -- Delete records (hard delete) -- */
                WHEN    MATCHED
                AND     source.source_indicator = 0 THEN
                DELETE

            {%- endif -%}
        {%- endcall -%}
    {%- endif -%}

    {# --- Execute any post-hooks --- #}
    {{- run_hooks(post_hooks) -}}

    {{- return({'relations': [this]}) -}}

{% endmaterialization %}
 