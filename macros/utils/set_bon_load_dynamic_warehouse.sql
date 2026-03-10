{#
-----------------------------------------------------------------------------------
 Macro: set_bon_load_dynamic_warehouse

 Purpose:
 Dynamically selects a Snowflake warehouse based on the number of distinct
 calendar days (`kal_tag_id`) in a source table, to optimize compute cost.

 Usage:
 This macro is intended to be used in a `pre_hook` of a dbt model. It determines
 the appropriate warehouse size by:
 - Counting rows from a metadata table (full-refresh) or from the latest changes
   (incremental mode).
 - Mapping the count to a warehouse size using predefined thresholds.
 - Constructing and executing a `USE WAREHOUSE` statement.

 Parameters:
 - env (str): Environment identifier, e.g. 'DEV', 'TEST', 'INT', 'PROD'
 - thresholds (list of tuples): List of thresholds to determine warehouse size.
     Example: [(160, 16), (120, 8), (90, 4), (30, 2)]
 - warehouse_base (dict): Mapping of environments to warehouse name prefixes.
     Example: { 'DEV': 'MY_DEV_WH', 'PROD': 'MY_PROD_WH' }
 - bon_meta_table (optional str): Name of the table to use for row counting.
     Allowed values must be defined in the `allowed_tables` list inside the macro.

 Returns:
 - A `USE WAREHOUSE` SQL statement during dbt run.
 - A dummy SQL statement during dbt compile (to prevent execution errors).

 Example usage in a model config:
 {{ config(
     pre_hook=[
         "{{ set_bon_load_dynamic_warehouse(
             env_var('DBT_ENV_TYPE', 'DEV'),
             [(160, 16), (120, 8), (90, 4), (30, 2)],
             {
               'DEV': 'PRODUCT_CO_DEV_CUSTOM_X',
               'TEST': 'PRODUCT_CO_DEV_CUSTOM_ETL_TEC_X',
               'INT': 'PRODUCT_CO_INT_CUSTOM_REBI_ETL_TEC_X',
               'PROD': 'PRODUCT_CO_PROD_CUSTOM_REBI_ETL_TEC_X'
             },
             'm_co_bon_position_daily_total_sum'
         ) }}"
     ]
 ) }}

-----------------------------------------------------------------------------------
#}

{% macro set_bon_load_dynamic_warehouse(env, thresholds, warehouse_base, bon_meta_table) %}
    {% if execute %}

        {# Define allowed table names for validation #}
        {% set allowed_tables = ['m_co_bon_position_daily_total_sum', 'm_co_bon_kopf_daily_total_sum'] %}

        {# Validate the table name if provided #}
        {% if bon_meta_table is not none and bon_meta_table not in allowed_tables %}
            {% do log("ERROR: Invalid table '" ~ bon_meta_table ~ "'. Allowed: " ~ allowed_tables | join(', '), info=True) %}
            select 1/0 as error; -- force failure on invalid input
        {% endif %}


        {# Build the SQL query for counting days #}
        {% if is_incremental() and bon_meta_table is not none %}
            -- use macro get_static_table to avoid a dbt cycle  
            {% set table_ref = get_static_table(bon_meta_table, 'edw') %}

            {% set query %}
                with data_change_date as (
                    select kal_tag_id
                    from {{ table_ref }}
                    where dss_last_modified = (select max(dss_last_modified) from {{ table_ref }})
                )

                select count(*) as cnt from data_change_date
            {% endset %}
        {% else %}
            {% set query %}
                with date_range as (
                    select
                        load_from_date::date as from_date,
                        load_to_date::date as to_date
                    from {{ ref('seed_co_bon_tables_load_date_config') }}
                    limit 1
                ),
                data_change_date as (
                    select k.kal_tag_id
                    from {{ ref('lu_d_kal_tag') }} k
                    cross join date_range
                    where k.kal_tag_id between date_range.from_date and date_range.to_date
                )
                select count(*) as cnt from data_change_date
            {% endset %}
        {% endif %}

        {# Execute the query and get the count #}
        {% set result = run_query(query) %}
        {% set count = result.columns[0].values()[0] | int %}

        {# Initialize mutable config object to store final size #}
        {% set wh_config = {'size': 1} %}

        {# Loop through thresholds to determine warehouse size #}
        {% for threshold, s in thresholds %}
            {% do log("Evaluating threshold: " ~ threshold ~ " with size: " ~ s ~ " against count: " ~ count, info=True) %}
            {% if count >= threshold %}
                {% set _ = wh_config.update({'size': s}) %}
                {% do log("Selected size = " ~ s ~ " for threshold " ~ threshold, info=True) %}
                {% break %}
            {% endif %}
        {% endfor %}

        {# Construct the final warehouse name and log it #}
        {% set wh = warehouse_base[env] ~ wh_config['size'] %}
        {% do log("Selected warehouse: " ~ wh ~ " based on kal_tag_count: " ~ count, info=True) %}

        use warehouse {{ wh }};

    {% else %}
        {# Fallback for compile-only mode (e.g. dbt compile) #}
        select 'compile only' as dummy;
    {% endif %}
{% endmacro %}
