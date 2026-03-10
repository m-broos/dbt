{#
Macro Name: join_metadata_table

Description:
    Checks a staging table for entries from new files, i.e. files that are not listed in the metadata table yet.
    If there are new files, return a new lfd_nr_load and a new lfd_nr_rohdat for each new file.
    If the metadata table is empty or does not exist yet, all files are considered new. Set lfd_nr_load = 1.
    
    The output can be used for an inner join to exclude entries from old files. Set include_old_files=true to add old files to the output.

Arguments:
    staging_table (string)      : Name of the staging table.
    metadata_table (string)     : Name of the metadata table.
    metadata_schema (string)    : Schema of the metadata table (default: 'edw').
    include_old_files (boolean) : If true, includes already processed files (default: false). 

Returns:
    SQL statement: Result set with dss_file_name, lfd_nr_load, and lfd_nr_rohdat.

Example:

with meta(
    {{ join_metadata_table('stg_table', 'metadata_table') }}
    )
    select ...
    from staging_table stg
    join meta
    on stg.dss_file_name = meta.dss_file_name
#}

{% macro join_metadata_table(staging_table, metadata_table, metadata_schema='edw', include_old_files=false) %}

    {%- set metadata_table_fqn=get_static_table(metadata_table, metadata_schema) -%}
    {%- set database, schema, identifier = metadata_table_fqn.split('.') -%}

    {%- set statement -%}
        {%- if adapter.get_relation(database=database, schema=schema, identifier=identifier) is none -%}
            select
                dss_file_name,
                1::number(38, 0) as lfd_nr_load,
                row_number() over (order by dss_last_modified)::number(38,0) as lfd_nr_rohdat
            from (
                select distinct
                    dss_file_name,
                    dss_last_modified
                from {{staging_table}}
            )
        {%- else -%}
            {%- if include_old_files -%}
                select distinct 
                    stg.dss_file_name,
                    m.lfd_nr_load,
                    m.lfd_nr_rohdat
                from {{staging_table}} stg
                inner join {{metadata_table_fqn}} m
                on stg.dss_file_name = m.dss_file_name
                
                union all

            {% endif -%}
            select
                stg.dss_file_name,
                m.max_lfd_nr_load + 1 as lfd_nr_load,
                m.max_lfd_nr_rohdat + row_number() over (order by stg.dss_last_modified) as lfd_nr_rohdat
            from (
                select distinct
                    dss_file_name,
                    dss_last_modified
                from {{staging_table}}
            ) stg
            cross join (
                select
                    max(lfd_nr_load) as max_lfd_nr_load,
                    max(lfd_nr_rohdat) as max_lfd_nr_rohdat
                from (
                    select
                        lfd_nr_load,
                        lfd_nr_rohdat
                    from {{metadata_table_fqn}}
                    union all
                    select
                        0 as lfd_nr_load,
                        0 as lfd_nr_rohdat
                )
            ) m
            left join {{metadata_table_fqn}} meta
                on stg.dss_file_name = meta.dss_file_name

            where meta.dss_file_name is null
        {%- endif -%}
    {%- endset -%}

    {{- statement -}}
{% endmacro %}