{#
Macro Name: update_h_co_datatransfer_status

Description:
    Updates the table dma.h_co_datatransfer_status.
    If a matching record exists, which is determined by the primary key (object, layer, source_system, version), an update statement gets executed.
    If no matching record exists, an insert statement gets executed.

    Usually, this macro should be part of the post-hook in the last model of a specific pipeline.

Arguments:
    object (string)             : Short business description of the data.
    layer (string)              : Data layer (e.g., 'DMA').
    source_system (string)      : Source system name (e.g., 'TM1', 'P60').
    version (string)            : Data version (e.g., 'IST', 'PLAN').
    type_of_data (string)       : Type of data (e.g., 'Fact data').
    scheduled_time (string)     : Scheduled time of the pipeline.
    extra_text (string)         : Additional business description of the data.
    datatransfer_text (string)  : Further information about status of data (e.g. 'Load contains data of previous day.').

Returns:
    SQL statement: Merge statement for the dma.h_co_datatransfer_status.

Example:

    post_hook="{{update_h_co_datatransfer_status(
        object='Administrative planning data',
        layer='DMA',
        source_system='TM1',
        version='PLAN',
        type_of_data='Fact data',
        scheduled_time='Every Thursday at 3 am',
        extra_text='Fact data for KPIs of administrative planning (Talk Umlage)',
        datatransfer_text='Data for current and previous year.'
    )}}"
#}

{% macro update_h_co_datatransfer_status(
    object,
    layer,
    source_system,
    version,
    type_of_data,
    scheduled_time,
    extra_text,
    datatransfer_text
) %}
    {%- set statement -%}
    merge into dma.h_co_datatransfer_status t
    using (
        select
            '{{object}}' as object,
            '{{layer}}' as layer,
            '{{source_system}}' as source_system,
            '{{version}}' as version,
            '{{type_of_data}}' as type_of_data,
            '{{scheduled_time}}' as scheduled_time,
            '{{extra_text}}' as extra_text,
            '{{datatransfer_text}}' as datatransfer_text
    ) s
    on t.object = s.object
    and t.layer = s.layer
    and t.source_system = s.source_system
    and t.version = s.version

    when matched then update
        set t.type_of_data = s.type_of_data
        ,t.scheduled_time = s.scheduled_time
        ,t.extra_text = s.extra_text
        ,t.datatransfer_text = s.datatransfer_text
        ,t.datatransfer_ts = current_timestamp(0)
        ,t.run_0 = current_timestamp(0)
        ,t.run_1 = t.run_0
        ,t.run_2 = t.run_1
        ,t.run_3 = t.run_2
        ,t.run_4 = t.run_3
        ,t.run_5 = t.run_4

    when not matched then insert (
        object,
        layer,
        source_system,
        version,
        type_of_data,
        scheduled_time,
        extra_text,
        datatransfer_text,
        datatransfer_ts,
        run_0
        )
    values (
        s.object,
        s.layer,
        s.source_system,
        s.version,
        s.type_of_data,
        s.scheduled_time,
        s.extra_text,
        s.datatransfer_text,
        current_timestamp(0),
        current_timestamp(0)
        )
    {%- endset -%}

    {{- statement -}}
{% endmacro %}