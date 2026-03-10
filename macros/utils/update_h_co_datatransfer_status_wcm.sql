{#
Macro Name: update_h_co_datatransfer_status_wcm

Description:
    Operation macro to update the table dma.h_co_datatransfer_status_wcm without requiring a dbt model.
    This macro can be executed independently using 'dbt run-operation' command.
    
    The macro performs the merge statement to update dma.h_co_datatransfer_status_wcm with inline helper query.
    The datatransfer_status_macro parameter provides the SQL expression used for calculating the actual 
    datatransfer_status value from the source table.
    
    This is useful when you want to update the status table without creating an actual dbt model,
    or when you need to update multiple source tables in a batch operation.

    Furthermore there is a View WCM_CO.ZCD_DATENSTAND_WCM depicting all column names in german.

Arguments:
    source_schema (string)                    : Source schema name (e.g., 'dma', 'edw')
    source_table (string)                     : Source table name (e.g., 's_wcm_sap_zahlungsziel')
    datatransfer_status_macro (string)        : SQL expression for calculating datatransfer_status (e.g., 'max(tsa_datum)')
    report (string)                           : Report name
    report_content (string)                   : Report description
    view (string)                             : View/Table name
    view_content (string)                     : View description
    layer (string)                            : Data layer (e.g., 'DMA')
    source_system (string)                    : Source system (e.g., 'SAP')
    source_product (string)                   : Source product (e.g., 'Corporate Controlling & Planning')
    version (string)                          : Version (e.g., 'IST')
    data_type (string)                        : Data type
    load_cycle (string)                       : Loading cycle
    datatransfer_status_calculation (string)  : Description of data transfer status calculation (e.g., 'MAX(tsa_datum)')
    datatransfer_status_text (string)         : Status description

Returns:
    Executes SQL statements directly (no return value)

Example Usage:
    dbt run-operation update_h_co_datatransfer_status_wcm --args '{
        "source_schema": "dma",
        "source_table": "s_wcm_sap_zahlungsziel",
        "datatransfer_status_macro": "max(tsa_datum)",
        "report": "Zahlungsziele",
        "report_content": "Ermittlung der effektiven Zahlungsziele je Lieferant, Konditionssatz und RZF-Beleg, Analyse der operativen Kapitalbindung in Tagen",
        "view": "FV_SAP_ZAHLUNGSZIEL",
        "view_content": "FI-Belege mit einem Ausgleichsdatum aus dem BUK 0401",
        "layer": "EDW and DMA",
        "source_system": "P02",
        "source_product": "Supplier Billing",
        "version": "IST",
        "data_type": "Bewegungsdaten",
        "load_cycle": "täglich",
        "datatransfer_status_calculation": "MAX(tsa_datum)",
        "datatransfer_status_text": "Full-Load oder Delta-Load, Zeitpunkt letztes TSA-Datum in der Datenlieferung"
    }'

Note:
    - This operation requires the source table to exist in the specified schema
    - The datatransfer_status_macro is used for actual SQL calculation
    - The datatransfer_status_calculation is stored as descriptive text
    - This bypasses the normal dbt model lifecycle
#}

{% macro update_h_co_datatransfer_status_wcm(
    source_schema,
    source_table,
    datatransfer_status_macro,
    report,
    report_content,
    view,
    view_content,
    layer,
    source_system,
    source_product,
    version,
    data_type,
    load_cycle,
    datatransfer_status_calculation,
    datatransfer_status_text
) %}
    
    {{ log("Starting WCM data transfer status update operation for: " ~ view, info=True) }}

    
    --Execute the merge statement with helper table in from statement
    {%- set merge_sql -%}
        merge into dma.h_co_datatransfer_status_wcm t
        using (
            select
                '{{report}}' as report,
                '{{report_content}}' as report_content,
                '{{view}}' as view,
                '{{view_content}}' as view_content,
                '{{layer}}' as layer,
                '{{source_system}}' as source_system,
                '{{source_product}}' as source_product,
                '{{version}}' as version,
                '{{data_type}}' as data_type,
                '{{load_cycle}}' as load_cycle,
                h.datatransfer_status,
                '{{datatransfer_status_calculation}}' as datatransfer_status_calculation,
                '{{datatransfer_status_text}}' as datatransfer_status_text,
                h.count_records as count_records
            from (  select
                    ({{datatransfer_status_macro}})::date as datatransfer_status,
                    count(*) as count_records
                    from {{source_schema}}.{{source_table}}) h
        ) s
        on t.report = s.report
        and t.view = s.view
        and t.layer = s.layer
        and t.source_system = s.source_system
        and t.version = s.version

        when matched then update
            set t.report_content = s.report_content
            ,t.view_content = s.view_content
            ,t.source_product = s.source_product
            ,t.data_type = s.data_type
            ,t.load_cycle = s.load_cycle
            ,t.datatransfer_status = s.datatransfer_status
            ,t.datatransfer_status_calculation = s.datatransfer_status_calculation
            ,t.datatransfer_status_text = s.datatransfer_status_text
            ,t.run_0 = current_timestamp(0)
            ,t.count_records_run_0 = s.count_records
            ,t.run_1 = t.run_0
            ,t.count_records_run_1 = t.count_records_run_0
            ,t.run_2 = t.run_1
            ,t.count_records_run_2 = t.count_records_run_1
            ,t.run_3 = t.run_2
            ,t.count_records_run_3 = t.count_records_run_2
            ,t.run_4 = t.run_3
            ,t.count_records_run_4 = t.count_records_run_3
            ,t.run_5 = t.run_4
            ,t.count_records_run_5 = t.count_records_run_4

        when not matched then insert (
            report, report_content, view, view_content, layer, source_system, source_product, version,
            data_type, load_cycle, datatransfer_status, datatransfer_status_calculation, datatransfer_status_text,
            run_0, count_records_run_0
            )
        values (
            s.report, s.report_content, s.view, s.view_content, s.layer, s.source_system, s.source_product, s.version,
            s.data_type, s.load_cycle, s.datatransfer_status, s.datatransfer_status_calculation, s.datatransfer_status_text,
            current_timestamp(0), s.count_records
            )
    {%- endset -%}
    
    {{ log("Executing merge statement for dma.h_co_datatransfer_status_wcm", info=True) }}
    {% do run_query(merge_sql) %}
    
    {{ log("WCM data transfer status update completed successfully for: " ~ view, info=True) }}
    
{% endmacro %}