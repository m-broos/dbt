#tests/test_file_ingestion_process.sql

--Testing the number of expecting files with the same ctm_run_id
{% test file_load_is_complete(model, ctm_run_id, expected_count) %}
    
    WITH files_loaded AS (
        SELECT *
        FROM {{ model }}
        WHERE dss_ctm_run_id = '{{ ctm_run_id }}'
    ),
    

    validation AS (
        SELECT 
            '{{ ctm_run_id }}' AS ctm_run_id,
            COUNT(*) AS actual_count,
            CASE 
                WHEN COUNT(*) != {{ expected_count }} THEN 
                    'ctm_run_id {{ ctm_run_id }} has an incorrect number of loads: Expected {{ expected_count }}, but found ' || COUNT(*)
                ELSE NULL
            END AS error_message
        FROM files_loaded
        HAVING COUNT(*) != {{ expected_count }}
    )

    SELECT error_message
    FROM validation
{% endtest %}

--Testing the if the same files has been loaded before
{% test file_already_loaded(model, ctm_run_id) %}
    
    with search_file_name as (
        SELECT 
            dss_file_name,
        FROM 
            {{ model }}
        WHERE 
            dss_ctm_run_id = '{{ ctm_run_id }}'
            AND dss_file_name IS NOT NULL
    )
    
    
    SELECT 
        dss_file_name,
        'The file associated with ctm_run_id {{ ctm_run_id }} has already been loaded.' AS error_message
    FROM 
        {{ model }}
    WHERE 
        dss_ctm_run_id != '{{ ctm_run_id }}' and
        dss_file_name IN (SELECT dss_file_name
                        FROM search_file_name
        )
{% endtest %}