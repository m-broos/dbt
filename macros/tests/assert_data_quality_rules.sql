-- -------------------------------------------------------------------------
-- Macro Name   : assert_data_quality_rules
-- File         : macros/tests/assert_data_quality_rules.sql
-- Purpose      : Validates data quality rules and captures rule violations as errors
-- Description  : This macro applies a set of custom data quality rules to a given model
--                and returns all records that violate these rules. It's designed to
--                identify data quality issues and create error records for audit trails.
--                Handles empty rule sets gracefully by returning correct schema structure.
-- Author       : Helmut Ehrlich
-- Created      : 2025-07-30
-- -------------------------------------------------------------------------
--
-- FUNCTIONALITY:
-- - Applies multiple data quality rules to a specified dbt model
-- - Captures records that violate any of the defined rules
-- - Returns standardized error record structure with metadata
-- - Handles empty rule sets without errors (returns empty result with correct schema)
-- - Supports complex conditional logic in rule definitions
-- - Preserves source metadata (file info, hash keys, timestamps)
--
-- PARAMETERS:
-- @model (required): Reference to the dbt model to validate (use ref() or source())
-- @source_identifier_column (optional): Column name containing business identifier 
--   (e.g., invoice number) for linking errors to raw data (default: 'dss_hash_key')
-- @rules (required): List of rule dictionaries, each containing:
--   - rule_name: Descriptive name for the validation rule
--   - field_name: Name of the field being validated
--   - rule: SQL condition that should be TRUE for valid records
--
-- RETURNED COLUMNS:
-- - dss_file_name: Source file name from metadata
-- - dss_row_number: Row number from source file
-- - dss_last_modified: File last modified timestamp
-- - dss_hash_key: Record hash key for deduplication
-- - source_data_identifier: Business identifier (configurable via source_identifier_column)
-- - rule_name: Name of the violated rule
-- - field_name: Field that violated the rule
-- - invalid_value: Actual value that caused the violation
--
-- USAGE EXAMPLES:
--
-- 1. Tax Validation Rules (from f_co_sap_paid_fi_items_error.sql):
--    {{ assert_data_quality_rules(
--        model=ref('tf_co_sap_paid_fi_items'),
--        source_identifier_column='wbeln',
--        rules=[
--            {
--                "rule_name": "tax_must_be_smaller_then_pos_netwr", 
--                "field_name": "mwsbk", 
--                "rule": "(case when netwr > 0 then mwsbk < netwr else true end)"
--            },
--            {
--                "rule_name": "tax_must_be_smaller_amount_then_neg_netwr",
--                "field_name": "mwsbk",
--                "rule": "(case when netwr < 0 then mwsbk > netwr else true end)"
--            }
--        ]
--    ) }}
--
-- 2. Basic Field Validation:
--    {{ assert_data_quality_rules(
--        model=ref('my_model'),
--        source_identifier_column='invoice_number',
--        rules=[
--            {
--                "rule_name": "amount_not_null",
--                "field_name": "amount",
--                "rule": "amount is not null"
--            },
--            {
--                "rule_name": "amount_positive",
--                "field_name": "amount", 
--                "rule": "amount > 0"
--            }
--        ]
--    ) }}
--
-- 3. Using Default Hash Key as Identifier:
--    {{ assert_data_quality_rules(
--        model=ref('my_model'),
--        rules=[]
--    ) }}
--    -- Uses dss_hash_key as source_data_identifier by default
--
-- INTEGRATION PATTERN:
-- Typically used in error tables within CTEs:
--   with data_quality_errors as (
--       {{ assert_data_quality_rules(model=ref('source_model'), rules=rules_list) }}
--   ),
--   duplicates as (
--       {{ detect_duplicates_across_edw(...) }}
--   )
--   select * from data_quality_errors
--   union all
--   select * from duplicates
--
-- REQUIREMENTS:
-- - Source model must have metadata columns: dss_file_name, dss_row_number,
--   dss_last_modified, dss_hash_key
-- - Source model should have a business identifier column (specified via source_identifier_column)
-- - Rules should use SQL expressions that return boolean values
-- - Complex conditional logic can be implemented using CASE statements
--
-- -------------------------------------------------------------------------

{% macro assert_data_quality_rules(model, source_identifier_column='dss_hash_key', rules='[]') %}

with input_data as (
    select * from {{ model }}
),

errors as (
    {% if rules and rules|length > 0 %}
        {% for rule in rules %}
        select
            dss_file_name,
            dss_row_number,
            dss_last_modified,
            dss_hash_key,
            {{ source_identifier_column }} as source_data_identifier,
            '{{ rule.rule_name }}' as rule_name,
            '{{ rule.field_name }}' as field_name,
            cast({{ rule.field_name }} as string) as invalid_value
        from input_data
        where not ({{ rule.rule }})
        {% if not loop.last %}
        union all
        {% endif %}
        {% endfor %}
    {% else %}
        -- No rules provided - return empty result with correct schema
        select
            cast(null as varchar) as dss_file_name,
            cast(null as number) as dss_row_number,
            cast(null as timestamp) as dss_last_modified,
            cast(null as varchar) as dss_hash_key,
            cast(null as varchar) as source_data_identifier,
            cast(null as varchar) as rule_name,
            cast(null as varchar) as field_name,
            cast(null as varchar) as invalid_value
        from input_data
        where 1 = 0
    {% endif %}
)

select * from errors

{% endmacro %}