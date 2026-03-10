-- macros/utils/strip_whitespace.sql
{% macro strip_whitespace(value) %}
    {{ return(value | replace(' ', '')) }}
{% endmacro %}