{%- macro create_or_replace_stored_procedure(relation, preferred_language, runtime_version, load_packages, parameters, return_type, handler, comment, execute_as, sql) -%}

{{ log("create or replace procedure " ~ relation, info=True) }}   
CREATE OR REPLACE PROCEDURE {{ relation.include(database=(not temporary), schema=(not temporary)) }}({{ parameters }})
RETURNS {{ return_type }}
LANGUAGE {{ preferred_language }}
{% if runtime_version is not none -%}
RUNTIME_VERSION = '{{ runtime_version }}'
{% endif -%}
{% if load_packages is not none -%}
PACKAGES = {{ load_packages }}
{% endif -%}
{% if handler is not none -%}
HANDLER = '{{ handler }}'
{% endif -%}
{% if comment is not none -%}
COMMENT = '{{ comment }}'
{% endif -%}
EXECUTE AS {{ execute_as }}
AS
$$
    {{ sql }}
$$
;

{%- endmacro -%}