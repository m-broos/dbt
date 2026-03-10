{% macro default__generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name -%}

		{%- if  env_var('DBT_ENV_TYPE','DEV') == 'DEV' -%}

 			{{ node.name }}
        
		{%- else -%}

			{{ custom_alias_name | trim }}
        
		{%- endif -%}

    {%- elif node.version -%}

        {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}

    {%- else -%}

        {{ node.name }}

    {%- endif -%}

{%- endmacro %}