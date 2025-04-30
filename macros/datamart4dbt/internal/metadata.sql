{% macro meta_describe_field(field=none) %}

    {%- if field is mapping -%}
        {%- set field_key= (field.items()|first)[0] -%}
        {%- set field_obj = (field.items()|first)[1] -%}

        {%- set return_value = {'name':             (field_obj.name or field_key),
                                'source_column':    (field_obj.source_column or field_key),
                                'source_transform': (field_obj.source_transform or none),
                                'isBusinessKey':    (field_obj.isBusinessKey or none),
                                'isChangeTracking': (field_obj.isChangeTracking or none)
                            } -%}
    {%- else -%}
        {%- set return_value = {'name': (field),
                                'source_column': (field),
                                'source_transform': (none),
                                'isBusinessKey':    (none),
                                'isChangeTracking': (none)
                            } -%}
    {%- endif -%}

    {{ return(return_value) }}

{% endmacro %}



{% macro meta_describe_effectivity(field=none, default_name=none) %}

    {%- if field is mapping -%}
        {%- set return_value = {'name':             (field.name or default_name),
                                'source_column':    (field.source_column),
                                'source_transform': (field.source_transform or none)
                            } -%}
    {%- else -%}
        {%- set return_value = {'name': (field),
                                'source_column': (field),
                                'source_transform': (none)
                            } -%}
    {%- endif -%}

    {{ return(return_value) }}

{% endmacro %}



{% macro meta_getkey(keyname=none, metadata_dict=none, required=False, default_value=none) %}

    {% if metadata_dict is not none and metadata_dict is defined and keyname in metadata_dict.keys() %}
        {% set return_value = metadata_dict.get(keyname) %}
    {% elif required %}
        {{ exceptions.raise_compiler_error("[" ~ this ~ "] Error: Required parameter '" ~ keyname ~ "' not defined in yaml_metadata!'" ) }}
    {% else %}
        {% set return_value = default_value %}
    {% endif %}

    {{ return(return_value) }}

{% endmacro %}
