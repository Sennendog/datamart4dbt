{% macro get_is_current_flags() %}

    {# config for is_current_flag setting in dimension #}
    {{ return({'current': "'Y'", 'old': "'N'"}) }}

{% endmacro %}
