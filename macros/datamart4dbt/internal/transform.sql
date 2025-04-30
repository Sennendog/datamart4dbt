{% macro transform(trfn=none) %}

    {% if trfn is string and trfn[:1] == '!' %}
        {% set return_value = "'"~trfn[1:]~"'" %}
    {% else %}
        {% set return_value = trfn %}
    {% endif %}

    {{ return(return_value) }}

{% endmacro %}
