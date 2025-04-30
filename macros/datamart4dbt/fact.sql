{# COPYRIGHT NOTICE: written by sennendog (sennendog@abwesend.de). This code is under the Apache 2.0 license. See: http://www.apache.org/licenses/ #}
{# HOME: http://www.github.com/sennendog/datamart4dbt #}
{# CREDITS: inspired by automateDV, datavault4dbt, datavault4coalesce #}
{% macro fact(yaml_metadata) %}

{# FACT macro -- YAML schema:

source_model: modelname
fact_name: "name_of_dimension"

dimensions:
    - dim_customer:
        dim_name: customer
        dim_hk: customer_hk
        dim_bk:
            - customer_code:
                  source_column: my_customer_code
                  source_transform: left(1)

            - customer_id:
                  source_column: my_customer_id
        dim_effective_from: name_from
        dim_effective_to: name_to
    - dim_xyz:
        #...

facts:
    - price
    - quantity

fact_ldts:
    name: name_of_ldts
    source_column: src_name
    #source_transform: trfn   

fact_effectivity:
    source_column: src_name
    #source_transform: trfn  

fact_rowcount: rowcount_name

#}
{# -- FEATURES --
    - full support for VIEW, TABLE and INCREMENTAL materializations (including Snowflake dynamic_table)
#}
   
    {%- set metadata_dict = fromyaml(yaml_metadata) -%}
    {%- if metadata_dict is none -%}
        {{ exceptions.raise_compiler_error("[" ~ this ~ "] Error: malformed metadata_yaml, cannot process!" ) }}
    {%- endif -%}

    {# parse configuration parameters from yaml #}
    {%- set source_model        = ref(meta_getkey('source_model', metadata_dict, required=True)) -%}
    {%- set fact_name           = meta_getkey('fact_name', metadata_dict, required=True) -%}
    {%- set dimensions          = meta_getkey('dimensions', metadata_dict, required=True) -%}
    {%- set facts               = meta_getkey('facts', metadata_dict, required=False) -%}
    {%- set fact_effectivity    = meta_getkey('fact_effectivity', metadata_dict, required=False) -%}    
    {%- set fact_ldts           = meta_getkey('fact_ldts', metadata_dict, required=False, default_value='ldts') -%}    
    {%- set fact_rowcount       = meta_getkey('fact_rowcount', metadata_dict, required=False, default_value='rowcnt') -%}    


    {# CONSTANTS FROM CONFIG #}
    {%- set IS_CURRENT_FLAGS    = get_is_current_flags() -%}    


    {# PROCESS FIELDS #}
    {%- set all_facts = [] -%}
    {%- for fact in facts -%}
        {%- do all_facts.append(meta_describe_field(fact)) -%}
    {%- endfor -%}

    {%- set fact_effectivity    = meta_describe_effectivity(fact_effectivity, none) -%}
    {%- set fact_ldts           = meta_describe_effectivity(fact_ldts, 'ldts') -%}

    
    {%- set all_dimensions = [] -%}
    {%- for dim in dimensions -%}
        {%- set dim_name = (dim.items()|first)[0] -%}
        {%- set dim_hk   = (dim.items()|first)[1].dim_hk -%}

        {%- set dim_eff_from = (dim.items()|first)[1].dim_effective_from -%}
        {%- set dim_eff_to = (dim.items()|first)[1].dim_effective_to -%}
        {%- set dim_is_current = (dim.items()|first)[1].dim_is_current -%}        
        {%- set dim_isSCD2 = true if (dim_eff_from and dim_eff_to) else false -%}

        {%- set bk_fields = [] -%}
        {% for col in (dim.items()|first)[1].dim_bk %}
            {%- do bk_fields.append(meta_describe_field(col)) -%}
        {% endfor %}
        {%- if bk_fields|length == 0 -%}
            {{ exceptions.raise_compiler_error("[" ~ this ~ "] Error: malformed metadata_yaml, dimension without dim_bk fields found!" ) }}
        {%- endif -%}

        {%- do all_dimensions.append({
            'dim_name': dim_name,
            'dim_hk': dim_hk,
            'dim_eff_from': dim_eff_from,
            'dim_eff_to': dim_eff_to,
            'dim_is_current': dim_is_current,
            'dim_isSCD2': dim_isSCD2,
            'dim_bk': bk_fields
        })-%}
    {%- endfor -%}


    {# BEGIN CODE GENERATION #}
    WITH staging AS (
        SELECT 
            -- DIMENSION HASHKEYS
            {% for dim in all_dimensions -%}
                {{dim.dim_name}}.{{dim.dim_hk}},
            {% endfor %}

            -- FACTS
            {% for fact in all_facts -%}
                {{transform(fact.source_transform) or fact.source_column}} AS {{fact.name}},
            {% endfor %}

            -- TECHNICAL FIELDS
            1 AS {{fact_rowcount}},
            {{transform(fact_ldts.source_transform) or ('CURRENT_TIMESTAMP()' if fact_ldts.source_column=='ldts' else fact_ldts.source_column)}} AS {{fact_ldts.name}}
        FROM {{source_model}} AS src

        {# DIMENSION LOOKUPS IF ONLY NK WAS PROVIDED #}
        {%- for dim in all_dimensions %}
            LEFT JOIN {{ ref(dim.dim_name) }} AS {{dim.dim_name}} ON (1=1
                      {% for field in dim.dim_bk %} AND {{dim.dim_name}}.{{field.name}}=src.{{field.source_column}}{% endfor -%}
                    {% if dim.dim_isSCD2 and fact_effectivity.source_column %}
                        AND {{dim.dim_name}}.{{dim.dim_eff_from}} <= src.{{fact_effectivity.source_column}}
                        AND (({{dim.dim_name}}.{{dim.dim_eff_to}} > src.{{fact_effectivity.source_column}})
                        OR  ({{dim.dim_name}}.{{dim.dim_eff_to}} IS NULL))
                    {% elif dim.dim_isSCD2 %}
                        AND {{dim.dim_name}}.{{dim.dim_is_current}} = {{IS_CURRENT_FLAGS.current}}
                    {% endif %}
                    )
        {%- endfor -%}

        {# INCREMENTAL LOAD FILTERING #}
        {% if is_incremental() %}
            WHERE {{transform(fact_ldts.source_transform) or ('CURRENT_TIMESTAMP()' if fact_ldts.source_column=='ldts' else fact_ldts.source_column)}} > (select max({{fact_ldts.name}}) from {{ this }})
        {% endif %}

    )
    {% set last_cte = 'staging' %}


    SELECT * FROM {{last_cte}}

{% endmacro %}
