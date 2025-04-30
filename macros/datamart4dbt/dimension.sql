{# COPYRIGHT NOTICE: written by sennendog (sennendog@abwesend.de). This code is under the Apache 2.0 license. See: http://www.apache.org/licenses/ #}
{# HOME: http://www.github.com/sennendog/datamart4dbt #}
{# CREDITS: inspired by automateDV, datavault4dbt, datavault4coalesce #}
{% macro dimension(yaml_metadata) %}

{# DIMENSION macro -- YAML schema:

source_model: modelname
dim_name: "name_of_dimension"

#variant 1: for renamed dimension hashkey (but generated on load)
dim_hk: name_of_hashkey_in_dim        

#variant 2: for renamed and already provided dimension hashkey
dim_hk:                         
    name: name_of_hashkey_in_dim
    source_column: src_name

dim_columns:
    - name1
    - name2
    - name3:
        source_column: src_name
        source_transform: lpad(src_name, 4)
    - name4:
        isBusinessKey: true
    - name5:
        isBusinessKey: true
        source_transform: "!I am a constant"
    - name6:
        isChangeTracking: true
    - name7:
        source_column: src_name
        isChangeTracking: true

dim_effective_from:
    name: name_of_valid_from
    source_column: src_name
dim_effective_to:
    name: name_of_valid_to
    source_column: src_name
dim_is_current: name_of_current_flag 

dim_ldts: 
    name: name_of_loaddate_timestamp
    source_column: src_name
    source_transform: optional trransform rule

#}
{# -- FEATURES --
    - automatically detects whether created dimension is of type SCD1 or SCD2 (due to isChangeTracking fields)
        - changes in isChangeTracking fields will be recorded as new entries in the dimension, making it a slow changing dimension type 2
        - changes in non-changeTracking fields do not impact the hashkey generation, and are thus ignored, enabling to mix changeTracking and non-changeTracking
    - drops generation of effective_from/to and is_current fields on non-changeTracking (SCD1) dimensions
    - creates stable hashkey from businessKeys (and changeTracking fields) on load
        - can be provided with already pre-computed hashkey from stage table, e.g. when loading from a data vault structure with hashkeys already present
    - setting a source-ldts (or effective_from in case of a changeTracking dimension) will prevent old data to overwrite current dimension state
    - full support for VIEW, TABLE and INCREMENTAL materializations (including Snowflake dynamic_table)
#}
   
    {%- set metadata_dict = fromyaml(yaml_metadata) -%}
    {%- if metadata_dict is none -%}
        {{ exceptions.raise_compiler_error("[" ~ this ~ "] Error: malformed metadata_yaml, cannot process!" ) }}
    {%- endif -%}


    {# parse configuration parameters from yaml #}
    {%- set source_model        = ref(meta_getkey('source_model', metadata_dict, required=True)) -%}
    {%- set dim_name            = meta_getkey('dim_name', metadata_dict, required=True) -%}
    {%- set dim_hk              = meta_getkey('dim_hk', metadata_dict, required=False) -%}
    {%- set dim_columns         = meta_getkey('dim_columns', metadata_dict, required=True) -%}    
    {%- set dim_effective_from  = meta_getkey('dim_effective_from', metadata_dict, required=False, default_value='dim_effective_from') -%}    
    {%- set dim_effective_to    = meta_getkey('dim_effective_to', metadata_dict, required=False, default_value='dim_effective_to') -%}    
    {%- set dim_is_current      = meta_getkey('dim_is_current', metadata_dict, required=False, default_value='dim_is_current') -%}    
    {%- set dim_ldts            = meta_getkey('dim_ldts', metadata_dict, required=Fals, default_value='dim_ldts') -%}    


    {# CONSTANTS FROM CONFIG #}
    {%- set IS_CURRENT_FLAGS    = get_is_current_flags() -%}       


    {# PROCESS FIELDS #}
    {%- if dim_hk is mapping -%}
        {%- set dim_hk = {'name': (dim_hk.name or 'dim_'~dim_name~'_hk'),
                          'source_column': (dim_hk.source_column or none)
                         } -%}
    {%- else -%}
        {%- set dim_hk = {'name': (dim_hk or 'dim_'~dim_name~'_hk'),
                          'source_column': none
                         } -%}
    {%- endif -%}

    {%- set all_fields = [] -%}
    {%- for col in dim_columns -%}
        {%- do all_fields.append(meta_describe_field(col)) -%}
    {%- endfor -%}

    {%- set dim_effective_from  = meta_describe_effectivity(dim_effective_from, 'dim_effective_from') -%}
    {%- set dim_effective_to    = meta_describe_effectivity(dim_effective_to, 'dim_effective_to') -%}
    {%- set dim_ldts            = meta_describe_effectivity(dim_ldts, 'dim_ldts') -%}

    {%- set businesskey_fields = [] -%}
    {%- set changetracking_fields = [] -%}
    {%- for field in all_fields -%}
        {%- if field.isBusinessKey -%}{%- do businesskey_fields.append(field) -%}{%- endif -%}
        {%- if field.isChangeTracking -%}{%- do changetracking_fields.append(field) -%}{%- endif -%}
    {%- endfor -%}
    {%- if businesskey_fields|length == 0 -%}
        {{ exceptions.raise_compiler_error("[" ~ this ~ "] Error: malformed metadata_yaml, dimension without at least one field with isBusinessKey=true is invalid!" ) }}
    {%- endif -%}
    {%- if changetracking_fields|length > 0 -%}
        {%- set dim_isSCD2 = true -%}
    {%- endif -%}


    {# BEGIN CODE GENERATION #}
    {%- set dim_natural_key        = businesskey_fields|map(attribute='name')|list|join(',') -%}
    {%- set deduplication_order    = (dim_ldts.name~' DESC, '~dim_effective_from.name~' ASC') if dim_isSCD2 else (dim_ldts.name~' DESC') -%}

    WITH staging AS (
        SELECT
            {# HASHKEY #}
            {% if dim_hk.source_column -%}
            -- HASH KEY (supplied from source)
                {{dim_hk.source_column}}
            {%- else -%}
            -- HASH KEY (generated from businesskeys and changetracking fields)
                {{dbt_utils.generate_surrogate_key(
                    businesskey_fields|map(attribute='source_column')|list +
                    changetracking_fields|map(attribute='source_column')|list
                    )}}
            {%- endif %} AS {{dim_hk.name}},

            -- PAYLOAD FIELDS
            {% for field in all_fields|list -%}
                {{transform(field.source_transform) or field.source_column}} AS {{field.name}},
            {% endfor %}

            -- LOAD DATE TIMESTAMP
            {{transform(dim_ldts.source_transform) or dim_ldts.source_column}} AS {{dim_ldts.name}}

            {% if dim_isSCD2 -%}
            -- SCD2: EFFECTIVITY FIELDS
                ,{{transform(dim_effective_from.source_transform) or dim_effective_from.source_column}} AS {{dim_effective_from.name}}
                --,{{transform(dim_effective_to.source_transform) or dim_effective_to.source_column}} AS {{dim_effective_to.name}}
            {%- endif %}

        FROM {{source_model}}

        QUALIFY row_number() OVER (PARTITION BY {{dim_hk.name}} ORDER BY {{deduplication_order}}) = 1
    )
    {% set last_cte = 'staging' %}

    ,deltaload_filtered AS (
        SELECT
            *
            {% if dim_isSCD2 -%}
            -- SCD2: EFFECTIVITY FIELDS
                --,{{dim_effective_from.name}}
                ,LAG({{dim_effective_from.name}}) OVER (PARTITION BY {{dim_natural_key}} ORDER BY {{dim_effective_from.name}} DESC) AS {{dim_effective_to.name}}

            -- SCD2: CURRENT FLAG
                ,CASE WHEN LAG({{dim_effective_from.name}} ) OVER (PARTITION BY {{dim_natural_key}} ORDER BY {{dim_effective_from.name}} DESC) IS NULL
                     THEN {{IS_CURRENT_FLAGS.current}}
                     ELSE {{IS_CURRENT_FLAGS.old}}
                END  AS {{dim_is_current}}
            {%- endif %}

        FROM {{last_cte}}
        {%- if is_incremental() and not dim_isSCD2 -%}
            {# Type 1 SCD incremental loads: overwrite with newer, but not older #}
            WHERE NOT EXISTS (select 1 from {{ this }} as tgt where 1=1 
                {% for field in businesskey_fields %} and tgt.{{field.name}}=staging.{{field.name}}{% endfor %}
                and tgt.{{dim_ldts.name}}>=staging.{{dim_ldts.name}}
            )
        {%- endif -%}
        {%- if is_incremental() and dim_isSCD2 -%}
            {# Type 2 SCD incremental loads: always create new dimension entry if newer values arrive #}
            WHERE NOT EXISTS (select 1 from {{ this }} as tgt where tgt.{{dim_hk.name}}=staging.{{dim_hk.name}}
                and tgt.{{dim_ldts.name}}>=staging.{{dim_ldts.name}}
            )
        {%- endif %}
        
    )
    {% set last_cte = 'deltaload_filtered' %}

    {% if is_incremental() and dim_isSCD2 %}        
        ,scd_update_previous AS (
            SELECT
                tgt.{{dim_hk.name}}
                {% for field in all_fields|selectattr('isBusinessKey', true)|list -%}
                ,tgt.{{field.name}}
                {% endfor -%}
                --all other fields from existing entry in dimension
                {% for field in all_fields|selectattr('isBusinessKey', false)|list -%}
                ,tgt.{{field.name}}
                {% endfor -%}
                ,tgt.{{dim_effective_from.name}}
                ,src.{{dim_effective_from.name}} AS {{dim_effective_to.name}}
                ,{{IS_CURRENT_FLAGS.old}} AS {{dim_is_current}}
            FROM {{ this }} AS tgt
            INNER JOIN {{last_cte}} AS src ON 1=1
                {% for field in businesskey_fields %}AND tgt.{{field.name}}=src.{{field.name}}{% endfor %}
                AND tgt.{{dim_is_current}} = {{IS_CURRENT_FLAGS.current}}
        )
    
        ,scd_update_union AS (
            SELECT * FROM deltaload_filtered
            UNION ALL
            SELECT * FROM scd_update_previous
        )
        {%- set last_cte = 'scd_update_union' -%}
    {% endif %}


    SELECT * FROM {{last_cte}}

{% endmacro %}
