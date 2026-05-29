{{ config(
    materialized='incremental',
    unique_key='brewery_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with source_data as (
    select *
    from {{ ref('stg_silver_breweries') }}
    
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select
    md5(upper(trim(id))) as brewery_key,
    
    md5(
        upper(trim(coalesce(country, ''))) || '|' ||
        upper(trim(coalesce(state, ''))) || '|' ||
        upper(trim(coalesce(city, '')))
    ) as location_key,
    
    md5(upper(trim(coalesce(brewery_type, '')))) as brewery_type_key,
    
    id as id_business,
    name,
    address_1,
    is_active,
    1 as quantity,
    updated_at
from source_data
