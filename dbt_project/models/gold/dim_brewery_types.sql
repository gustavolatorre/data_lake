with unique_types as (
    select distinct
        coalesce(brewery_type, '') as brewery_type
    from {{ ref('stg_silver_breweries') }}
)

select
    {{ surrogate_key(['brewery_type']) }} as brewery_type_key,
    brewery_type
from unique_types
