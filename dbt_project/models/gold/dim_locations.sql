with grouped_locations as (
    select
        upper(trim(coalesce(country, ''))) as country_upper,
        upper(trim(coalesce(state, ''))) as state_upper,
        upper(trim(coalesce(city, ''))) as city_upper,
        max(trim(coalesce(country, ''))) as country,
        max(trim(coalesce(state, ''))) as state,
        max(trim(coalesce(city, ''))) as city
    from {{ ref('stg_silver_breweries') }}
    group by 1, 2, 3
)

select
    md5(country_upper || '|' || state_upper || '|' || city_upper) as location_key,
    city,
    state,
    country
from grouped_locations

