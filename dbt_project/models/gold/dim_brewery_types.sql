-- Dimension table for brewery types.
--
-- The set of *active* types comes from the staging data so we never expose a
-- type that has not actually been seen in Silver. The textual description is
-- pulled from the brewery_type_mapping seed (canonical list maintained by the
-- analytics team). A LEFT JOIN preserves any type that exists in Silver but
-- is not yet in the seed — the dim_brewery_types_brewery_type__relationships
-- test will flag that mismatch loudly so the seed can be updated.

with unique_types as (
    select distinct
        coalesce(brewery_type, '') as brewery_type
    from {{ ref('stg_silver_breweries') }}
),

mapping as (
    select
        brewery_type,
        description
    from {{ ref('brewery_type_mapping') }}
)

select
    {{ surrogate_key(['ut.brewery_type']) }} as brewery_type_key,
    ut.brewery_type,
    m.description as brewery_type_description
from unique_types as ut
left join mapping as m
    on ut.brewery_type = m.brewery_type
