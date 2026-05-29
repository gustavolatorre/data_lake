-- Dimension table for brewery types.
--
-- The set of *active* types comes from the staging data so we never expose a
-- type that has not actually been seen in Silver. The textual description is
-- pulled from the brewery_type_mapping seed (canonical list maintained by the
-- analytics team). A LEFT JOIN preserves any type that exists in Silver but
-- is not yet in the seed — the dim_brewery_types_brewery_type__relationships
-- test will flag that mismatch loudly so the seed can be updated.

-- Dedup whitespace/case variants (e.g. ' micro' vs 'micro' vs 'MICRO')
-- BEFORE the surrogate_key macro hashes the value. Without this, two raw
-- variants would produce identical brewery_type_key hashes on distinct
-- output rows, violating the unique test on the dim. The displayed
-- brewery_type is preserved as min(trim(...)) so the lowercase canonical
-- form keeps matching the brewery_type_mapping seed (which keys on
-- lowercase 'micro', 'brewpub', etc.) — uppercasing the column would
-- silently break the LEFT JOIN below.
--
-- NOTE: Changes existing surrogate_keys for any row whose raw value had
-- leading/trailing whitespace. Run `dbt run --full-refresh --select
-- dim_brewery_types fact_breweries` on first deploy to regenerate the
-- FK chain.
with unique_types as (
    select
        min(coalesce(trim(brewery_type), '')) as brewery_type
    from {{ ref('stg_silver_breweries') }}
    group by upper(trim(coalesce(brewery_type, '')))
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
