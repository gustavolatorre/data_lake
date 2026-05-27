select
    id,
    name,
    brewery_type,
    address_1,
    city,
    state,
    country,
    is_active,
    updated_at,
    ingestion_date
from {{ source('nessie_silver', 'breweries') }} AT BRANCH main
