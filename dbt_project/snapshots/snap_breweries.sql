{#
  Point-in-time snapshot of the Silver breweries table.

  Tracks every change in (name, brewery_type, address_1, city, state, country,
  is_active) using dbt's timestamp strategy on `updated_at`. dbt manages
  `dbt_valid_from` / `dbt_valid_to` columns automatically.

  This enables queries like "what was the brewery_type of id=X on 2025-04-01?"
  which is impossible against the Silver MERGE'd table (only current state).
#}
{% snapshot snap_breweries %}

{{
    config(
      target_schema='silver',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=False,
    )
}}

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

{% endsnapshot %}
