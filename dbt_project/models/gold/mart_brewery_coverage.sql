with fact as (
    select * from {{ ref('fact_breweries') }}
),

locations as (
    select * from {{ ref('dim_locations') }}
),

aggregations as (
    select
        l.country,
        l.state,
        l.city,
        sum(case when f.is_active = true then 1 else 0 end) as active_breweries_count,
        sum(case when f.is_active = false then 1 else 0 end) as inactive_breweries_count,
        count(*) as total_breweries
    from fact f
    inner join locations l on f.location_key = l.location_key
    group by l.country, l.state, l.city
),

final as (
    select
        country,
        state,
        city,
        active_breweries_count,
        inactive_breweries_count,
        total_breweries,
        round(
            (active_breweries_count * 100.0) / nullif(total_breweries, 0), 
            2
        ) as active_ratio,
        dense_rank() over (
            partition by state 
            order by active_breweries_count desc
        ) as city_state_rank
    from aggregations
)

select * from final
