with listings as (

    select * from {{ ref('fact_apartments') }}

),

district_dim as (

    select * from {{ ref('dim_district') }}

),

joined as (

    select
        district_dim.city,
        district_dim.district,
        listings.price,
        listings.price_per_sqm_calculated

    from listings
    inner join district_dim
        on listings.district_key = district_dim.district_key

    -- 'Unknown' isn't a real district — exclude it from the ranking so it
    -- can't show up as the "cheapest" or "most expensive" district.
    where district_dim.district != 'Unknown'

),

aggregated as (

    select
        city,
        district,
        count(*)                                 as num_listings,
        round(avg(price), 2)                     as avg_price,
        round(avg(price_per_sqm_calculated), 2)  as avg_price_per_sqm

    from joined
    group by city, district

)

select
    *,
    dense_rank() over (order by avg_price_per_sqm desc) as rank_most_expensive,
    dense_rank() over (order by avg_price_per_sqm asc)  as rank_cheapest

from aggregated