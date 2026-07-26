with listings as (

    select * from {{ ref('fact_apartments') }}

),

city_dim as (

    select * from {{ ref('dim_city') }}

),

joined as (

    select
        city_dim.city,
        listings.price,
        listings.area_sqm,
        listings.num_rooms,
        listings.price_per_sqm_calculated

    from listings
    inner join city_dim
        on listings.city_key = city_dim.city_key

)

select
    city,
    count(*)                                  as num_listings,
    round(avg(price), 2)                      as avg_price,
    approx_quantiles(area_sqm, 2)[offset(1)]  as median_area_sqm,
    round(avg(num_rooms), 2)                  as avg_rooms,
    round(avg(price_per_sqm_calculated), 2)   as avg_price_per_sqm

from joined
group by city