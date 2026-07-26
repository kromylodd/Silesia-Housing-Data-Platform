with listings as (

    select * from {{ ref('fact_apartments') }}

),

price_stats as (

    select
        count(*)                                                  as total_listings,
        round(avg(price), 2)                                      as avg_price,
        approx_quantiles(price, 2)[offset(1)]                     as median_price,
        min(price)                                                as min_price,
        max(price)                                                as max_price,
        round(stddev(price), 2)                                   as stddev_price,
        round(avg(price_per_sqm_calculated), 2)                   as avg_price_per_sqm,
        approx_quantiles(price_per_sqm_calculated, 2)[offset(1)]  as median_price_per_sqm

    from listings

)

select
    current_timestamp() as generated_at,
    *
from price_stats