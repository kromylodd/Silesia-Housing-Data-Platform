with base as (

    select * from {{ ref('int_listings_daily') }}

),

daily as (

    select
        'day'                as period_type,
        date_collected        as period_start,
        avg(price)            as avg_price,
        avg(price_per_sqm_calculated) as avg_price_per_sqm,
        count(*)              as num_listings

    from base
    group by period_start

),

weekly as (

    select
        'week'                              as period_type,
        date_trunc(date_collected, week(monday)) as period_start,
        avg(price)                          as avg_price,
        avg(price_per_sqm_calculated)       as avg_price_per_sqm,
        count(*)                            as num_listings

    from base
    group by period_start

),

monthly as (

    select
        'month'                            as period_type,
        date_trunc(date_collected, month)   as period_start,
        avg(price)                          as avg_price,
        avg(price_per_sqm_calculated)       as avg_price_per_sqm,
        count(*)                            as num_listings

    from base
    group by period_start

),

unioned as (

    select * from daily
    union all
    select * from weekly
    union all
    select * from monthly

),

with_growth as (

    select
        period_type,
        period_start,
        round(avg_price, 2)            as avg_price,
        round(avg_price_per_sqm, 2)    as avg_price_per_sqm,
        num_listings,

        round(
            avg_price - lag(avg_price) over (partition by period_type order by period_start),
            2
        ) as price_change_abs,
        round(
            safe_divide(
                avg_price - lag(avg_price) over (partition by period_type order by period_start),
                lag(avg_price) over (partition by period_type order by period_start)
            ) * 100,
            2
        ) as price_change_pct

    from unioned

)

select * from with_growth
order by period_type, period_start