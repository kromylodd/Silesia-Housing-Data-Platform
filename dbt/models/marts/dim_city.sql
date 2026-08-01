with distinct_cities as (

    select distinct city
    from {{ ref('stg_listings') }}

),

city_lookup as (

    select * from {{ ref('city_lookup') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['distinct_cities.city']) }} as city_key,
    distinct_cities.city,
    city_lookup.voivodeship,
    coalesce(city_lookup.is_mvp, false) as is_mvp

from distinct_cities
left join city_lookup
    on distinct_cities.city = city_lookup.city_name