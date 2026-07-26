with distinct_cities as (

    select distinct city
    from {{ ref('stg_listings') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_key,
    city
from distinct_cities