with distinct_districts as (

    -- district is legitimately missing for a meaningful share of listings
    -- (see GE warning suite note in stg_listings) — coalesce to a sentinel
    -- so those rows still join cleanly in fact_apartments.
    select distinct
        city,
        coalesce(district, 'Unknown') as district
    from {{ ref('stg_listings') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'district']) }} as district_key,
    city,
    district
from distinct_districts