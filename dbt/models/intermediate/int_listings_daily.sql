{{ config(materialized='ephemeral') }}

-- Daily-grain base for mart_market_trends. Deliberately NOT built on
-- stg_listings — that model collapses to one row per listing_id (latest
-- scrape only), which would make every trend point identical. This reads
-- straight from the append-only raw table so each day a listing was
-- scraped keeps its own row.
--
-- Mirrors stg_listings' city-standardization and price/area sanity filter
-- rather than refactoring both onto a shared base model — avoids touching
-- the already-tested staging layer for a single downstream mart. Revisit
-- if a third model ends up needing this same daily grain.

with source as (

    select *
    from {{ source('raw_housing', 'raw_apartment_listings') }}

),

city_lookup as (

    select * from {{ ref('city_lookup') }}

),

renamed as (

    select
        source.id                                                     as listing_id,
        source.date_collected,
        coalesce(city_lookup.city_name, initcap(source.source_city))  as city,
        source.price,
        source.area_sqm,
        round(source.price / nullif(source.area_sqm, 0), 2)           as price_per_sqm_calculated

    from source
    left join city_lookup
        on source.source_city = city_lookup.source_city

),

filtered as (

    select *
    from renamed
    where
        listing_id is not null
        and price between 1 and 20000000
        and area_sqm between 10 and 500

)

select * from filtered