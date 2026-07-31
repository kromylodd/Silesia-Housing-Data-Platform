{{ config(materialized='ephemeral') }}

-- Daily-grain base for mart_market_trends. Deliberately NOT built on
-- stg_listings — that model collapses to one row per listing_id (latest
-- scrape only, across ALL history), which would make every trend point
-- identical. This reads straight from the append-only raw table so each
-- day a listing was scraped keeps its own row, one row per listing_id
-- per calendar day.
--
-- Deduped WITHIN a day (see `deduped` below): raw_apartment_listings has
-- no dedup guarantee on (listing_id, day) — a manually re-triggered run
-- (e.g. testing the Cloud Run Job before the daily schedule was confirmed
-- working) appends another full scrape for that day on top of the first.
-- 2026-07-29 saw up to 19x duplicate rows per listing_id from exactly this
-- during Cloud Run rollout testing, which inflated that day's num_listings
-- ~4x in mart_market_trends before this dedup was added.
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

),

deduped as (

    select *
    from filtered
    qualify row_number() over (
        partition by listing_id, date(date_collected)
        order by date_collected desc
    ) = 1

)

select * from deduped