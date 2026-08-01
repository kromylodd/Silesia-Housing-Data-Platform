{{
    config(
        materialized='incremental',
        unique_key='listing_key',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with listings as (

    select *
    from {{ ref('stg_listings') }} as src

    {% if is_incremental() %}
    -- stg_listings is a view that dedupes across the FULL raw_apartment_listings
    -- history on every query. As that table grows daily, a full-table rebuild
    -- here would rescan all of it every run. This filter bounds the scan to
    -- listings rescraped since this table's last successful load; merge then
    -- inserts new listing_ids and updates existing ones in place - so scan
    -- cost stays roughly flat as raw history grows, instead of growing with it.
    --
    -- Uses >= rather than > deliberately: with a strict >, any dbt build run
    -- after the first one on a given calendar day would compare today's date
    -- against itself (today > today = false) and silently merge 0 rows for
    -- the rest of that day - no error, just quietly stale until the next
    -- calendar day. >= re-scans (and re-merges, harmlessly, since merge is
    -- keyed on listing_key) today's slice on every same-day rerun instead -
    -- more correct at the cost of reprocessing one day's worth of rows
    -- instead of zero, not the whole table.
    --
    -- NOTE: if stg_listings' dedup/mapping logic changes retroactively (e.g.
    -- a city_lookup edit), already-loaded rows here won't be reprocessed by
    -- this filter. Run `dbt run --full-refresh -s fact_apartments` after such
    -- changes.
    where cast(format_date('%Y%m%d', date(src.date_collected)) as int64)
        >= (select coalesce(max(date_collected_key), 0) from {{ this }})
    {% endif %}

),

fact as (

    select
        listings.listing_key,
        listings.listing_id,

        {{ dbt_utils.generate_surrogate_key(['listings.city']) }}
            as city_key,
        {{ dbt_utils.generate_surrogate_key(['listings.city', "coalesce(listings.district, 'Unknown')"]) }}
            as district_key,
        {{ dbt_utils.generate_surrogate_key(["coalesce(listings.building_type, 'Unknown')"]) }}
            as building_type_key,
        {{ dbt_utils.generate_surrogate_key(["coalesce(listings.market_type, 'Unknown')"]) }}
            as market_key,

        cast(format_date('%Y%m%d', date(listings.date_collected)) as int64)
            as date_collected_key,
        case
            when listings.date_published is not null
                then cast(format_date('%Y%m%d', date(listings.date_published)) as int64)
        end as date_published_key,

        listings.price,
        listings.currency,
        listings.area_sqm,
        listings.price_per_sqm_listed,
        round(listings.price / nullif(listings.area_sqm, 0), 2) as price_per_sqm_calculated,
        listings.num_rooms,
        listings.rooms_capped,
        listings.floor,
        listings.floor_capped,
        listings.is_furnished,
        listings.extra_rent_pln,
        listings.latitude,
        listings.longitude,
        listings.url

    from listings

)

select * from fact