with source as (

    select *
    from {{ source('raw_housing', 'raw_apartment_listings') }}

),

city_lookup as (

    select * from {{ ref('city_lookup') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['source.id']) }}    as listing_key,
        source.id                                                as listing_id,
        source.url,
        source.title,
        safe_cast(source.created_time as timestamp)              as date_published,
        source.date_collected,

        -- source_city is the reliable scrape-target field (see raw table notes);
        -- `city` is OLX's noisy fuzzy-search text field and isn't used here.
        coalesce(city_lookup.city_name, initcap(source.source_city)) as city,
        source.source_city                                       as source_city_raw,
        source.district,

        source.latitude,
        source.longitude,
        source.price,
        source.currency,
        source.area_sqm,
        source.extra_rent_pln,
        source.num_rooms,
        source.rooms_capped,
        source.floor,
        source.floor_capped,
        source.is_furnished,
        source.building_type,
        source.market_type,
        source.price_per_sqm_listed

    from source
    left join city_lookup
        on source.source_city = city_lookup.source_city

),

filtered as (

    -- Great Expectations already gates these bounds before raw load (price
    -- 1-20M, area 10-500, rooms mostly within 1-10 — "mostly" allows a 5%
    -- outlier margin on rooms). This is a second, defensive gate: cheap
    -- insurance against future schema drift or a GE suite change upstream,
    -- and it documents the staging contract independently of the ingestion
    -- code.
    select *
    from renamed
    where
        listing_id is not null
        and price between 1 and 20000000
        and area_sqm between 10 and 500

    -- Same listing_id can recur across days (still-active listing, re-scraped).
    -- Staging represents current state; price-change history is the job of
    -- dbt snapshots (planned next), not this model.
    qualify row_number() over (
        partition by listing_id
        order by date_collected desc
    ) = 1

)

select * from filtered
