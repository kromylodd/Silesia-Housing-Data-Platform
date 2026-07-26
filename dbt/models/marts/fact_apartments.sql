with listings as (

    select * from {{ ref('stg_listings') }}

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