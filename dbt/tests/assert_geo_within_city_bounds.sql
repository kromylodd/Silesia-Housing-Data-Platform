-- Defense-in-depth: GE already blocks this pre-load (validate_batch.py's
-- per-city latitude/longitude bounding-box check, sourced from the same
-- city_lookup.bbox_* columns). Re-asserted here to catch any future path
-- into BigQuery that bypasses GE, or a bbox edited in the seed without a
-- matching GE deploy.
--
-- Rows with null lat/lon are excluded (OLX's `map` field is legitimately
-- absent for a meaningful share of listings — not a data quality bug, same
-- reasoning as the district warning suite).
select
    listings.listing_id,
    listings.city,
    listings.latitude,
    listings.longitude
from {{ ref('stg_listings') }} as listings
inner join {{ ref('city_lookup') }} as city_lookup
    on listings.city = city_lookup.city_name
where
    listings.latitude is not null
    and listings.longitude is not null
    and (
        listings.latitude < city_lookup.bbox_lat_min
        or listings.latitude > city_lookup.bbox_lat_max
        or listings.longitude < city_lookup.bbox_lon_min
        or listings.longitude > city_lookup.bbox_lon_max
    )