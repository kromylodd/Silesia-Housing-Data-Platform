{{ config(severity='error') }}

-- Defense-in-depth: GE already blocks this pre-load (validate_batch.py's
-- per-city latitude/longitude bounding-box check, sourced from the same
-- city_lookup.bbox_* columns). Re-asserted here to catch any future path
-- into BigQuery that bypasses GE, or a bbox edited in the seed without a
-- matching GE deploy.
--
-- severity: warn (temporary) — the 34-city raw data currently in
-- raw_apartment_listings was loaded before this check existed in
-- validate_batch.py, so GE never screened it. First dbt build after adding
-- this test (2026-08-02) surfaced 974 pre-existing failures — not
-- necessarily a bbox miscalibration, likely a mix of genuine OLX
-- fuzzy-search noise on that historical batch and possibly some bboxes
-- that are tighter than OLX's real search radius for a given city. Flip
-- back to the default (error) once triaged via the per-city breakdown
-- query and either the seed bboxes are widened or the bad historical rows
-- are backfilled/removed. Going forward, GE already quarantines new bad
-- rows individually at ingestion (see QUARANTINE_MAX_FRACTION) — this
-- warn-severity window only affects already-loaded historical rows.
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