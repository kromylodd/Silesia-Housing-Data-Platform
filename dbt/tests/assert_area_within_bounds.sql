select listing_id, area_sqm
from {{ ref('stg_listings') }}
where area_sqm is null or area_sqm < 10 or area_sqm > 500