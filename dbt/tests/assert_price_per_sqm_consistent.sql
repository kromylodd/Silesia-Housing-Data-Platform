-- Mirrors GE's PRICE_PER_SQM_TOLERANCE (5%) cross-field check.
select
    listing_id, price, area_sqm, price_per_sqm_listed,
    round(price / nullif(area_sqm, 0), 2) as price_per_sqm_calculated
from {{ ref('stg_listings') }}
where price_per_sqm_listed is not null
  and area_sqm is not null and area_sqm != 0
  and abs(price / area_sqm - price_per_sqm_listed) / price_per_sqm_listed > 0.05