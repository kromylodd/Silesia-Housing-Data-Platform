-- Defense-in-depth: GE already blocks this pre-load (validate_batch.py
-- PRICE_MIN/PRICE_MAX). Re-asserted here to catch any future path into
-- BigQuery that bypasses GE.
select listing_id, price
from {{ ref('stg_listings') }}
where price is null or price <= 0 or price > 20000000