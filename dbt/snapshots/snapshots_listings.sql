{% snapshot snapshots_listings %}

{{
    config(
        target_schema='staging_housing',
        unique_key='listing_key',
        strategy='check',
        check_cols=['price', 'listing_status'],
    )
}}

select
    listing_key,
    listing_id,
    city,
    district,
    price,
    area_sqm,
    market_type,
    date_collected,
    case
        when date(date_collected) >= current_date - 2 then 'active'
        else 'likely_removed'
    end as listing_status
from {{ ref('stg_listings') }}

{% endsnapshot %}