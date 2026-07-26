with market as (

    select distinct
        coalesce(market_type, 'Unknown') as market_type
    from {{ ref('stg_listings') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['market_type']) }} as market_key,
    market_type
from market