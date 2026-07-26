with distinct_building_types as (

    select distinct
        coalesce(building_type, 'Unknown') as building_type
    from {{ ref('stg_listings') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['building_type']) }} as building_type_key,
    building_type
from distinct_building_types