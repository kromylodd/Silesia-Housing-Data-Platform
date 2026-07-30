with mart_total as (
    select sum(num_listings) as total from {{ ref('mart_city_summary') }}
),
stg_total as (
    select count(*) as total from {{ ref('stg_listings') }}
)
select mart_total.total as mart_count, stg_total.total as stg_count
from mart_total, stg_total
where mart_total.total != stg_total.total