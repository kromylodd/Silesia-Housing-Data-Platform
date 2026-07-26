with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="date_add(current_date(), interval 1 year)"
    ) }}

),

renamed as (

    select
        cast(format_date('%Y%m%d', date_day) as int64) as date_key,
        date_day,
        extract(year from date_day)                     as year,
        extract(quarter from date_day)                   as quarter,
        extract(month from date_day)                     as month,
        format_date('%B', date_day)                      as month_name,
        extract(day from date_day)                       as day_of_month,
        extract(dayofweek from date_day)                  as day_of_week,
        format_date('%A', date_day)                       as day_name,
        extract(isoweek from date_day)                     as iso_week_of_year,
        extract(dayofweek from date_day) in (1, 7)          as is_weekend

    from spine

)

select * from renamed