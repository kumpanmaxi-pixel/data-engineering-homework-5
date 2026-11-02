{{ config(
    materialized='table',
    distributed_replicated=true
) }}
with dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2026-03-01' as date)",
        end_date="cast('2026-07-12' as date)"
    ) }}
)
select
    date_day::date as date_day,
    extract(year from date_day)::int as year,
    extract(quarter from date_day)::int as quarter,
    extract(month from date_day)::int as month,
    extract(day from date_day)::int as day,
    extract(isodow from date_day)::int as day_of_week,
    extract(isodow from date_day) in (6, 7) as is_weekend
from dates