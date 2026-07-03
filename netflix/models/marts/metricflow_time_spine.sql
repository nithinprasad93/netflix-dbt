{{
    config(
        materialized='table'
    )
}}

WITH spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1995-01-09' as date)",
        end_date="cast('2015-03-31' as date)"
    ) }}
)

SELECT
    cast(date_day as date) as date_day
FROM spine