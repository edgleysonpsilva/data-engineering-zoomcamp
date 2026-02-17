{{ config(materialized='table') }}

with green as (
    select * from {{ ref('stg_green_tripdata') }}
),
yellow as (
    select * from {{ ref('stg_yellow_tripdata') }}
)

select * from green
union all
select * from yellow