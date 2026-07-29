{{ config(
    alias='latest_market_snapshot',
    materialized='table'
) }}

with latest_date as (
    select max(snapshot_date) as snapshot_date
    from {{ ref('daily_snapshot') }}
)

select daily.*
from {{ ref('daily_snapshot') }} daily
inner join latest_date
    on daily.snapshot_date = latest_date.snapshot_date
