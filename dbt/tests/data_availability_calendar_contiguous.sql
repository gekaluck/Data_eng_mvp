with dates as (
    select
        snapshot_date,
        lag(snapshot_date) over (order by snapshot_date) as previous_date
    from {{ ref('data_availability_daily') }}
)

select *
from dates
where previous_date is not null
  and date_diff('day', previous_date, snapshot_date) <> 1
