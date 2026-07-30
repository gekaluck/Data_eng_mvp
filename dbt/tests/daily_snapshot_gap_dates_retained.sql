-- Fails when a gap date is not handled the way D025 requires.
--
-- A "gap date" is one whose previous day is absent from Silver — normal under the
-- build-forward strategy (D024), where coverage is sparse by design. I3 was the
-- opposite failure: Gold computed the day-over-day LAG and then filtered
-- `WHERE prev_price_usd IS NOT NULL`, so an isolated date produced *zero* rows and
-- tripped the count validator. D025 changed the rule to: keep every row, leave the
-- change columns null.
--
-- So a gap date must have exactly as many Gold rows as Silver (nothing dropped) and
-- no populated change columns at all (nothing invented from a non-adjacent day).

with silver_daily as (
    select
        snapshot_date,
        count(*) as silver_row_count
    from {{ source('silver', 'price_snapshots') }}
    group by snapshot_date
),

gap_dates as (
    select current_day.snapshot_date, current_day.silver_row_count
    from silver_daily current_day
    left join silver_daily previous_day
        on previous_day.snapshot_date = date_add('day', -1, current_day.snapshot_date)
    where previous_day.snapshot_date is null
),

gold_daily as (
    select
        snapshot_date,
        count(*) as gold_row_count,
        count_if(
            prev_price_usd is not null
            or price_change_pct is not null
            or price_change_rank is not null
        ) as populated_change_row_count
    from {{ ref('daily_snapshot') }}
    group by snapshot_date
)

select
    gap_dates.snapshot_date,
    gap_dates.silver_row_count,
    coalesce(gold_daily.gold_row_count, 0) as gold_row_count,
    coalesce(gold_daily.populated_change_row_count, 0) as populated_change_row_count
from gap_dates
left join gold_daily
    on gap_dates.snapshot_date = gold_daily.snapshot_date
where coalesce(gold_daily.gold_row_count, 0) <> gap_dates.silver_row_count
    or coalesce(gold_daily.populated_change_row_count, 0) <> 0
