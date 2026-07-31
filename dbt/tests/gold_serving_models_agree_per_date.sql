-- Fails when the three dbt serving models disagree on per-date row counts.
--
-- I9's lesson applied to the rest of the serving layer. `gold_covers_every_silver_date`
-- asserts that daily_snapshot covers Silver, but nothing asserted the same for
-- mc_rank_change and weekly_roll_avg. Both were incremental-only and had never been
-- backfilled, so they held 9 dates against daily_snapshot's 107 while every test
-- stayed green. data_availability_daily reported it as 98 `partial` days, but a
-- report nobody has to acknowledge is not an assertion.
--
-- Row counts, not just date presence: a date that exists with half its coins is the
-- same failure one step later.

with daily as (
    select snapshot_date, count(*) as row_count
    from {{ ref('daily_snapshot') }}
    group by snapshot_date
),

rank_change as (
    select snapshot_date, count(*) as row_count
    from {{ ref('mc_rank_change') }}
    group by snapshot_date
),

weekly as (
    select snapshot_date, count(*) as row_count
    from {{ ref('wkly_roll_avg') }}
    group by snapshot_date
)

select
    daily.snapshot_date,
    daily.row_count as daily_snapshot_row_count,
    rank_change.row_count as rank_change_row_count,
    weekly.row_count as weekly_average_row_count
from daily
full join rank_change
    on daily.snapshot_date = rank_change.snapshot_date
full join weekly
    on daily.snapshot_date = weekly.snapshot_date
where daily.row_count is distinct from rank_change.row_count
    or daily.row_count is distinct from weekly.row_count
