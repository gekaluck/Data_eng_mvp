{{ config(
    alias='data_availability_daily',
    materialized='table'
) }}

{% set availability_start_date = var('availability_start_date', none) %}
{% set availability_end_date = var('availability_end_date', none) %}
{% set expected_asset_count = var('expected_asset_count', 20) %}

-- A date spine is essential here: aggregating only existing records would make
-- completely absent dates invisible, which is exactly the failure this model
-- is intended to expose.
with bounds as (
    select
        {% if availability_start_date %}
        cast('{{ availability_start_date }}' as date) as start_date,
        {% else %}
        coalesce(min(snapshot_date), current_date) as start_date,
        {% endif %}
        {% if availability_end_date %}
        cast('{{ availability_end_date }}' as date) as end_date
        {% else %}
        current_date as end_date
        {% endif %}
    from {{ source('silver', 'price_snapshots') }}
),

calendar as (
    select snapshot_date
    from bounds
    cross join unnest(sequence(start_date, end_date, interval '1' day))
        as dates(snapshot_date)
    where start_date <= end_date
),

-- Row counts alone pass a day that arrived structurally incomplete: backfilled days
-- carry price and market cap but no volume or vwap. Counting the populated fields
-- separately keeps that visible without changing what `available` means.
silver_daily as (
    select
        snapshot_date,
        count(*) as silver_row_count,
        count(volume_usd_24hr) as volume_row_count,
        count(vwap_24hr) as vwap_row_count
    from {{ source('silver', 'price_snapshots') }}
    group by snapshot_date
),

daily_snapshot_daily as (
    select
        snapshot_date,
        count(*) as daily_snapshot_row_count
    from {{ ref('daily_snapshot') }}
    group by snapshot_date
),

rank_change_daily as (
    select
        snapshot_date,
        count(*) as rank_change_row_count
    from {{ ref('mc_rank_change') }}
    group by snapshot_date
),

weekly_average_daily as (
    select
        snapshot_date,
        count(*) as weekly_average_row_count
    from {{ ref('wkly_roll_avg') }}
    group by snapshot_date
),

coverage as (
    select
        calendar.snapshot_date,
        cast({{ expected_asset_count }} as integer) as expected_asset_count,
        cast(coalesce(silver.silver_row_count, 0) as integer) as silver_row_count,
        cast(coalesce(silver.volume_row_count, 0) as integer) as volume_row_count,
        cast(coalesce(silver.vwap_row_count, 0) as integer) as vwap_row_count,
        cast(coalesce(daily.daily_snapshot_row_count, 0) as integer) as daily_snapshot_row_count,
        cast(coalesce(rank_change.rank_change_row_count, 0) as integer) as rank_change_row_count,
        cast(coalesce(weekly.weekly_average_row_count, 0) as integer) as weekly_average_row_count
    from calendar
    left join silver_daily silver
        on calendar.snapshot_date = silver.snapshot_date
    left join daily_snapshot_daily daily
        on calendar.snapshot_date = daily.snapshot_date
    left join rank_change_daily rank_change
        on calendar.snapshot_date = rank_change.snapshot_date
    left join weekly_average_daily weekly
        on calendar.snapshot_date = weekly.snapshot_date
)

select
    snapshot_date,
    expected_asset_count,
    silver_row_count,
    volume_row_count,
    vwap_row_count,
    daily_snapshot_row_count,
    rank_change_row_count,
    weekly_average_row_count,
    round(100.0 * silver_row_count / nullif(expected_asset_count, 0), 2)
        as source_coverage_pct,
    round(100.0 * daily_snapshot_row_count / nullif(silver_row_count, 0), 2)
        as daily_snapshot_coverage_pct,
    round(100.0 * volume_row_count / nullif(silver_row_count, 0), 2)
        as volume_coverage_pct,
    round(100.0 * vwap_row_count / nullif(silver_row_count, 0), 2)
        as vwap_coverage_pct,
    case
        when silver_row_count = 0 then 'missing'
        when silver_row_count < expected_asset_count then 'partial'
        when daily_snapshot_row_count <> silver_row_count then 'partial'
        when rank_change_row_count <> silver_row_count then 'partial'
        when weekly_average_row_count <> silver_row_count then 'partial'
        else 'available'
    end as availability_status,
    case
        when silver_row_count = 0 then 'silver_missing'
        when silver_row_count < expected_asset_count then 'silver_below_expected_count'
        when daily_snapshot_row_count = 0 then 'daily_snapshot_missing'
        when daily_snapshot_row_count <> silver_row_count then 'daily_snapshot_row_count_mismatch'
        when rank_change_row_count = 0 then 'rank_change_missing'
        when rank_change_row_count <> silver_row_count then 'rank_change_row_count_mismatch'
        when weekly_average_row_count = 0 then 'weekly_average_missing'
        when weekly_average_row_count <> silver_row_count then 'weekly_average_row_count_mismatch'
        else null
    end as availability_reason
from coverage
