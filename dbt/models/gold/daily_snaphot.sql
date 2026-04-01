{{ config(alias='daily_snapshot') }}

{% set snapshot_date = var('snapshot_date', none) %}

with source_snapshots as (
    select
        coin_id,
        snapshot_date,
        price_usd,
        market_cap_usd,
        volume_usd_24hr,
        vwap_24hr
    from {{ source('silver', 'price_snapshots') }}
    {% if snapshot_date %}
    where snapshot_date between date_add('day', -1, cast('{{ snapshot_date }}' as date))
        and cast('{{ snapshot_date }}' as date)
    {% endif %}
),

snapshots_with_prev as (
    select
        coin_id,
        snapshot_date,
        price_usd,
        market_cap_usd,
        volume_usd_24hr,
        vwap_24hr,
        lag(snapshot_date) over (
            partition by coin_id
            order by snapshot_date
        ) as prev_snapshot_date,
        lag(price_usd) over (
            partition by coin_id
            order by snapshot_date
        ) as prev_price_usd
    from source_snapshots
),

ranked_snapshot as (
    select
        coin_id,
        snapshot_date,
        price_usd,
        prev_price_usd,
        ((price_usd - prev_price_usd) / nullif(prev_price_usd, 0)) * 100.0 as price_change_pct,
        cast(
            rank() over (
                partition by snapshot_date
                order by ((price_usd - prev_price_usd) / nullif(prev_price_usd, 0)) * 100.0 desc
            ) as integer
        ) as price_change_rank,
        market_cap_usd,
        volume_usd_24hr,
        vwap_24hr
    from snapshots_with_prev
    where prev_price_usd is not null
        and prev_snapshot_date = date_add('day', -1, snapshot_date)
    {% if snapshot_date %}
        and snapshot_date = cast('{{ snapshot_date }}' as date)
    {% endif %}
),

coins as (
    select
        id as coin_id,
        symbol,
        name,
        rank as coin_rank
    from {{ source('silver', 'coins') }}
)

select
    ranked_snapshot.snapshot_date,
    ranked_snapshot.coin_id,
    coins.symbol,
    coins.name,
    coins.coin_rank,
    ranked_snapshot.price_usd,
    ranked_snapshot.prev_price_usd,
    ranked_snapshot.price_change_pct,
    ranked_snapshot.price_change_rank,
    ranked_snapshot.market_cap_usd,
    ranked_snapshot.volume_usd_24hr,
    ranked_snapshot.vwap_24hr
from ranked_snapshot
left join coins
    on ranked_snapshot.coin_id = coins.coin_id
