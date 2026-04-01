{{ config(alias='mc_rank_change') }}

{% set snapshot_date = var('snapshot_date', none) %}

with base_snapshots as (
    select
        coin_id,
        snapshot_date,
        price_usd,
        market_cap_usd
    from {{ source('silver', 'price_snapshots') }}
    {% if snapshot_date %}
    where snapshot_date in (
        cast('{{ snapshot_date }}' as date),
        date_add('day', -14, cast('{{ snapshot_date }}' as date)),
        date_add('day', -30, cast('{{ snapshot_date }}' as date))
    )
    {% endif %}
),

ranked_snapshots as (
    select
        coin_id,
        snapshot_date,
        price_usd,
        market_cap_usd,
        cast(
            rank() over (
                partition by snapshot_date
                order by market_cap_usd desc
            ) as integer
        ) as mc_rank
    from base_snapshots
),

today as (
    select
        coin_id,
        snapshot_date,
        price_usd,
        market_cap_usd,
        mc_rank
    from ranked_snapshots
    {% if snapshot_date %}
    where snapshot_date = cast('{{ snapshot_date }}' as date)
    {% endif %}
),

lookback_14d as (
    select
        coin_id,
        price_usd as price_usd_t_14,
        market_cap_usd as market_cap_usd_t_14,
        mc_rank as mc_rank_t_14
    from ranked_snapshots
    {% if snapshot_date %}
    where snapshot_date = date_add('day', -14, cast('{{ snapshot_date }}' as date))
    {% endif %}
),

lookback_30d as (
    select
        coin_id,
        price_usd as price_usd_t_30,
        market_cap_usd as market_cap_usd_t_30,
        mc_rank as mc_rank_t_30
    from ranked_snapshots
    {% if snapshot_date %}
    where snapshot_date = date_add('day', -30, cast('{{ snapshot_date }}' as date))
    {% endif %}
),

coins as (
    select
        id as coin_id,
        symbol,
        name
    from {{ source('silver', 'coins') }}
)

select
    t.snapshot_date,
    t.coin_id,
    c.symbol,
    c.name,
    t.price_usd,
    t.market_cap_usd,
    t.mc_rank,
    cast((-t.mc_rank + d14.mc_rank_t_14) as integer) as mc_rank_diff_14d,
    cast((-t.mc_rank + d30.mc_rank_t_30) as integer) as mc_rank_diff_30d,
    ((t.price_usd - d14.price_usd_t_14) / nullif(d14.price_usd_t_14, 0)) * 100.0 as price_diff_14d_pct,
    ((t.price_usd - d30.price_usd_t_30) / nullif(d30.price_usd_t_30, 0)) * 100.0 as price_diff_30d_pct
from today t
left join lookback_14d d14
    on t.coin_id = d14.coin_id
left join lookback_30d d30
    on t.coin_id = d30.coin_id
left join coins c
    on t.coin_id = c.coin_id
