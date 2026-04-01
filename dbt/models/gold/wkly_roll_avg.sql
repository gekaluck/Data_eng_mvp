{{ config(alias='weekly_roll_avg') }}

{% set snapshot_date = var('snapshot_date', none) %}

with base_snapshots as (
    select
        coin_id,
        snapshot_date,
        price_usd,
        market_cap_usd,
        volume_usd_24hr,
        vwap_24hr
    from {{ source('silver', 'price_snapshots') }}
    {% if snapshot_date %}
    where snapshot_date between date_add('day', -6, cast('{{ snapshot_date }}' as date))
        and cast('{{ snapshot_date }}' as date)
    {% endif %}
),

coins as (
    select
        id as coin_id,
        symbol,
        name
    from {{ source('silver', 'coins') }}
),

rolling_metrics as (
    select
        coin_id,
        snapshot_date,
        price_usd,
        market_cap_usd,
        volume_usd_24hr,
        vwap_24hr,
        avg(price_usd) over (
            partition by coin_id
            order by snapshot_date
            rows between 6 preceding and current row
        ) as wkly_roll_avg_price,
        avg(volume_usd_24hr) over (
            partition by coin_id
            order by snapshot_date
            rows between 6 preceding and current row
        ) as wkly_roll_avg_volume
    from base_snapshots
)

select
    r.snapshot_date,
    r.coin_id,
    c.symbol,
    c.name,
    r.price_usd,
    r.market_cap_usd,
    r.volume_usd_24hr,
    r.vwap_24hr,
    r.wkly_roll_avg_price,
    r.wkly_roll_avg_volume
from rolling_metrics r
left join coins c
    on r.coin_id = c.coin_id
{% if snapshot_date %}
where r.snapshot_date = cast('{{ snapshot_date }}' as date)
{% endif %}
