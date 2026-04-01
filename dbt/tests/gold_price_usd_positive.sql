select
    'daily_snapshot' as model_name,
    coin_id,
    snapshot_date,
    price_usd
from {{ ref('daily_snapshot') }}
where price_usd <= 0

union all

select
    'mc_rank_change' as model_name,
    coin_id,
    snapshot_date,
    price_usd
from {{ ref('mc_rank_change') }}
where price_usd <= 0

union all

select
    'wkly_roll_avg' as model_name,
    coin_id,
    snapshot_date,
    price_usd
from {{ ref('wkly_roll_avg') }}
where price_usd <= 0
