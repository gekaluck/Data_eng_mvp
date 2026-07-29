-- Fails when a snapshot date's prices are identical to the previous day's for
-- EVERY coin. That can't happen naturally -- 20 assets never all close exactly
-- flat -- so it means one API response was written under two different dates.
--
-- How that happened before: the old Bronze DAG fetched live /assets but labelled
-- the object with the run's logical_date. When several late catch-up runs fired
-- within seconds of each other, they each stored the same response under a
-- different date. The resulting price_change_pct is exactly 0.00 for every coin,
-- which reads as a genuinely flat market instead of a pipeline artifact.
--
-- Requiring *all* coins to be unchanged is what makes this safe: stablecoins sit
-- near zero every day, so a threshold on individual coins would false-positive
-- constantly. The coin_count guard keeps a sparse date from tripping it when only
-- a stablecoin or two happens to be present.

with per_date as (
    select
        snapshot_date,
        count(*) as coin_count,
        count_if(price_usd = prev_price_usd) as unchanged_coin_count
    from {{ ref('daily_snapshot') }}
    where prev_price_usd is not null
    group by snapshot_date
)

select
    snapshot_date,
    coin_count,
    unchanged_coin_count
from per_date
where coin_count >= 5
    and unchanged_coin_count = coin_count
