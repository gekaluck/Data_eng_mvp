-- Fails when a Gold row has a null price_change_pct even though Silver *does* hold
-- that coin's previous day. That combination is impossible to produce in a correct
-- build, so it is always a symptom rather than a data property.
--
-- This is the I12 signature. Gold computes the day-over-day LAG at run time, so a
-- date built before its predecessor landed in Silver sees no prior day and writes a
-- null. Nothing recomputes it when the missing day arrives later, so the null is
-- permanent until someone rebuilds — Gold's correctness silently depends on build
-- order. A genuine gap (prior day absent from Silver too) is fine and expected under
-- D025; that case is covered by daily_snapshot_gap_dates_retained.sql instead.

select
    gold.snapshot_date,
    gold.coin_id,
    gold.price_usd
from {{ ref('daily_snapshot') }} as gold
inner join {{ source('silver', 'price_snapshots') }} as previous_day
    on previous_day.coin_id = gold.coin_id
    and previous_day.snapshot_date = date_add('day', -1, gold.snapshot_date)
where gold.price_change_pct is null
