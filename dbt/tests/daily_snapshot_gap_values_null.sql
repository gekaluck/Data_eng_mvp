select *
from {{ ref('daily_snapshot') }}
where prev_price_usd is null
  and (price_change_pct is not null or price_change_rank is not null)
