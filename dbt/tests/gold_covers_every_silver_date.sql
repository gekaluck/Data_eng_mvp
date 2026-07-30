-- Fails when a date exists in Silver but is missing from either Gold implementation.
--
-- This is the test that would have caught I9 on day one. Two Gold DAGs were paused
-- for 8 days while Bronze and Silver kept landing fresh data daily; every layer
-- looked green because nothing ever asserted that Gold covers Silver. The gap was
-- eventually spotted by a human looking at a Superset chart.
--
-- It also covers I13, where Silver was missing a date Bronze had: layer coverage
-- drifts independently, so it has to be asserted, not assumed.

with silver_dates as (
    select distinct snapshot_date
    from {{ source('silver', 'price_snapshots') }}
),

dbt_gold_dates as (
    select distinct snapshot_date
    from {{ ref('daily_snapshot') }}
),

spark_gold_dates as (
    select distinct snapshot_date
    from {{ source('gold_spark', 'daily_snapshot') }}
)

select
    silver_dates.snapshot_date,
    dbt_gold_dates.snapshot_date is not null as in_dbt_gold,
    spark_gold_dates.snapshot_date is not null as in_spark_gold
from silver_dates
left join dbt_gold_dates
    on silver_dates.snapshot_date = dbt_gold_dates.snapshot_date
left join spark_gold_dates
    on silver_dates.snapshot_date = spark_gold_dates.snapshot_date
where dbt_gold_dates.snapshot_date is null
    or spark_gold_dates.snapshot_date is null
