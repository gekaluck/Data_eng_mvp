-- Fails when the two Gold implementations disagree on date coverage or row counts.
--
-- The Spark and dbt Gold models exist side by side so they can be compared. A
-- difference between them is only ever a bug in one of them, so any divergence is
-- worth failing on. Historically they drifted twice and nothing noticed:
--
--   I15 — range support was added to Spark Gold but not dbt Gold, so a 7-day
--         catch-up would have built 7 days in one and 1 day in the other.
--   I16 — D025 taught Spark Gold to keep rows when the prior day is missing; the
--         dbt model kept filtering them out, so the two diverged on the first day
--         after every gap — exactly the dates a comparison is most interesting.
--
-- Both failures invalidated the comparison at the interesting dates while every
-- individual table still looked fine on its own.

with dbt_gold as (
    select snapshot_date, count(*) as row_count
    from {{ ref('daily_snapshot') }}
    group by snapshot_date
),

spark_gold as (
    select snapshot_date, count(*) as row_count
    from {{ source('gold_spark', 'daily_snapshot') }}
    group by snapshot_date
)

select
    coalesce(dbt_gold.snapshot_date, spark_gold.snapshot_date) as snapshot_date,
    dbt_gold.row_count as dbt_row_count,
    spark_gold.row_count as spark_row_count
from dbt_gold
full join spark_gold
    on dbt_gold.snapshot_date = spark_gold.snapshot_date
where dbt_gold.row_count is distinct from spark_gold.row_count
