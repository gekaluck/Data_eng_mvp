select *
from {{ ref('data_availability_daily') }}
where (availability_status = 'missing' and silver_row_count <> 0)
   or (
       availability_status = 'available'
       and (
           silver_row_count < expected_asset_count
           or daily_snapshot_row_count <> silver_row_count
           or rank_change_row_count <> silver_row_count
           or weekly_average_row_count <> silver_row_count
       )
   )
   or (availability_status = 'available' and availability_reason is not null)
   or (availability_status <> 'available' and availability_reason is null)
