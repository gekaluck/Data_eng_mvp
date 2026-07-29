select *
from {{ ref('data_availability_daily') }}
where expected_asset_count < 0
   or silver_row_count < 0
   or daily_snapshot_row_count < 0
   or rank_change_row_count < 0
   or weekly_average_row_count < 0
