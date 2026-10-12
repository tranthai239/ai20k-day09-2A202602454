select trip_id, drive_id,
       cast(t_start_utc as timestamptz) as t_start, cast(t_end_utc as timestamptz) as t_end,
       lighting, conditions, try_cast(precipitation as double) as precipitation
from {{ source('lake', 'trips_raw') }}
