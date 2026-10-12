-- Source A: speed already m/s, ISO timestamps, no clock offset.
select drive_id,
       cast(ts_utc as timestamptz)         as ts,
       try_cast(nullif(speed_mps, '') as double) as speed_mps,
       cast(heading_deg as double)         as heading_deg,
       cast(lat as double) as lat, cast(lon as double) as lon, cast(alt_m as double) as alt_m,
       cast(acc_x as double) as acc_x, cast(acc_y as double) as acc_y,
       cast(gas as double) as gas, cast(brake as double) as brake, cast(steer as double) as steer,
       _batch_id, _row_no
from {{ source('lake', 'raw_telemetry_a') }}
