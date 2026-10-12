-- Source B drifted: other column names, km/h, epoch milliseconds, percent pedals, and a device clock that runs
-- `telemetry_clock_offset_ms` ahead of the video clock (known from the ingest manifest, stored in bronze.video_files).
with off as (select drive_id, telemetry_clock_offset_ms as offset_ms from {{ source('lake', 'video_files') }})
select t.drive_id,
       to_timestamp((cast(t.timestamp_ms as bigint) - off.offset_ms) / 1000.0)  as ts,
       try_cast(nullif(t.spd_kmh, '') as double) / 3.6                           as speed_mps,
       cast(t.hdg as double)                                                     as heading_deg,
       cast(t.latitude as double) as lat, cast(t.longitude as double) as lon, cast(t.altitude as double) as alt_m,
       cast(t.ax as double) as acc_x, cast(t.ay as double) as acc_y,
       cast(t.throttle_pct as double) / 100 as gas, cast(t.brake_pct as double) / 100 as brake, cast(t.steering as double) as steer,
       t._batch_id, t._row_no
from {{ source('lake', 'raw_telemetry_b') }} t
join off using (drive_id)
