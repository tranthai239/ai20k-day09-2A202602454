-- Join by EVENT time: each frame gets the latest telemetry row at or before it (ASOF), and we keep the gap so a
-- bad match is visible instead of hidden.
select f.*,
       t.ts as telemetry_ts, t.speed_mps, t.heading_deg, t.lat, t.lon, t.acc_x, t.acc_y, t.gas, t.brake, t.steer,
       date_diff('millisecond', t.ts, f.event_time) as sync_gap_ms,
       coalesce(date_diff('millisecond', t.ts, f.event_time) <= {{ var('sync_tol_ms') }}, false) as sync_ok,
       not coalesce(date_diff('millisecond', t.ts, f.event_time) <= {{ var('sync_tol_ms') }}, false) as sync_lost,
       tr.lighting, tr.conditions, tr.precipitation
from {{ ref('silver_frames_flagged') }} f
asof left join {{ ref('silver_telemetry') }} t on f.drive_id = t.drive_id and f.event_time >= t.ts
left join {{ ref('silver_trips') }} tr using (trip_id)
