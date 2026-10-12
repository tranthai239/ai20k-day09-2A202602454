select drive_id, ts, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer
from {{ ref('silver_telemetry_classified') }}
where reject_reason is null
order by drive_id, ts
