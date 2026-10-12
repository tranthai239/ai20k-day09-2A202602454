select f.drive_id, f.trip_id, any_value(f.lighting) as lighting, any_value(f.conditions) as conditions,
       count(*) as frames, count(*) filter (where d.frame_id is not null) as frames_kept,
       round(avg(f.speed_mps) * 3.6, 1) as avg_speed_kmh, any_value(d.split) as split
from {{ ref('silver_frame_telemetry') }} f
left join {{ ref('gold_dataset_frames') }} d using (frame_id)
group by f.drive_id, f.trip_id
