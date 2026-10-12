-- LAB_STUDENT_ID (your MSSV, set in .env) salts the trip order, so every student gets a different but valid split.
-- The training-set manifest for images: one row per frame worth training on, with its split.
-- Split is decided per TRIP (a scene), stratified by drive, never per frame, so near-identical neighbours can not land on both sides.
with kept as (
  select * from {{ ref('silver_frame_telemetry') }}
  where not is_black and not is_blurry and not is_near_dup and sync_ok
), trips as (   -- ranked inside each drive, so every drive appears in train, val and test
  select drive_id, trip_id, row_number() over (partition by drive_id order by md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')) as rn, count(*) over (partition by drive_id) as n
  from (select distinct drive_id, trip_id from kept)
)
select k.frame_id, k.frame_uri, k.drive_id, k.trip_id,
       case when t.rn <= round(t.n * {{ var('split_train') }}) then 'train'
            when t.rn <= round(t.n * ({{ var('split_train') }} + {{ var('split_val') }})) then 'val'
            else 'test' end as split,
       k.event_time, k.speed_mps, k.heading_deg, k.steer, k.gas, k.brake, k.lat, k.lon,
       k.lighting, k.conditions, k.precipitation, k.sync_gap_ms
from kept k join trips t using (drive_id, trip_id)
order by k.drive_id, k.frame_idx
