select split, count(*) as frames, count(distinct trip_id) as trips, count(distinct drive_id) as drives,
       round(avg(speed_mps) * 3.6, 1) as avg_speed_kmh
from {{ ref('gold_dataset_frames') }}
group by split
