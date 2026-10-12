select drive_id,
       case when speed_mps * 3.6 < 10 then '0-10' when speed_mps * 3.6 < 30 then '10-30'
            when speed_mps * 3.6 < 50 then '30-50' else '50+' end as speed_kmh_bucket,
       count(*) as frames
from {{ ref('gold_dataset_frames') }}
group by 1, 2
