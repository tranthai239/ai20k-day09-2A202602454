-- Frames with a browser-readable URL, for Metabase thumbnails (the bucket prefix is public in this lab only).
select frame_id, drive_id, trip_id, event_time, split, round(speed_mps * 3.6, 1) as speed_kmh,
       replace(frame_uri, 's3://', 'http://localhost:9000/') as thumb_url
from {{ ref('gold_dataset_frames') }}
