-- Where frames go: how many come in per drive and why each is dropped (a frame can be counted in several reasons).
select drive_id,
       count(*)                                          as frames_total,
       count(*) filter (where is_black)                  as dropped_black,
       count(*) filter (where is_blurry)                 as dropped_blurry,
       count(*) filter (where is_dark)                   as flagged_dark_kept,
       count(*) filter (where is_near_dup)               as dropped_near_dup,
       count(*) filter (where not sync_ok)               as dropped_out_of_sync,
       count(*) filter (where not is_black and not is_blurry and not is_near_dup and sync_ok) as frames_kept
from {{ ref('silver_frame_telemetry') }}
group by drive_id
