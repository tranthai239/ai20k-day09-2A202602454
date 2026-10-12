select drive_id,
       count(*)                                                         as rows_received,
       count(*) filter (where reject_reason is null)                    as rows_clean,
       count(*) filter (where reject_reason = 'duplicate')              as rows_duplicate,
       count(*) filter (where reject_reason = 'speed_missing')          as rows_speed_missing,
       count(*) filter (where reject_reason = 'speed_out_of_range')     as rows_speed_out_of_range
from {{ ref('silver_telemetry_classified') }}
group by drive_id
