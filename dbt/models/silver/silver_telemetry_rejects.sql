select drive_id, ts, reject_reason, speed_mps, _batch_id, _row_no
from {{ ref('silver_telemetry_classified') }}
where reject_reason is not null
