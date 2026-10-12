{{ config(materialized='view') }}
-- One conformed schema for both sources, then a verdict per row. Rows are never silently dropped: they are either
-- clean (reject_reason is null) or go to the quarantine table with the reason.
with unioned as (
  select * from {{ ref('stg_telemetry_a') }}
  union all by name
  select * from {{ ref('stg_telemetry_b') }}
), ranked as (
  select *, row_number() over (partition by drive_id, ts, lat, lon, heading_deg, acc_x, acc_y, steer order by _row_no) as copy_no
  from unioned
)
select *,
  case when copy_no > 1                       then 'duplicate'
       when speed_mps is null                  then 'speed_missing'
       when speed_mps < 0 or speed_mps > {{ var('max_speed_mps') }} then 'speed_out_of_range'
  end as reject_reason
from ranked
