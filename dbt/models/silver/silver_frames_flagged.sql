-- Cheap, explainable quality flags per frame. Near-duplicate = almost the same 64-bit dHash as the previous frame of the trip.
select f.*,
       f.brightness < {{ var('dark_luma') }}                                                  as is_black,
       f.brightness >= {{ var('dark_luma') }} and f.brightness < {{ var('dark_soft') }}      as is_dark,
       f.blur_score < {{ var('blur_var') }} and f.brightness >= {{ var('dark_luma') }}        as is_blurry,
       coalesce(bit_count(xor(f.dhash, lag(f.dhash) over (partition by f.trip_id order by f.frame_idx))) <= {{ var('dup_hamming') }}, false) as is_near_dup
from {{ source('lake', 'frames') }} f
