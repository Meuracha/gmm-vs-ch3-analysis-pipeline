-- Silver: dedupe (เก็บแค่ snapshot ล่าสุดต่อวันต่อ video), normalize timezone เป็น ICT ตอนแสดงผล
with ranked as (
    select
        *,
        row_number() over (
            partition by snapshot_date, video_id
            order by ingested_at desc
        ) as rn
    from {{ source('bronze', 'bronze_youtube_engagement') }}
)

select
    snapshot_date,
    pairing_id,
    label_id,
    genre,
    video_id,
    view_count,
    like_count,
    comment_count,
    -- แปลง ingested_at (UTC) เป็นเวลาไทยตอนแสดงผล
    datetime(ingested_at, "Asia/Bangkok") as ingested_at_ict
from ranked
where rn = 1
