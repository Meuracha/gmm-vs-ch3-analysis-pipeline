-- Silver: dedupe (เก็บแค่ snapshot ล่าสุดต่อวันต่อ video ต่อคู่), normalize timezone เป็น ICT ตอนแสดงผล
-- หมายเหตุ: partition ต้องรวม pairing_id ด้วย เพราะหลายคู่อาจปรากฏร่วมกันใน video เดียวกัน
-- (เช่น trailer รวมของ Girl Rules ที่มีทั้ง milk_love, namtan_film, view_mim ใช้ video_id เดียวกัน)
-- ถ้า partition แค่ snapshot_date+video_id จะทำให้ dedupe ทิ้งคู่อื่นที่แชร์ video เดียวกันไปโดยไม่ตั้งใจ
with ranked as (
    select
        *,
        row_number() over (
            partition by snapshot_date, video_id, pairing_id
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