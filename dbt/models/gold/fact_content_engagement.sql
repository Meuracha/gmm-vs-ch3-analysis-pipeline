-- Gold: รวม engagement ต่อคู่ต่อวัน (sum ข้าม video ในคู่เดียวกัน) + คำนวณ daily delta
with daily_totals as (
    select
        snapshot_date,
        pairing_id,
        label_id,
        genre,
        sum(view_count) as total_views,
        sum(like_count) as total_likes,
        sum(comment_count) as total_comments
    from {{ ref('silver_youtube_engagement') }}
    group by 1, 2, 3, 4
)

select
    snapshot_date,
    pairing_id,
    label_id,
    genre,
    total_views,
    total_likes,
    total_comments,
    total_views - lag(total_views) over (
        partition by pairing_id order by snapshot_date
    ) as views_delta_1d,
    -- comment velocity เป็น leading indicator แทน search interest (Google Trends ตัดออกแล้ว
    -- เพราะ unreliable — ดู README หัวข้อ "Google Trends Limitations")
    -- สมมติฐาน: คนคอมเมนต์เร็วมักสะท้อนความตื่นเต้น/พูดถึงแบบ real-time
    -- ก่อนที่ view count จะสะสมตามมา
    total_comments - lag(total_comments) over (
        partition by pairing_id order by snapshot_date
    ) as comments_delta_1d
from daily_totals