"""
ดึง YouTube engagement เขียนลง BigQuery แบบ append (Load Job, ไม่ใช่ DML)
ไม่ต้องผูก billing account เพราะไม่มี MERGE/INSERT ผ่าน SQL เลย
ความซ้ำซ้อนจาก dual-write (Airflow local + GitHub Actions) จัดการที่ Silver layer
ด้วย ROW_NUMBER() แทน

ใช้:
  python scripts/poll_youtube.py --date 2026-07-10

ต้องการ env vars: YOUTUBE_API_KEY, GCP_PROJECT_ID
ต้องการไฟล์: GOOGLE_APPLICATION_CREDENTIALS ชี้ไป service account json
"""
import os
import argparse
import yaml
import requests
from datetime import datetime, timezone
from google.cloud import bigquery

CONFIG_PATH = os.environ.get("CONFIG_PATH", "config/pairings.yaml")
YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = "gmm_ch3_dw"
TABLE = f"{GCP_PROJECT_ID}.{DATASET}.bronze_youtube_engagement"

SCHEMA = [
    bigquery.SchemaField("snapshot_date", "DATE"),
    bigquery.SchemaField("pairing_id", "STRING"),
    bigquery.SchemaField("label_id", "STRING"),
    bigquery.SchemaField("genre", "STRING"),
    bigquery.SchemaField("video_id", "STRING"),
    bigquery.SchemaField("view_count", "INT64"),
    bigquery.SchemaField("like_count", "INT64"),
    bigquery.SchemaField("comment_count", "INT64"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]


def load_pairings():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fetch_youtube_stats(video_ids: list[str]) -> list[dict]:
    if not video_ids:
        return []
    results = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        resp = requests.get(
            "https://www.googleapis.com/youtube/v3/videos",
            params={"part": "statistics", "id": ",".join(batch), "key": YOUTUBE_API_KEY},
            timeout=30,
        )
        resp.raise_for_status()
        for item in resp.json().get("items", []):
            stats = item.get("statistics", {})
            results.append(
                {
                    "video_id": item["id"],
                    "view_count": int(stats.get("viewCount", 0)),
                    "like_count": int(stats.get("likeCount", 0)),
                    "comment_count": int(stats.get("commentCount", 0)),
                }
            )
    return results


def ensure_table(client: bigquery.Client):
    client.create_dataset(DATASET, exists_ok=True)
    client.create_table(bigquery.Table(TABLE, schema=SCHEMA), exists_ok=True)


def append_rows(client: bigquery.Client, rows: list[dict]):
    """เขียนข้อมูลแบบ append เข้า Bronze โดยตรง — ใช้ Load Job (jobs.load) ไม่ใช่ DML query
    จึงใช้ได้ในโหมดฟรีโดยไม่ต้องผูก billing account เลย
    ความซ้ำซ้อนจาก dual-write จะถูกจัดการที่ Silver layer แทน
    ด้วย ROW_NUMBER() partition by (snapshot_date, video_id) order by ingested_at desc"""
    if not rows:
        return
    job = client.load_table_from_json(
        rows,
        TABLE,
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition="WRITE_APPEND",
        ),
    )
    job.result()  # รอจน load job เสร็จ


def main(snapshot_date: str):
    config = load_pairings()
    client = bigquery.Client(project=GCP_PROJECT_ID)
    ensure_table(client)

    rows = []
    for pairing in config["pairings"]:
        video_ids = pairing.get("youtube_video_ids", [])
        if not video_ids:
            continue
        for s in fetch_youtube_stats(video_ids):
            rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "pairing_id": pairing["pairing_id"],
                    "label_id": pairing["label_id"],
                    "genre": pairing["genre"],
                    "video_id": s["video_id"],
                    "view_count": s["view_count"],
                    "like_count": s["like_count"],
                    "comment_count": s["comment_count"],
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
            )
    append_rows(client, rows)
    print(f"Done (appended, dedupe handled downstream): {snapshot_date}, {len(rows)} rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.date)