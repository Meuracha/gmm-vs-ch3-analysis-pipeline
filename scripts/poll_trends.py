"""
ดึง Google Trends search interest เขียนลง BigQuery แบบ append (Load Job, ไม่ใช่ DML)
"""
import os
import time
import argparse
import yaml
from datetime import datetime, timezone
from google.cloud import bigquery
from pytrends.request import TrendReq

CONFIG_PATH = os.environ.get("CONFIG_PATH", "config/pairings.yaml")
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = "gmm_ch3_dw"
TABLE = f"{GCP_PROJECT_ID}.{DATASET}.bronze_search_interest"

SCHEMA = [
    bigquery.SchemaField("snapshot_date", "DATE"),
    bigquery.SchemaField("pairing_id", "STRING"),
    bigquery.SchemaField("label_id", "STRING"),
    bigquery.SchemaField("genre", "STRING"),
    bigquery.SchemaField("keyword", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("interest_score", "INT64"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]


def load_pairings():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_table(client: bigquery.Client):
    client.create_dataset(DATASET, exists_ok=True)
    client.create_table(bigquery.Table(TABLE, schema=SCHEMA), exists_ok=True)


def append_rows(client: bigquery.Client, rows: list[dict]):
    """เขียนข้อมูลแบบ append เข้า Bronze โดยตรง — ใช้ Load Job ไม่ใช่ DML query
    จึงใช้ได้ในโหมดฟรีโดยไม่ต้องผูก billing account เลย
    ความซ้ำซ้อนจาก dual-write จะถูกจัดการที่ Silver layer แทน"""
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
    job.result()


def fetch_trend_batch(pytrends: TrendReq, keywords: list[str], max_retries: int = 1) -> dict:
    """ดึง search interest ของหลาย keyword พร้อมกันใน 1 request (Google Trends รองรับสูงสุด 5 คำ/ครั้ง)
    ลดจำนวน request ลงมาก ช่วยเลี่ยงการโดน IP block จาก Google Trends
    (unofficial API ไม่มี SLA — บล็อกระดับ IP รุนแรงกว่าแค่ rate-limit รายคำขอ)
    max_retries=1 ชั่วคราว เพื่อให้ workflow จบเร็วตอนทดสอบว่า IP ยังโดนบล็อกอยู่ไหม"""
    for attempt in range(max_retries):
        try:
            pytrends.build_payload(keywords, timeframe="now 1-d", geo="TH")
            df = pytrends.interest_over_time()
            if df.empty:
                return {}
            return {kw: int(df[kw].iloc[-1]) for kw in keywords if kw in df.columns}
        except Exception as e:
            wait = 90 * (attempt + 1)  # 90s, 180s, 270s — ให้เวลา IP คูลดาวน์นานขึ้น
            print(f"[warn] Trends batch fetch failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"[info] Waiting {wait}s before retry...")
                time.sleep(wait)
    print(f"[error] Giving up on batch {keywords} after {max_retries} attempts")
    return {}


def main(snapshot_date: str):
    config = load_pairings()
    client = bigquery.Client(project=GCP_PROJECT_ID)
    ensure_table(client)
    pytrends = TrendReq(hl="th-TH", tz=420)

    pairings = config["pairings"]
    keyword_to_pairing = {
        f'{p["artist_1"]} {p["artist_2"]}': p for p in pairings
    }
    all_keywords = list(keyword_to_pairing.keys())

    rows = []
    # แบ่งเป็นกลุ่มละ 5 คำ (ข้อจำกัดของ Google Trends ต่อ 1 request)
    for i in range(0, len(all_keywords), 5):
        batch = all_keywords[i : i + 5]
        scores = fetch_trend_batch(pytrends, batch)
        for keyword, score in scores.items():
            pairing = keyword_to_pairing[keyword]
            rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "pairing_id": pairing["pairing_id"],
                    "label_id": pairing["label_id"],
                    "genre": pairing["genre"],
                    "keyword": keyword,
                    "country": "TH",
                    "interest_score": score,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        time.sleep(30)  # เว้นช่วงระหว่าง batch

    append_rows(client, rows)
    print(f"Done (appended, dedupe handled downstream): {snapshot_date}, {len(rows)} rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.date)