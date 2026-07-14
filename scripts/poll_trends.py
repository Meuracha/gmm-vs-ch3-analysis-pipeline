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
    จึงใช้ได้ในโหมดฟรีโดยไม่ต้องผูก billing account เลย"""
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


def fetch_trend_score(pytrends: TrendReq, keyword: str, max_retries: int = 3) -> int | None:
    """ดึง search interest พร้อม retry + exponential backoff"""
    for attempt in range(max_retries):
        try:
            pytrends.build_payload([keyword], timeframe="now 1-d", geo="TH")
            df = pytrends.interest_over_time()
            if not df.empty:
                return int(df[keyword].iloc[-1])
            return None
        except Exception as e:
            wait = 60 * (attempt + 1)
            print(f"[warn] Trends fetch failed for {keyword} (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"[info] Waiting {wait}s before retry...")
                time.sleep(wait)
    print(f"[error] Giving up on {keyword} after {max_retries} attempts")
    return None


def main(snapshot_date: str):
    config = load_pairings()
    client = bigquery.Client(project=GCP_PROJECT_ID)
    ensure_table(client)
    pytrends = TrendReq(hl="th-TH", tz=420)

    rows = []
    for pairing in config["pairings"]:
        keyword = f'{pairing["artist_1"]} {pairing["artist_2"]}'
        score = fetch_trend_score(pytrends, keyword)
        if score is not None:
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
        time.sleep(30)

    append_rows(client, rows)
    print(f"Done (appended, dedupe handled downstream): {snapshot_date}, {len(rows)} rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.date)