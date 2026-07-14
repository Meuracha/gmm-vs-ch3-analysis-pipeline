# GMM vs ช่อง 3: GL Series-to-Music Crossover Intelligence

วิเคราะห์ว่าซีรีส์ GL ของ GMMTV แปลงความนิยม (คู่จิ้น) เป็น engagement/momentum
ได้เร็ว-แรงแค่ไหนเทียบกับช่อง 3 (BEC World) — ใช้ public data เท่านั้น ไม่มีค่าใช้จ่าย
(รวมถึงไม่ต้องผูกบัตรเครดิตกับ BigQuery เลย — ดูเหตุผลด้านล่าง)

## Business question
ซีรีส์ GL ของ GMMTV แปลงความนิยมคู่จิ้นเป็น engagement ได้เร็ว/แรงแค่ไหนเทียบกับช่อง 3
และควร sync การปล่อยคอนเทนต์ช่วงไหนถึงได้ผลสูงสุด?

## Scope และ Limitation (อ่านก่อนใช้งาน)
- **Genre**: เทียบเฉพาะ GL (ช่อง 3 ยังไม่มีคอนเทนต์ BL ที่ทำตลาดชัดเจน) เก็บคู่ BL ของ GMM
  ไว้ใน schema เพื่อการวิเคราะห์ภายในเท่านั้น ไม่ใช้เทียบข้ามค่าย
- **Data source**: YouTube Data API เป็นแหล่งหลัก (เสถียร, official API) ส่วน Google Trends
  เป็น **best-effort เสริม** — ดูหัวข้อ "ข้อจำกัดของ Google Trends" ด้านล่าง
- **Spotify API ตัดออก** เพราะถูกล็อกดาวน์ ก.พ. 2026 (ดึง popularity/followers ไม่ได้แล้วในโหมดฟรี)
- **ไม่มีข้อมูลต้นทุนการผลิต** (งบ, จำนวนตอน) เพราะเป็น internal data ที่เข้าถึงไม่ได้จากภายนอก
  — เป็นข้อจำกัดที่ตั้งใจ ไม่ใช่จุดบอด
- **ไม่ใช่ full market coverage** — เลือกเฉพาะคู่ที่ engagement สูงสุดของแต่ละค่าย (top 3-4)
  ไม่ใช่ทุกซีรีส์ GL ที่มีอยู่
- **Framing**: โปรเจกต์นี้เป็น market intelligence เพื่อหา whitespace ไม่ใช่การด้อยค่าคู่แข่ง

## โครงสร้างโปรเจกต์
```
gmm-vs-ch3/
├── docker-compose.yml              # Airflow (metadata) + PostgreSQL
├── .env.example                    # template credentials — คัดลอกเป็น .env แล้วเติมค่าจริง
├── .gitignore                      # กัน .env / gcp-service-account.json หลุดขึ้น git
├── config/pairings.yaml            # รายชื่อคู่จิ้น + video_id (ต้องเติมเอง)
├── scripts/
│   ├── poll_youtube.py             # ดึง YouTube engagement → append เข้า BigQuery
│   └── poll_trends.py              # ดึง Google Trends search interest → append เข้า BigQuery
├── airflow/dags/
│   ├── youtube_ingestion_dag.py    # เรียก scripts/poll_youtube.py รายวัน
│   └── trends_ingestion_dag.py     # เรียก scripts/poll_trends.py รายวัน
├── .github/workflows/
│   └── daily_poll.yml              # ตัวสำรอง รันบน GitHub แทน local เมื่อไม่เปิดเครื่อง
├── dbt/
│   ├── profiles.yml                # เชื่อม BigQuery ผ่าน service account
│   ├── models/bronze/sources.yml   # raw sources + tests
│   ├── models/silver/              # dedupe (ROW_NUMBER), timezone normalize
│   └── models/gold/                # star schema fact tables, lead-lag delta
└── requirements.txt
```

## วิธีเริ่มต้น (Setup)

### 1. ขอ YouTube API Key (ฟรี, ไม่ต้องผูกบัตร)
GCP Console → Library → เปิด "YouTube Data API v3" → Credentials → Create API key
→ **Restrict key**: จำกัด API restriction ให้เหลือแค่ YouTube Data API v3
(quota ฟรี 10,000 units/วัน — โปรเจกต์นี้ใช้แค่ไม่กี่ units/วัน)

### 2. สร้าง GCP Service Account (สำหรับ BigQuery)
IAM & Admin → Service Accounts → Create → ให้ role **BigQuery Data Editor** + **BigQuery Job User**
→ สร้าง key แบบ JSON → เก็บไฟล์ชื่อ `gcp-service-account.json` ไว้ที่ root ของโปรเจกต์
(อยู่ใน `.gitignore` แล้ว ห้าม commit เด็ดขาด)

**หมายเหตุ**: ต้องเปิด [billing account](https://console.cloud.google.com/billing) ของ GCP project
ด้วย (ไม่ถูกเรียกเก็บเงินถ้าไม่เกิน free tier 1TB query + 10GB storage/เดือน) — แต่ pipeline
ที่เขียนไว้ **ไม่ใช้ DML (MERGE/INSERT ผ่าน SQL) เลย** ใช้ Load Job แทนทั้งหมด ซึ่งบางกรณี
ใช้ได้แม้ไม่เปิด billing ก็ได้ ขึ้นกับ org policy ของแต่ละ GCP account — ถ้าเจอ
`403 Billing has not been enabled` ให้เปิด billing ตามลิงก์ด้านบน

### 3. เติม video_id ใน config/pairings.yaml
เข้า YouTube channel ทางการของ GMMTV และช่อง 3 ค้นหา MV/trailer ของแต่ละคู่
คัด video_id จาก URL (youtube.com/watch?v=**VIDEO_ID**) ใส่ในลิสต์ `youtube_video_ids`

### 4. ตั้งค่า .env
```bash
cp .env.example .env
```
เติมค่าให้ครบ: `YOUTUBE_API_KEY`, `GCP_PROJECT_ID`, `POSTGRES_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`

### 5. รันทั้งระบบ
```bash
docker-compose up -d
```
เปิด Airflow UI ที่ http://localhost:8080 (user: `admin` / password: ตามที่ตั้งใน `.env`)
เปิด DAG `youtube_engagement_daily_poll` และ `google_trends_daily_poll` แล้วกด trigger

**ไม่ต้องรันค้างตลอด 24 ชม.** — เปิดตอนต้องการ พอ DAG รันเสร็จก็ `docker-compose down` ได้

### 6. รัน dbt (หลังมีข้อมูลใน bronze แล้วอย่างน้อย 1 วัน)
```bash
cd dbt
dbt run
dbt test
```

### 7. เชื่อม Looker Studio
เชื่อมต่อ Looker Studio เข้ากับ BigQuery dataset `gmm_ch3_dw` (schema `gold`)
สร้าง dashboard: Overview, Lead-lag, Comparison, Global reach, Mascot, Recommendation

## Data Warehouse: BigQuery

ข้อมูลโปรเจกต์ (bronze/silver/gold) ทั้งหมดอยู่ใน **BigQuery** — เชื่อม Looker Studio ได้ลื่นกว่า
(native Google integration) และเป็น pattern เดียวกับที่เคยใช้สำเร็จในโปรเจกต์ SCG Net Zero และ NYC TLC

**PostgreSQL ใน docker-compose ใช้แค่สำหรับ Airflow metadata** (DAG run history) เท่านั้น
ไม่ใช่ที่เก็บข้อมูลโปรเจกต์ — แยกหน้าที่ชัดเจนเพื่อไม่ให้สับสน

## ป้องกันข้อมูลซ้ำ: Append + Downstream Dedupe (ไม่ใช้ MERGE)

เพราะมี 2 ทางเขียนข้อมูล (Airflow local + GitHub Actions cloud) ที่อาจรันชนวันเดียวกัน
เดิมออกแบบให้ใช้ BigQuery `MERGE` (upsert) แต่ MERGE เป็น DML ซึ่งต้องมี billing account
เปิดอยู่เท่านั้น — เพื่อลด dependency กับ billing (และแก้ปัญหากรณีบัตรเครดิตผูกไม่ได้)
จึงเปลี่ยนมาใช้แนวทางนี้แทน:

1. **Ingestion (`poll_youtube.py`, `poll_trends.py`)**: เขียนข้อมูลแบบ **append เสมอ**
   ผ่าน `load_table_from_json` (Load Job — ไม่ใช่ DML query) ต่อให้รันซ้ำวันเดียวกัน
   ก็แค่สร้างแถวซ้ำใน Bronze โดยไม่ error
2. **Transformation (`silver_youtube_engagement.sql`)**: ใช้
   `ROW_NUMBER() OVER (PARTITION BY snapshot_date, video_id ORDER BY ingested_at DESC)`
   เลือกเก็บแค่แถวล่าสุดต่อ (วัน, video) — Gold/dashboard เห็นข้อมูลที่ dedupe แล้วเสมอ

วิธีนี้ **ไม่ต้องมี billing account เลยก็ทำงานได้** เพราะ Bronze layer ยอมรับข้อมูลซ้ำได้
โดยไม่กระทบความถูกต้องของผลลัพธ์สุดท้าย

## ข้อจำกัดของ Google Trends (Known Limitation)

`pytrends` เป็น unofficial library (ไม่มี official Google Trends API) และ Google ตรวจจับ
bot/automated request เข้มงวดขึ้นเรื่อย ๆ — ระหว่างพัฒนาโปรเจกต์นี้เจอ `429 Too Many Requests`
ซ้ำหลายครั้งแม้จะมี retry + exponential backoff (60/120/180 วินาที) แล้วก็ตาม ในบางกรณี
IP อาจโดนบล็อกชั่วคราวเป็นชั่วโมงถึงวันจากการทดสอบซ้ำ ๆ

**การออกแบบที่รองรับปัญหานี้**:
- `fetch_trend_score()` คืนค่า `None` แทนการ crash ถ้าดึงไม่ได้ — แถวนั้นจะถูกข้ามไปเฉย ๆ
  ไม่กระทบ YouTube data ที่เก็บสำเร็จ
- GitHub Actions workflow ตั้ง `continue-on-error: true` ให้ step Trends โดยเฉพาะ
  เพื่อไม่ให้ทั้ง workflow fail เพราะ Trends ล้มเหลว
- **บาง snapshot วันอาจไม่มีข้อมูล Trends เลย** — เป็น known limitation ที่ยอมรับไว้ตั้งแต่ต้น
  ไม่ใช่บั๊กของ pipeline เวลาวิเคราะห์ lead-lag ต้อง handle missing data ในมิตินี้ด้วย

## Scheduling: Airflow (หลัก) + GitHub Actions (สำรอง)

**Airflow** เป็นตัวหลักที่รันบนเครื่อง local ผ่าน Docker Compose — เหมาะกับตอนที่เปิดเครื่องทำงานอยู่แล้ว
และต้องการเห็น DAG/log ผ่าน UI

**ข้อจำกัด**: Airflow ต้องมีเครื่อง/เซิร์ฟเวอร์รันอยู่ตอนถึงเวลา schedule ถ้าไม่เปิดเครื่องไว้
DAG จะไม่ trigger วันนั้น ทำให้ time-series ขาดช่วง

**ทางแก้**: `.github/workflows/daily_poll.yml` เป็นตัวสำรอง — รันบนเซิร์ฟเวอร์ของ GitHub
(ฟรี, ไม่ต้องเปิดเครื่องตัวเอง) ใช้ script เดียวกัน (`scripts/poll_youtube.py`,
`scripts/poll_trends.py`) ที่ Airflow เรียกอยู่ เพื่อไม่ให้โค้ดซ้ำกัน — แยกเป็นคนละ step
เพื่อให้ Trends fail ได้โดยไม่กระทบ YouTube

**Setup GitHub Actions**:
1. ไปที่ repo Settings → Secrets and variables → Actions เพิ่ม:
   - `YOUTUBE_API_KEY`
   - `GCP_PROJECT_ID`
   - `GCP_SA_KEY_BASE64` — encode service account json ด้วย:
     ```bash
     base64 -i gcp-service-account.json | pbcopy   # macOS คัดลอกเข้า clipboard เลย
     ```
2. Workflow รันอัตโนมัติทุกวัน 08:00 เวลาไทย หรือกด "Run workflow" เองได้จากแท็บ Actions

**สรุปการใช้งานจริง**: Airflow เป็นตัวโชว์ใน portfolio (ตรงกับเครื่องมือที่ถนัด) ส่วน GitHub Actions
เป็น safety net กันข้อมูลขาดช่วงเวลาที่ไม่ได้เปิดเครื่อง — ใช้ script เดียวกันทั้งคู่ ไม่ซ้ำโค้ด

## Cost
ทุกส่วน (Airflow, PostgreSQL, dbt-core, Docker) เป็น open-source/self-hosted
YouTube Data API และ Google Trends อยู่ใน free tier — **ไม่มีค่าใช้จ่ายทั้งโปรเจกต์**
BigQuery free tier (1TB query + 10GB storage/เดือน) เกินพอสำหรับขนาดข้อมูลของโปรเจกต์นี้มาก

## Production Considerations

Setup นี้เหมาะกับ portfolio project ขนาดเล็ก (2 DAG รันวันละครั้ง) ถ้าต้อง deploy จริงระดับ
production จะต้องปรับเพิ่ม:
- CeleryExecutor/KubernetesExecutor แทน LocalExecutor เพื่อรองรับ parallelism ข้ามหลายเครื่อง
- Managed service (Cloud Composer/MWAA) แทน local Docker
- Monitoring/alerting (เช่น แจ้งเตือนผ่าน Slack เมื่อ DAG fail)
- Secrets manager (Google Secret Manager) แทนไฟล์ `.env`/JSON key บนเครื่อง
- Official Google Trends alternative หรือ paid social listening tool (Wisesight/Zocial Eye)
  แทน `pytrends` เพื่อความเสถียรระดับ production

## Next steps (ยังไม่ได้ทำ)
- [ ] ดึง engagement snapshot จริงเพื่อจัดอันดับและเลือก top 3-4 คู่สุดท้าย
- [ ] ระบุมาสคอต GMMTV ที่ active + เช็คช่อง 3
- [ ] เพิ่ม dim_calendar สำหรับ seasonality control
- [ ] เพิ่ม dbt tests เต็มรูปแบบ (unique, relationships)
- [ ] สร้าง Looker Studio dashboard