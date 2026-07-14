# GMM vs Channel 3: GL Series-to-Music Crossover Intelligence

Analyzes how effectively GMMTV's GL (Girls' Love) series convert pairing popularity into
music/content engagement and momentum, compared to Channel 3 (BEC World) — using public
data only, at zero cost (including no credit card requirement for BigQuery — see below).

## Business Question
How quickly and strongly does GMMTV convert GL pairing popularity into engagement compared
to Channel 3, and what is the optimal timing for syncing content releases?

## Scope and Limitations (read before use)
- **Genre**: Compares GL only (Channel 3 does not yet have a clearly market-facing BL
  content lineup). GMM's BL pairings are kept in the schema for internal analysis only,
  not used in the cross-label comparison.
- **Data source**: YouTube Data API is the primary source (stable, official API). Google
  Trends is a **best-effort supplement** — see "Google Trends Limitations" below.
- **Spotify API excluded** because it was locked down in Feb 2026 (popularity/follower
  data is no longer accessible in free mode).
- **No production cost data** (budget, episode count) since this is internal data not
  accessible externally — this is an intentional limitation, not an oversight.
- **Not full market coverage** — only the top 3-4 highest-engagement pairings per label
  are tracked, not every GL series that exists.
- **Framing**: this project is market intelligence to identify whitespace, not an attempt
  to disparage a competitor.

## Project Structure
```
gmm-vs-ch3/
├── docker-compose.yml              # Airflow (metadata) + PostgreSQL
├── .env.example                    # credential template — copy to .env and fill in
├── .gitignore                      # keeps .env / gcp-service-account.json out of git
├── config/pairings.yaml            # pairing list + video_id (must be filled in manually)
├── scripts/
│   ├── poll_youtube.py             # fetch YouTube engagement → append to BigQuery
│   └── poll_trends.py              # fetch Google Trends search interest → append to BigQuery
├── airflow/dags/
│   ├── youtube_ingestion_dag.py    # calls scripts/poll_youtube.py daily
│   └── trends_ingestion_dag.py     # calls scripts/poll_trends.py daily
├── .github/workflows/
│   └── daily_poll.yml              # backup runner — runs on GitHub when local machine is off
├── dbt/
│   ├── profiles.yml                # connects to BigQuery via service account
│   ├── models/bronze/sources.yml   # raw sources + tests
│   ├── models/silver/              # dedupe (ROW_NUMBER), timezone normalization
│   └── models/gold/                # star schema fact tables, lead-lag delta
└── requirements.txt
```

## Getting Started (Setup)

### 1. Get a YouTube API Key (free, no card required)
GCP Console → Library → enable "YouTube Data API v3" → Credentials → Create API key
→ **Restrict the key**: limit API restrictions to YouTube Data API v3 only
(free quota is 10,000 units/day — this project uses only a few units per day)

### 2. Create a GCP Service Account (for BigQuery)
IAM & Admin → Service Accounts → Create → grant **BigQuery Data Editor** +
**BigQuery Job User** roles → create a JSON key → save the file as
`gcp-service-account.json` in the project root (already in `.gitignore` — never commit it)

**Note**: You still need to link a [billing account](https://console.cloud.google.com/billing)
to the GCP project (you won't be charged if you stay under the free tier of 1TB query +
10GB storage/month) — however, the pipeline as written **uses no DML at all**
(no MERGE/INSERT via SQL), relying entirely on Load Jobs instead, which in some cases
work even without billing enabled depending on your GCP account's org policy. If you hit
`403 Billing has not been enabled`, enable billing via the link above.

### 3. Fill in video_id in config/pairings.yaml
Visit GMMTV's and Channel 3's official YouTube channels, search for each pairing's
MV/trailer, and copy the video_id from the URL (youtube.com/watch?v=**VIDEO_ID**) into
the `youtube_video_ids` list.

### 4. Set up .env
```bash
cp .env.example .env
```
Fill in all values: `YOUTUBE_API_KEY`, `GCP_PROJECT_ID`, `POSTGRES_PASSWORD`,
`AIRFLOW_ADMIN_PASSWORD`

### 5. Run the whole stack
```bash
docker-compose up -d
```
Open the Airflow UI at http://localhost:8080 (user: `admin` / password: as set in `.env`)
Open the `youtube_engagement_daily_poll` and `google_trends_daily_poll` DAGs and trigger them.

**No need to keep this running 24/7** — start it when needed, then `docker-compose down`
once the DAG run finishes.

### 6. Run dbt (once Bronze has at least one day of data)
```bash
cd dbt
dbt run
dbt test
```

### 7. Connect Looker Studio
Connect Looker Studio to the BigQuery dataset `gmm_ch3_dw` (schema `gold`) and build the
dashboard: Overview, Lead-lag, Comparison, Global reach, Mascot, Recommendation.

## Data Warehouse: BigQuery

All project data (bronze/silver/gold) lives in **BigQuery** — it connects to Looker Studio
more smoothly (native Google integration) and follows the same pattern used successfully
in the SCG Net Zero and NYC TLC projects.

**PostgreSQL in docker-compose is used only for Airflow metadata** (DAG run history), not
for project data — the two responsibilities are kept clearly separate to avoid confusion.

## Preventing Duplicate Data: Append + Downstream Dedupe (no MERGE)

Because there are two write paths (local Airflow + cloud GitHub Actions) that could
potentially run on the same day, the original design used BigQuery `MERGE` (upsert).
However, MERGE is DML, which requires an active billing account. To reduce the billing
dependency (and work around cases where a credit card can't be linked), the approach was
changed to the following instead:

1. **Ingestion (`poll_youtube.py`, `poll_trends.py`)**: always writes data as an
   **append**, via `load_table_from_json` (a Load Job — not a DML query). Even if run
   twice on the same day, it simply creates duplicate rows in Bronze without erroring.
2. **Transformation (`silver_youtube_engagement.sql`)**: uses
   `ROW_NUMBER() OVER (PARTITION BY snapshot_date, video_id ORDER BY ingested_at DESC)`
   to keep only the latest row per (day, video) — Gold/the dashboard always sees
   already-deduplicated data.

This approach **works without a billing account at all**, since the Bronze layer can
tolerate duplicate data without affecting the correctness of the final output.

## Google Trends Limitations (Known Limitation)

`pytrends` is an unofficial library (there is no official Google Trends API), and Google
has been tightening bot/automated-request detection over time. During development of this
project, `429 Too Many Requests` errors occurred repeatedly even with retry + exponential
backoff (60/120/180 seconds). In some cases the IP appeared to be temporarily blocked for
hours to days after repeated testing.

**Design decisions that account for this**:
- `fetch_trend_score()` returns `None` instead of crashing when a fetch fails — that row
  is simply skipped, without affecting the YouTube data that was successfully collected.
- The GitHub Actions workflow sets `continue-on-error: true` specifically on the Trends
  step, so the whole workflow doesn't fail just because Trends failed.
- **Some daily snapshots may have no Trends data at all** — this is an accepted known
  limitation from the start, not a pipeline bug. Lead-lag analysis needs to handle
  missing data on this dimension accordingly.

## Scheduling: Airflow (primary) + GitHub Actions (backup)

**Airflow** is the primary runner, running locally via Docker Compose — suited for when
the machine is already on and you want to see DAGs/logs via the UI.

**Limitation**: Airflow needs a machine/server running at the scheduled time. If the
machine isn't on, the DAG won't trigger that day, leaving a gap in the time series.

**Solution**: `.github/workflows/daily_poll.yml` acts as a backup — it runs on GitHub's
servers (free, no need to keep your own machine on), using the same scripts
(`scripts/poll_youtube.py`, `scripts/poll_trends.py`) that Airflow calls, so there's no
duplicated code. YouTube and Trends are split into separate steps so a Trends failure
doesn't affect YouTube.

**GitHub Actions Setup**:
1. Go to repo Settings → Secrets and variables → Actions and add:
   - `YOUTUBE_API_KEY`
   - `GCP_PROJECT_ID`
   - `GCP_SA_KEY_BASE64` — encode the service account json with:
     ```bash
     base64 -i gcp-service-account.json | pbcopy   # macOS: copies straight to clipboard
     ```
2. The workflow runs automatically daily at 08:00 Thailand time, or can be triggered
   manually via "Run workflow" from the Actions tab.

**In practice**: Airflow is the one showcased in the portfolio (matches the tool I'm most
familiar with), while GitHub Actions is a safety net against data gaps when the local
machine is off — both use the same scripts, no duplication.

## Cost
Everything (Airflow, PostgreSQL, dbt-core, Docker) is open-source/self-hosted. The
YouTube Data API and Google Trends are both within free tier — **zero cost for the
entire project**. BigQuery's free tier (1TB query + 10GB storage/month) is far more than
this project's data volume requires.

## Production Considerations

This setup is scoped for a small portfolio project (2 DAGs running once daily). A real
production deployment would require additional work:
- CeleryExecutor/KubernetesExecutor instead of LocalExecutor, for parallelism across
  multiple machines
- A managed service (Cloud Composer/MWAA) instead of local Docker
- Monitoring/alerting (e.g., Slack notification on DAG failure)
- A secrets manager (Google Secret Manager) instead of local `.env`/JSON key files
- An official Google Trends alternative, or a paid social listening tool
  (Wisesight/Zocial Eye), instead of `pytrends`, for production-grade reliability

## Next Steps (not yet done)
- [ ] Pull real engagement snapshots to rank and select the final top 3-4 pairings
- [ ] Identify GMMTV's currently active mascots + check whether Channel 3 has any
- [ ] Add a dim_calendar for seasonality control
- [ ] Add full dbt tests (unique, relationships)
- [ ] Build the Looker Studio dashboard