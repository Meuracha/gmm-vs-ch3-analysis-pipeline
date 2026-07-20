import os
from google.cloud import bigquery

project_id = os.environ["GCP_PROJECT_ID"]
client = bigquery.Client(project=project_id)

query = f"""
SELECT *
FROM `{project_id}.gmm_ch3_dw.bronze_search_interest`
ORDER BY ingested_at DESC
LIMIT 20
"""

rows = list(client.query(query).result())
print(f"Found {len(rows)} rows:\n")
for row in rows:
    print(dict(row))