import sys
import os
from datetime import datetime
from google.cloud import storage
import json
import traceback
import pandas as pd  # added for safety check
import sqlite3      # added for conn_check

# Make sure we can import from the same directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import scraper and ETL
from stock_scraper_db import init_database, daily_scrape_job
from etl_pipeline import etl_pipeline as run_etl

# ── GCP Configuration ────────────────────────────────────────────────────────
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_KEY_JSON = os.getenv("GCP_KEY_JSON")  # full JSON string from GitHub Secrets

BUCKET_NAME = "chidinma-stock-db-2026"
BLOB_NAME = "stock_data.db"
LOCAL_PATH = "/tmp/stock_data.db"

# Initialize GCS client
if GCP_KEY_JSON:
    with open("/tmp/gcp-key.json", "w") as f:
        f.write(GCP_KEY_JSON)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcp-key.json"

client = storage.Client(project=GCP_PROJECT_ID)

def download_db():
    print("Downloading latest DB from GCS...")
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(BLOB_NAME)
    blob.download_to_filename(LOCAL_PATH)
    print(f"Downloaded {LOCAL_PATH} ({os.path.getsize(LOCAL_PATH):,} bytes)")

def upload_db():
    print("Uploading updated DB to GCS...")
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(BLOB_NAME)
    blob.upload_from_filename(LOCAL_PATH)
    print(f"Uploaded {LOCAL_PATH} → gs://{BUCKET_NAME}/{BLOB_NAME}")

if __name__ == "__main__":
    print("=== Starting daily full job ===")
    print(f"Date/time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Running on weekday: {datetime.now().weekday()} (0=Mon … 6=Sun)")

    try:
        # Step 0: Download latest DB
        download_db()

        # Step 1: Initialize DB & scrape (only weekdays)
        init_database()
        daily_scrape_job()

        # Step 2: ETL – clean & load to cleaned_stocks
        print("Running ETL...")
        run_etl()

        # NEW SAFETY CHECK - verify table exists before upload
        conn_check = sqlite3.connect(LOCAL_PATH)
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn_check)
        print("Tables in DB before upload:", tables['name'].tolist())
        
        try:
            max_clean = pd.read_sql("SELECT MAX(scraped_at) FROM cleaned_stocks", conn_check).iloc[0,0]
            print(f"Before upload — cleaned max scraped_at: {max_clean}")
        except Exception as check_err:
            print(f"Before upload — cleaned_stocks check failed: {check_err}")

        conn_check.close()

        # Step 3: Upload back to GCS
        upload_db()

        print("=== Daily full job completed successfully ===")

    except Exception as e:
        print("!!! ERROR in daily full job !!!")
        print(traceback.format_exc())
        raise  # re-raise so Actions shows red status
