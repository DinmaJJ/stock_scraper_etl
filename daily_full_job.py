# daily_full_job.py
import sys
import os
from datetime import datetime
from google.cloud import storage
import json

# Make sure we can import from the same directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import and run scraper
from stock_scraper_db import init_database, daily_scrape_job

# Import and run ETL
from etl_pipeline import etl_pipeline as run_etl

# These will come from GitHub Secrets
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_KEY_JSON = os.getenv("GCP_KEY_JSON")  # the full JSON string

# Initialize client using the service account key
if GCP_KEY_JSON:
    # Write key to temp file (GitHub Actions way)
    with open("/tmp/gcp-key.json", "w") as f:
        f.write(GCP_KEY_JSON)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcp-key.json"

client = storage.Client(project=GCP_PROJECT_ID)

BUCKET_NAME = "chidinma-stock-db-2026"  # your bucket name
BLOB_NAME = "stock_data.db"
LOCAL_PATH = "/tmp/stock_data.db"

def download_db():
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(BLOB_NAME)
    blob.download_to_filename(LOCAL_PATH)
    print("Downloaded DB from GCS")

def upload_db():
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(BLOB_NAME)
    blob.upload_from_filename(LOCAL_PATH)
    print("Uploaded DB to GCS")

if __name__ == "__main__":
    print("=== Starting daily full job ===")
    print(f"Date/time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Step 0: Download latest DB from GCS
    download_db()

    # Step 1: Scrape + save raw data (only on weekdays)
    init_database()
    daily_scrape_job()

    # Step 2: Run ETL (clean + load to cleaned_stocks)
    run_etl()

    # Step 3: Upload updated DB back to GCS
    upload_db()

    print("=== Daily full job completed ===")
