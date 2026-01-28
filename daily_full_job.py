# daily_full_job.py
import sys
import os

# Make sure we can import from the same directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import and run scraper
from stock_scraper_db import init_database, daily_scrape_job

# Import and run ETL
from etl_pipeline import etl_pipeline as run_etl

if __name__ == "__main__":
    print("=== Starting daily full job ===")
    print(f"Date/time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Step 1: Scrape + save raw data (only on weekdays)
    init_database()
    daily_scrape_job()

    # Step 2: Run ETL (clean + load to cleaned_stocks)
    run_etl()

    print("=== Daily full job completed ===")
