import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_FILE = "stock_data.db"

def get_connection():
    return sqlite3.connect(DB_FILE)

# EXTRACTION: Get ALL raw data (not just recent) — this is the key change
def extract_all_raw_stocks():
    conn = get_connection()
    query = '''
        SELECT * FROM raw_stocks
        ORDER BY scraped_at DESC
    '''
    df = pd.read_sql_query(query, conn)
    conn.close()
    print(f"Extracted {len(df)} total raw records")
    return df

# TRANSFORMATION (same as before, but applied to all raw data)
def transform_stocks_data(df_raw):
    df = df_raw.copy()
    df['clean_ticker'] = df['ticker'].str.strip()
    df['clean_name'] = df['name'].str.strip()
    df['clean_price'] = df['price'].str.replace(r'[\$,]', '', regex=True).astype(float, errors='ignore')
    df['clean_change'] = df['change'].str.replace(r'[\+\$,-]', '', regex=True).astype(float, errors='ignore')
    df['clean_pct_change'] = df['pct_change'].str.replace('%', '').astype(float, errors='ignore')
    df['full_url'] = 'https://www.google.com' + df['href'].str.lstrip('.')

    df['scraped_at_dt'] = pd.to_datetime(df['scraped_at'], errors='coerce')
    df = df.dropna(subset=['clean_price', 'clean_ticker'])
    df['cleaned_at'] = datetime.now().isoformat()

    keep_cols = [
        'clean_ticker', 'clean_name', 'clean_price', 'clean_change',
        'clean_pct_change', 'full_url', 'scraped_at_dt', 'cleaned_at'
    ]
    df_cleaned = df[keep_cols].rename(columns={
        'clean_ticker': 'ticker',
        'clean_name': 'name',
        'clean_price': 'price',
        'clean_change': 'change',
        'clean_pct_change': 'pct_change',
        'full_url': 'url',
        'scraped_at_dt': 'scraped_at'
    })

    # Deduplicate (important!)
    df_cleaned = df_cleaned.drop_duplicates(subset=['ticker', 'scraped_at'], keep='last')

    print(f"After cleaning & dedup: {len(df_cleaned)} rows")
    return df_cleaned

# LOADING: Replace the entire cleaned table (since we re-process everything)
def load_cleaned_data(df_cleaned):
    if df_cleaned.empty:
        print("No cleaned data to load")
        return
    conn = get_connection()
    df_cleaned.to_sql('cleaned_stocks', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Replaced 'cleaned_stocks' with {len(df_cleaned)} cleaned records")
    
# ETL PIPELINE
def etl_pipeline():
    print(f"ETL started at {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    # Extract ALL raw data (not just recent)
    df_raw = extract_all_raw_stocks()
    if df_raw.empty:
        print("No raw data at all. ETL terminated.")
        return
    
    cleaned_df = transform_stocks_data(df_raw)
    load_cleaned_data(cleaned_df)
    print("ETL completed successfully.\n")

if __name__ == "__main__":
    etl_pipeline()
