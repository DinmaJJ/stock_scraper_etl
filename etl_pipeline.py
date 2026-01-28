import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_FILE = "stock_data.db"

def get_connection():
    return sqlite3.connect(DB_FILE)

# EXTRACTION

def extraxt_recent_raw_stocks(days=30):
    conn = get_connection()
    since_time = (datetime.now() - timedelta(days=days)).isoformat()
    query = '''
        SELECT * FROM raw_stocks
        WHERE scraped_at >= ?
    '''
    df = pd.read_sql_query(query, conn, params=(since_time,))
    conn.close()

    print(f"Extracted {len(df)} records from the last {days} days")
    return df

# TRANSFORMATION

def transform_stocks_data(df_raw):
    df = df_raw.copy()

    df['clean_ticker'] = df['ticker'].str.strip()

    df['clean_name'] = df['name'].str.strip()

    df['clean_price'] = df['price'].str.replace(r'[\$,]', '', regex=True).astype(float)

    df['clean_change'] = df['change'].str.replace(r'[\$,]', '', regex=True).astype(float)

    df['clean_pct_change'] = df['pct_change'].str.replace('%', '').astype(float)

    df['full_url'] = 'https://www.google.com' + df['href'].str.lstrip('.')
    
    df['scraped_at_dt'] = pd.to_datetime(df['scraped_at'], errors='coerce')

    df = df.dropna(subset=['clean_price', 'ticker'])

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

    print(f"After cleaning: {len(df_cleaned)} rows remain")
    return df_cleaned

# LOADING

def load_cleaned_data(df_cleaned):
    if df_cleaned.empty:
        print("No cleaned data to load")
        return

    conn = get_connection()
    df_cleaned.to_sql('cleaned_stocks', conn, if_exists='append', index=False)
    conn.close()

    print(f"Loaded {len(df_cleaned)} cleaned records into 'cleaned_stocks' table")

# ETL PIPELINE

def etl_pipeline():
    print(f"ETL started at {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    df_raw = extraxt_recent_raw_stocks(days=30)
    if df_raw.empty:
        print("No new raw data to process. ETL terminated.")
        return
    
    cleaned_df = transform_stocks_data(df_raw)
    load_cleaned_data(cleaned_df)

    print("ETL completed successfully. \n")

if __name__ == "__main__":
    etl_pipeline()
