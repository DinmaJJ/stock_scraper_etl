import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_FILE = "stock_data.db"

def get_connection():
    return sqlite3.connect(DB_FILE)

def extract_recent_raw_stocks(days=2):
    """Extract only recent raw data to avoid processing old junk every time"""
    conn = get_connection()
    query = f'''
        SELECT * FROM raw_stocks
        WHERE scraped_at >= date('now', '-{days} days')
        ORDER BY scraped_at DESC
    '''
    df = pd.read_sql_query(query, conn)
    conn.close()
    print(f"Extracted {len(df)} recent raw records (last {days} days)")
    return df

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
    # Deduplicate (very important!)
    df_cleaned = df_cleaned.drop_duplicates(subset=['ticker', 'scraped_at'], keep='last')
    print(f"After cleaning & dedup: {len(df_cleaned)} rows")
    return df_cleaned

def load_cleaned_data(df_cleaned):
    if df_cleaned.empty:
        print("No cleaned data to load — skipping")
        return

    conn = get_connection()

    # Step 1: Append or create the table (this line creates it if missing)
    df_cleaned.to_sql('cleaned_stocks', conn, if_exists='append', index=False)

    # Step 2: Now safe to add index (table definitely exists)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ticker_scraped
        ON cleaned_stocks (ticker, scraped_at)
    ''')

    # Step 3: Clean old data (keep last 45 days)
    cursor.execute("DELETE FROM cleaned_stocks WHERE scraped_at < date('now', '-45 days')")

    conn.commit()
    conn.close()

    print(f"Successfully appended {len(df_cleaned)} new rows to cleaned_stocks (table now exists)")
    
def etl_pipeline():
    print(f"ETL started at {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    # Use recent data only (much safer and faster)
    df_raw = extract_recent_raw_stocks(days=2)
    
    if df_raw.empty:
        print("No new raw data found. Skipping ETL.")
        return
    
    cleaned_df = transform_stocks_data(df_raw)
    load_cleaned_data(cleaned_df)
    print("ETL completed successfully.\n")

if __name__ == "__main__":
    etl_pipeline()
