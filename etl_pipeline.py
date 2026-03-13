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
    
    # Force scraped_at to be datetime — use current time if parsing fails
    df['scraped_at_dt'] = pd.to_datetime(df['scraped_at'], errors='coerce')
    df['scraped_at_dt'] = df['scraped_at_dt'].fillna(datetime.now())

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
    
    df_cleaned = df_cleaned.drop_duplicates(subset=['ticker', 'scraped_at'], keep='last')
    print(f"After cleaning & dedup: {len(df_cleaned)} rows")
    print(f"Max scraped_at after transform: {df_cleaned['scraped_at'].max()}")  # debug
    return df_cleaned

def load_cleaned_data(df_cleaned, conn=None):
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    df_cleaned.to_sql('cleaned_stocks', conn, if_exists='append', index=False)

    cursor = conn.cursor()
    cursor.execute("SELECT MAX(scraped_at) FROM cleaned_stocks")
    print(f"DEBUG in load — Max scraped_at: {cursor.fetchone()[0]}")

    cursor.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ticker_scraped
        ON cleaned_stocks (ticker, scraped_at)
    ''')

    cursor.execute("DELETE FROM cleaned_stocks WHERE scraped_at < date('now', '-45 days')")

    conn.commit()

    if close_conn:
        conn.close()

    print(f"Successfully appended {len(df_cleaned)} new rows")
    
def etl_pipeline(conn=None):
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    print(f"ETL started at {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    df_raw = extract_recent_raw_stocks(days=2)
    if df_raw.empty:
        print("No new raw data found. Skipping ETL.")
        if close_conn:
            conn.close()
        return

    cleaned_df = transform_stocks_data(df_raw)
    load_cleaned_data(cleaned_df, conn=conn)  # pass conn

    if close_conn:
        conn.close()
    print("ETL completed successfully.\n")
    
    cleaned_df = transform_stocks_data(df_raw)
    load_cleaned_data(cleaned_df)
    print("ETL completed successfully.\n")

if __name__ == "__main__":
    etl_pipeline()
