import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from datetime import datetime, timedelta
import os
import json
import urllib.request

st.set_page_config(page_title="Stock Dashboard", layout="wide")

st.title("Stock Most Active Dashboard")

# ── GCS Configuration ──────────────────────────────────────────────────────
BUCKET_NAME = "chidinma-stock-db-2026"  # Your bucket name
BLOB_NAME = "stock_data.db"
LOCAL_DB_PATH = "stock_data.db"  # Where we save the downloaded file


# ── Load data with GCS sync ────────────────────────────────────────────────
@st.cache_data(ttl=3600)  # 1 hour cache – adjust to 300 for 5 min testing
def load_data():
    url = "https://storage.googleapis.com/chidinma-stock-db-2026/stock_data.db"
    
    try:
        # Download the latest file from public GCS
        urllib.request.urlretrieve(url, "stock_data.db")
        st.caption("Loaded fresh data from Google Cloud Storage")
    except Exception as e:
        st.warning(f"Couldn't download latest DB from GCS: {e}\nUsing cached/local copy.")
    
    conn = sqlite3.connect("stock_data.db")
    df = pd.read_sql("""
        SELECT 
            ticker, name, price, change, pct_change,
            datetime(scraped_at) AS scraped_at,
            cleaned_at
        FROM cleaned_stocks 
        ORDER BY scraped_at DESC
    """, conn)
    
    df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
    conn.close()
    return df

if df.empty:
    st.warning("No data yet. Run the ETL pipeline first.")
    st.stop()

last_refresh = df['scraped_at'].max()
st.markdown(f"**Last data refresh**: {last_refresh}")

# ── All 5 queries defined at the top ───────────────────────────────────────
query_1 = """
SELECT ticker, name, price, change, pct_change 
FROM cleaned_stocks
WHERE scraped_at = (SELECT MAX(scraped_at) FROM cleaned_stocks)
ORDER BY pct_change DESC
LIMIT 10;
"""

query_2 = """
SELECT 
    DATE(scraped_at)                           AS trade_date,
    COUNT(*)                                   AS number_of_stocks,
    ROUND(AVG(pct_change), 2)                  AS avg_pct_change,
    ROUND(MIN(pct_change), 2)                  AS worst_pct_change,
    ROUND(MAX(pct_change), 2)                  AS best_pct_change,
    ROUND(
        SUM(CASE WHEN pct_change > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        1
    )                                          AS percentage_of_gainers
FROM cleaned_stocks
WHERE scraped_at = (SELECT MAX(scraped_at) FROM cleaned_stocks)
GROUP BY DATE(scraped_at);
"""

query_3 = """
SELECT 
    ticker,
    name,
    COUNT(*)                                    AS days_appeared,
    ROUND(AVG(pct_change), 2)                   AS avg_daily_pct_change,
    ROUND(SUM(pct_change), 2)                   AS cumulative_pct_this_week,
    ROUND(MIN(pct_change), 2)                   AS worst_day_pct,
    ROUND(MAX(pct_change), 2)                   AS best_day_pct
FROM cleaned_stocks
WHERE scraped_at >= DATE('now', '-7 days')
GROUP BY ticker, name
HAVING COUNT(*) >= 2
ORDER BY cumulative_pct_this_week DESC
LIMIT 10;
"""

query_4 = """
WITH weekly AS (
    SELECT 
        strftime('%Y-%W', scraped_at)           AS year_week,
        ROUND(AVG(pct_change), 2)               AS avg_pct_change,
        COUNT(DISTINCT DATE(scraped_at))        AS trading_days
    FROM cleaned_stocks
    WHERE scraped_at >= DATE('now', '-14 days')
    GROUP BY year_week
    ORDER BY year_week DESC
    LIMIT 3
)
SELECT 
    year_week,
    avg_pct_change                              AS this_week_avg_pct_change,
    LAG(avg_pct_change) OVER (ORDER BY year_week) AS previous_week_avg_pct_change,
    ROUND(
        avg_pct_change - LAG(avg_pct_change) OVER (ORDER BY year_week), 
        2
    )                                           AS week_on_week_change
FROM weekly
ORDER BY year_week DESC;
"""

query_5 = """
WITH last_week AS (
    SELECT DISTINCT ticker, name
    FROM cleaned_stocks
    WHERE scraped_at >= DATE('now', '-14 days')
      AND scraped_at < DATE('now', '-7 days')
),
this_week AS (
    SELECT DISTINCT ticker, name
    FROM cleaned_stocks
    WHERE scraped_at >= DATE('now', '-7 days')
)
SELECT 
    COALESCE(l.ticker, t.ticker) AS ticker,
    COALESCE(l.name, t.name)     AS name,
    CASE 
        WHEN l.ticker IS NULL THEN 'New entrant this week'
        WHEN t.ticker IS NULL THEN 'Dropped out this week'
        ELSE 'Continued from last week'
    END AS status
FROM last_week l
FULL OUTER JOIN this_week t ON l.ticker = t.ticker
WHERE l.ticker IS NULL OR t.ticker IS NULL
ORDER BY status, ticker;
"""

# ── Load main data once ────────────────────────────────────────────────────
@st.cache_data(ttl=3600)  # refresh every hour
def load_data():
    conn = sqlite3.connect("stock_data.db")
    df = pd.read_sql("""
        SELECT 
            ticker, name, price, change, pct_change,
            datetime(scraped_at) AS scraped_at,
            cleaned_at
        FROM cleaned_stocks 
        ORDER BY scraped_at DESC
    """, conn)
    
    # Force datetime conversion
    df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
    
    conn.close()
    return df

df = load_data()

if df.empty:
    st.warning("No data yet. Run the ETL pipeline first.")
    st.stop()

last_refresh = df['scraped_at'].max()
st.markdown(f"**Last data refresh**: {last_refresh}")

# Open DB connection once for queries
conn = sqlite3.connect("stock_data.db")

# Q1: Strongest movers today
st.header("Today's Strongest Movers")
latest = df['scraped_at'].max()
today = df[df['scraped_at'] == latest].copy()
today['pct_change_pct'] = today['pct_change'] * 100
top10 = today.sort_values('pct_change', ascending=False).head(10)
fig1 = px.bar(
    top10,
    x='ticker',
    y='pct_change_pct',
    color='pct_change_pct',
    hover_data=['name', 'price', 'change'],
    title="Top 10 by % Change"
)
st.plotly_chart(fig1, use_container_width=True)

# Q2: Market mood
st.header("Today's Market Mood")
avg_pct = round(today['pct_change'].mean() * 100, 2)
gainers = round((today['pct_change'] > 0).mean() * 100, 1)
col1, col2, col3 = st.columns(3)
col1.metric("Avg % Change", f"{avg_pct}%")
col2.metric("% Gainers", f"{gainers}%")
col3.metric("Stocks", len(today))

# Q3: Cumulative this week
st.header("Sustained Strength This Week")
this_week = df[df['scraped_at'] >= pd.Timestamp.now() - pd.Timedelta(days=7)]
if len(this_week['scraped_at'].dt.date.unique()) < 2:
    st.info("Need more days of data.")
else:
    weekly = this_week.groupby(['ticker', 'name']).agg({
        'pct_change': ['count', 'mean', 'sum']
    }).reset_index()
    weekly.columns = ['ticker', 'name', 'days', 'avg_daily_pct', 'cumulative_pct']
    weekly['avg_daily_pct'] *= 100
    weekly['cumulative_pct'] *= 100
    weekly = weekly[weekly['days'] >= 2].sort_values('cumulative_pct', ascending=False).head(10)
    st.dataframe(weekly.style.format({
        'avg_daily_pct': '{:.2f}%',
        'cumulative_pct': '{:.2f}%'
    }))

# Question 4: Week-on-Week Momentum
st.header("4. Week-on-Week Market Momentum")
df_q4 = pd.read_sql(query_4, conn)
if df_q4.empty or len(df_q4) < 2:
    st.info("Not enough data yet — need at least 2 weeks for week-on-week comparison.")
else:
    st.dataframe(df_q4.style.format({
        'this_week_avg_pct_change': '{:.2f}%',
        'previous_week_avg_pct_change': '{:.2f}%',
        'week_on_week_change': '{:+.2f}%'
    }))

    fig4 = px.bar(
        df_q4,
        x='year_week',
        y=['this_week_avg_pct_change', 'previous_week_avg_pct_change'],
        barmode='group',
        title="Avg % Change: This Week vs Previous Week",
        labels={'value': 'Avg % Change', 'variable': 'Week'},
        color_discrete_sequence=['#1f77b4', '#ff7f0e']
    )
    fig4.add_hline(y=0, line_dash="dash", line_color="gray")
    st.plotly_chart(fig4, use_container_width=True)

    change = df_q4.iloc[0]['week_on_week_change']
    if change > 0:
        st.success(f"Momentum is **strengthening** (+{change:.2f}%)")
    elif change < 0:
        st.warning(f"Momentum is **weakening** ({change:.2f}%)")
    else:
        st.info(f"Momentum is stable ({change:.2f}%)")

# Question 5: Rotation in Spotlight
st.header("5. Stocks Entering or Leaving the Spotlight")
df_q5 = pd.read_sql(query_5, conn)
if df_q5.empty:
    st.info("No rotation detected yet — need at least 2 weeks of data.")
else:
    new_count = len(df_q5[df_q5['status'] == 'New entrant this week'])
    dropped_count = len(df_q5[df_q5['status'] == 'Dropped out this week'])

    col1, col2 = st.columns(2)
    col1.metric("New Entrants This Week", new_count)
    col2.metric("Dropped Out This Week", dropped_count)

    def color_status(val):
        if val == 'New entrant this week':
            return 'background-color: #d4edda; color: #155724'
        elif val == 'Dropped out this week':
            return 'background-color: #f8d7da; color: #721c24'
        return ''

    st.dataframe(
        df_q5.style.applymap(color_status, subset=['status'])
    )

conn.close()
