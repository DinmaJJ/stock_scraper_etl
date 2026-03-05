import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from datetime import datetime, timedelta
import urllib.request
import hashlib
import os

st.set_page_config(page_title="Stock Dashboard", layout="wide")
st.title("Stock Most Active Dashboard")

# ── GCS Configuration ──────────────────────────────────────────────────────
DB_URL = "https://storage.googleapis.com/chidinma-stock-db-2026/stock_data.db"
LOCAL_DB_PATH = "stock_data.db"

# ── Load data with hash-based cache invalidation ───────────────────────────
@st.cache_data(show_spinner="Downloading & validating latest stock data...")
def load_data():
    try:
        with urllib.request.urlopen(DB_URL) as response:
            content = response.read()
            file_hash = hashlib.md5(content).hexdigest()

        # Write to disk
        with open(LOCAL_DB_PATH, "wb") as f:
            f.write(content)

        conn = sqlite3.connect(LOCAL_DB_PATH)
        df = pd.read_sql_query("""
            SELECT
                ticker, name, price, change, pct_change,
                datetime(scraped_at) AS scraped_at,
                cleaned_at
            FROM cleaned_stocks
            ORDER BY scraped_at DESC
        """, conn)
        
        df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
        conn.close()

        st.caption(f"Loaded fresh data from GCS • File hash: {file_hash[:8]}...")
        return df, file_hash, df['scraped_at'].max()

    except Exception as e:
        st.error(f"Failed to download or process database: {e}")
        # Fallback: try to use existing local file if any
        if os.path.exists(LOCAL_DB_PATH):
            try:
                conn = sqlite3.connect(LOCAL_DB_PATH)
                df = pd.read_sql_query("SELECT * FROM cleaned_stocks LIMIT 1", conn)
                conn.close()
                if not df.empty:
                    st.warning("Using previously downloaded local copy (may be outdated).")
                    return df, "fallback-local", df['scraped_at'].max() if 'scraped_at' in df else None
            except:
                pass
        return pd.DataFrame(), "error", None

# Load the data
df, current_hash, last_refresh = load_data()

if df.empty:
    st.warning("No data available yet. Please run the ETL pipeline first.")
    st.stop()

# Show refresh info
st.markdown(f"**Last data refresh**: {last_refresh.strftime('%Y-%m-%d %H:%M:%S') if last_refresh else 'Unknown'}")

# ── Define queries ─────────────────────────────────────────────────────────
query_1 = """
SELECT ticker, name, price, change, pct_change
FROM cleaned_stocks
WHERE scraped_at = (SELECT MAX(scraped_at) FROM cleaned_stocks)
ORDER BY pct_change DESC
LIMIT 10;
"""

query_2 = """
SELECT
    DATE(scraped_at) AS trade_date,
    COUNT(*) AS number_of_stocks,
    ROUND(AVG(pct_change), 2) AS avg_pct_change,
    ROUND(MIN(pct_change), 2) AS worst_pct_change,
    ROUND(MAX(pct_change), 2) AS best_pct_change,
    ROUND(
        SUM(CASE WHEN pct_change > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        1
    ) AS percentage_of_gainers
FROM cleaned_stocks
WHERE scraped_at = (SELECT MAX(scraped_at) FROM cleaned_stocks)
GROUP BY DATE(scraped_at);
"""

query_3 = """
SELECT
    ticker,
    name,
    COUNT(*) AS days_appeared,
    ROUND(AVG(pct_change), 2) AS avg_daily_pct_change,
    ROUND(SUM(pct_change), 2) AS cumulative_pct_this_week,
    ROUND(MIN(pct_change), 2) AS worst_day_pct,
    ROUND(MAX(pct_change), 2) AS best_day_pct
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
        strftime('%Y-%W', scraped_at) AS year_week,
        ROUND(AVG(pct_change), 2) AS avg_pct_change,
        COUNT(DISTINCT DATE(scraped_at)) AS trading_days
    FROM cleaned_stocks
    WHERE scraped_at >= DATE('now', '-14 days')
    GROUP BY year_week
    ORDER BY year_week DESC
    LIMIT 3
)
SELECT
    year_week,
    avg_pct_change AS this_week_avg_pct_change,
    LAG(avg_pct_change) OVER (ORDER BY year_week) AS previous_week_avg_pct_change,
    ROUND(
        avg_pct_change - LAG(avg_pct_change) OVER (ORDER BY year_week),
        2
    ) AS week_on_week_change
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
    COALESCE(l.name, t.name) AS name,
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

# ── Visualizations ─────────────────────────────────────────────────────────

# Q1: Today's Strongest Movers
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

# Q2: Market Mood
st.header("Today's Market Mood")
avg_pct = round(today['pct_change'].mean() * 100, 2) if not today.empty else 0
gainers = round((today['pct_change'] > 0).mean() * 100, 1) if not today.empty else 0
col1, col2, col3 = st.columns(3)
col1.metric("Avg % Change", f"{avg_pct}%")
col2.metric("% Gainers", f"{gainers}%")
col3.metric("Stocks", len(today))

# Q3: Sustained Strength This Week
st.header("Sustained Strength This Week")
this_week = df[df['scraped_at'] >= pd.Timestamp.now() - pd.Timedelta(days=7)]
if len(this_week['scraped_at'].dt.date.unique()) < 2:
    st.info("Need more days of data for weekly trends.")
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

# Q4: Week-on-Week Momentum
st.header("Week-on-Week Market Momentum")
conn = sqlite3.connect(LOCAL_DB_PATH)
df_q4 = pd.read_sql(query_4, conn)
conn.close()

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

# Q5: Rotation in Spotlight
st.header("Stocks Entering or Leaving the Spotlight")
conn = sqlite3.connect(LOCAL_DB_PATH)
df_q5 = pd.read_sql(query_5, conn)
conn.close()

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
