import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import schedule

def scrape_stock_data ():
    url = "https://www.google.com/finance/markets/most-active?hl=en"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch page: {e}")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    ul_container = soup.find('ul', class_='sbnBtf')

    if ul_container is None:
        print("Couldn't find ul.sbnBtf — website structure changed!")
        return []

    stocks = []
    for a_tag in ul_container.find_all("a"):
        try:
            ticker     = a_tag.find(class_="COaKTb")
            name       = a_tag.find(class_="ZvmM7")
            price      = a_tag.find(class_="YMlKec")
            abs_change = a_tag.find(class_="P2Luy")
            pct_span   = a_tag.find(class_="JwB6zf")

            stock = {
                "ticker":     ticker.get_text(strip=True) if ticker else None,
                "name":       name.get_text(strip=True) if name else None,
                "price":      price.get_text(strip=True) if price else None,
                "change":     abs_change.get_text(strip=True) if abs_change else None,
                "pct_change": pct_span.get_text(strip=True) if pct_span else None,
                "href":       a_tag.get("href"),
                "scraped_at": datetime.now().isoformat()
            }
            stocks.append(stock)
        except:
            continue

    print(f"Scraped {len(stocks)} stocks at {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    return stocks



# Database setup

DB_FILE = "stock_data.db"

def init_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            name TEXT,
            price TEXT,
            change TEXT,
            pct_change TEXT,
            href TEXT,
            scraped_at TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()
    print(f"Database initialized/ready: {DB_FILE}")


def save_to_db(stocks):
    if not stocks:
        print("No data to save")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    for stock in stocks:
        cursor.execute('''
            INSERT INTO raw_stocks 
            (ticker, name, price, change, pct_change, href, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            stock['ticker'], stock['name'], stock['price'],
            stock['change'], stock['pct_change'], stock['href'],
            stock['scraped_at']
        ))

    conn.commit()
    conn.close()
    print(f"Saved {len(stocks)} records to database")


# Daily Scrape Job

def daily_scrape_job():
    # Run on weekdays only
    today = datetime.now().weekday()  # 0 = Monday ... 6 = Sunday
    if today >= 5:
        print("Weekend → skipping scrape")
        return

    stocks = scrape_stock_data()
    save_to_db(stocks)


# -------------------------------
# Initialization & Scheduler
# -------------------------------
if __name__ == "__main__":
    init_database()  # Run once to create table

    # For testing: run immediately once
    daily_scrape_job()

    # Schedule to run every day at e.g. 10:05 (adjust to your preferred market time)
    schedule.every().day.at("08:00:00").do(daily_scrape_job)

    print("Scheduler started. Waiting for next run... (press Ctrl+C to stop)")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute