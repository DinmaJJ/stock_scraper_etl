import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import schedule
from playwright.sync_api import sync_playwright

def scrape_stock_data():
    url = "https://www.google.com/finance/markets/most-active?hl=en"

    stocks = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
                viewport={'width': 1280, 'height': 800}
            )
            page = context.new_page()
            page.goto(url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(3000)  # Give extra time for dynamic content

            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, 'html.parser')

        # Find the list container (one of the classes you saw)
        container = soup.find('ul', class_='sbnBtf') or soup.find('ul', class_='LTfwk')

        if not container:
            print("Could not find stock list container.")
            print("Page title:", soup.title.string)
            return []

        # Loop through each stock item
        for item in container.find_all('li'):
            try:
                # Ticker (often in a span/div with COaKTb or similar)
                ticker_tag = item.find(class_='COaKTb') or item.find('div', class_='ticker-class') or item.find('span', string=lambda t: t and len(t.strip()) <= 6 and t.isupper())
                ticker = ticker_tag.get_text(strip=True) if ticker_tag else None

                # Name
                name_tag = item.find(class_='ZvmM7')
                name = name_tag.get_text(strip=True) if name_tag else None

                # Price
                price_tag = item.find(class_='YMlKec')
                price = price_tag.get_text(strip=True) if price_tag else None

                # Change
                change_tag = item.find(class_='P2Luy')
                change = change_tag.get_text(strip=True) if change_tag else None

                # % Change
                pct_tag = item.find(class_='JwB6zf')
                pct_change = pct_tag.get_text(strip=True) if pct_tag else None

                # Link
                link_tag = item.find('a')
                href = link_tag['href'] if link_tag else None

                if ticker and price:
                    stocks.append({
                        "ticker": ticker,
                        "name": name,
                        "price": price,
                        "change": change,
                        "pct_change": pct_change,
                        "href": href,
                        "scraped_at": datetime.now().isoformat()
                    })
            except Exception as e:
                print(f"Error parsing one stock item: {e}")
                continue

        print(f"Scraped {len(stocks)} stocks at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if stocks:
            print("First stock sample:", stocks[0])

        return stocks

    except Exception as e:
        print(f"Playwright / fetch error: {e}")
        return []
    
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
    print("Forcing scrape job (weekday check disabled for testing)")
    stocks = scrape_stock_data()
    save_to_db(stocks)


# -------------------------------
# Initialization & Scheduler
# -------------------------------
if __name__ == "__main__":
    init_database()  # Run once to create table

    # For testing: run immediately once
    daily_scrape_job()

    
