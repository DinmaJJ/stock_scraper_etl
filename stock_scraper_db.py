import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime

DB_FILE = "stock_data.db"

def scrape_stock_data():
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
            ticker = a_tag.find(class_="COaKTb")
            name = a_tag.find(class_="ZvmM7")
            price = a_tag.find(class_="YMlKec")
            abs_change = a_tag.find(class_="P2Luy")
            pct_span = a_tag.find(class_="JwB6zf")

            stock = {
                "ticker": ticker.get_text(strip=True) if ticker else None,
                "name": name.get_text(strip=True) if name else None,
                "price": price.get_text(strip=True) if price else None,
                "change": abs_change.get_text(strip=True) if abs_change else None,
                "pct_change": pct_span.get_text(strip=True) if pct_span else None,
                "href": a_tag.get("href"),
                "scraped_at": datetime.now().isoformat()
            }
            stocks.append(stock)
        except Exception as e:
            print(f"Error parsing stock: {e}")
            continue

    print(f"Scraped {len(stocks)} stocks at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return stocks

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
        print("No new data to save")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    inserted = 0
    for stock in stocks:
        try:
            cursor.execute('''
                INSERT INTO raw_stocks
                (ticker, name, price, change, pct_change, href, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                stock['ticker'], stock['name'], stock['price'],
                stock['change'], stock['pct_change'], stock['href'],
                stock['scraped_at']
            ))
            inserted += 1
        except Exception as e:
            print(f"Failed to insert stock {stock.get('ticker')}: {e}")
            continue

    conn.commit()
    conn.close()
    print(f"Saved {inserted} new records to database")

def daily_scrape_job():
    today = datetime.now().weekday()  # 0 = Monday, 6 = Sunday
    if today >= 5:
        print("Weekend → skipping scrape")
        return

    print("Starting daily scrape job...")
    stocks = scrape_stock_data()
    save_to_db(stocks)
    print("Daily scrape job completed.")

if __name__ == "__main__":
    init_database()          # Create table if missing
    daily_scrape_job()       # Run once immediately (for local testing)
    print("Script finished.")
