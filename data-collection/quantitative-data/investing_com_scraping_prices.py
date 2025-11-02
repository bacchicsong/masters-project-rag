import time
import csv
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

# === CONFIGURATION ===
INVESTING_URL = "https://www.investing.com/"
RUSSIAN_STOCKS_URL = "https://www.investing.com/equities/russia"
USERNAME = "..."
PASSWORD = "..."
OUTPUT_FILE = "russian_stocks_prices.csv"

def init_driver():
    """Initialize a headless Chrome browser."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(10)
    return driver

def login_investing(driver):
    """Log in to investing.com using provided credentials."""
    driver.get(INVESTING_URL)
    time.sleep(3)

    # Open login menu
    login_button = driver.find_element(By.CSS_SELECTOR, "a.login")
    login_button.click()
    time.sleep(2)

    # Fill in credentials (popup iframe)
    iframe = driver.find_element(By.CSS_SELECTOR, "iframe[id^='loginPopup']")
    driver.switch_to.frame(iframe)

    email_field = driver.find_element(By.CSS_SELECTOR, "input[type='email']")
    password_field = driver.find_element(By.CSS_SELECTOR, "input[type='password']")

    email_field.send_keys(USERNAME)
    password_field.send_keys(PASSWORD)
    password_field.send_keys(Keys.RETURN)
    time.sleep(5)

    driver.switch_to.default_content()
    print("Logged in successfully.")

def get_russian_stocks(driver):
    """Scrape Russian equities list."""
    driver.get(RUSSIAN_STOCKS_URL)
    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # The main table of Russian equities
    table = soup.find("table", {"class": "datatable"})
    results = []

    if not table:
        print("Could not find stocks table — page structure may have changed.")
        return results

    rows = table.find_all("tr")[1:]  # skip header
    for r in rows:
        cols = r.find_all("td")
        if len(cols) < 6:
            continue
        name = cols[1].get_text(strip=True)
        price = cols[2].get_text(strip=True)
        change = cols[5].get_text(strip=True)
        results.append({
            "name": name,
            "price": price,
            "change_percent": change
        })

    return results

def save_to_csv(data):
    """Save data to CSV file."""
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "price", "change_percent"])
        writer.writeheader()
        writer.writerows(data)
    print(f"Saved {len(data)} stocks to {OUTPUT_FILE}")

def main():
    driver = init_driver()
    try:
        login_investing(driver)
        stocks = get_russian_stocks(driver)
        if stocks:
            save_to_csv(stocks)
        else:
            print("No stock data scraped.")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
    
