# investing_russian_stocks_login_full.py
# Requires: pip install selenium beautifulsoup4

import time
import csv
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === CONFIGURATION ===
URL = "https://www.investing.com/equities/russia"
USERNAME = "..." # has to be loaded from .env
PASSWORD = "..." # has to be loaded from .env
OUTPUT_FILE = "russian_stocks_prices.csv"

def init_driver():
    """Initialize headless Chrome WebDriver."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(10)
    return driver

def login_via_email(driver):
    """Handles the 'Sign In' → 'Sign In with Email' flow."""
    driver.get(URL)
    wait = WebDriverWait(driver, 20)

    # Click the "Sign In" button at the top right
    sign_in_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.login")))
    sign_in_btn.click()
    time.sleep(3)

    # Click the "Sign in with Email" button in the modal
    sign_in_email_btn = wait.until(EC.element_to_be_clickable(
        (By.XPATH, "//button[contains(., 'Sign in with Email')]")))
    sign_in_email_btn.click()
    time.sleep(3)

    # Switch to the login iframe
    iframe = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='accounts']")))
    driver.switch_to.frame(iframe)

    # Fill email and password
    email_field = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='email']")))
    password_field = driver.find_element(By.CSS_SELECTOR, "input[type='password']")
    email_field.send_keys(USERNAME)
    password_field.send_keys(PASSWORD)
    password_field.send_keys(Keys.RETURN)

    # Wait for login to complete
    driver.switch_to.default_content()
    time.sleep(8)
    print("Logged in successfully.")

def scrape_russian_stocks(driver):
    """Scrapes Russian stock prices."""
    driver.get(URL)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.find("table", {"class": "datatable"})
    results = []
    if not table:
        print("Stocks table not found. The page structure may have changed.")
        return results
    rows = table.find_all("tr")[1:]
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
    """Save scraped data to a CSV file."""
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "price", "change_percent"])
        writer.writeheader()
        writer.writerows(data)
    print(f"Saved {len(data)} stocks to {OUTPUT_FILE}")

def main():
    driver = init_driver()
    try:
        login_via_email(driver)
        data = scrape_russian_stocks(driver)
        if data:
            save_to_csv(data)
        else:
            print("No data scraped.")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
