"""
Script to load all Russian stock prices from Investing.com
Required packages: pandas, requests, BeautifulSoup4, lxml
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import json
from datetime import datetime
import os

class RussianStocksScraper:
    def __init__(self):
        self.base_url = "https://ru.investing.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_stock_list(self):
        """
        Get list of Russian stocks from Investing.com
        """
        url = f"{self.base_url}/equities/russia"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            stocks = []
            # Find the table with stock data
            table = soup.find('table', {'id': 'cross_rate_markets_stocks_1'})
            
            if table:
                rows = table.find('tbody').find_all('tr')
                
                for row in rows:
                    try:
                        # Extract stock information
                        cells = row.find_all('td')
                        
                        if len(cells) >= 5:
                            # Get stock name and symbol
                            name_cell = cells[1].find('a')
                            if name_cell:
                                stock_name = name_cell.text.strip()
                                stock_link = name_cell.get('href', '')
                                stock_symbol = stock_link.split('/')[-1] if stock_link else ''
                                
                                # Get last price
                                last_price = cells[2].text.strip()
                                
                                # Get change and change percentage
                                change = cells[3].text.strip()
                                change_percent = cells[4].text.strip()
                                
                                stocks.append({
                                    'name': stock_name,
                                    'symbol': stock_symbol,
                                    'last_price': self._clean_price(last_price),
                                    'change': self._clean_price(change),
                                    'change_percent': change_percent,
                                    'url': f"{self.base_url}{stock_link}" if stock_link else '',
                                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                })
                    except Exception as e:
                        print(f"Error parsing row: {e}")
                        continue
            
            return stocks
            
        except Exception as e:
            print(f"Error fetching stock list: {e}")
            return []

    def get_detailed_stock_data(self, stock_url):
        """
        Get detailed data for a specific stock
        """
        try:
            response = self.session.get(stock_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            stock_data = {}
            
            # Get current price
            price_element = soup.find('span', {'data-test': 'instrument-price-last'})
            if price_element:
                stock_data['current_price'] = self._clean_price(price_element.text)
            
            # Get additional metrics
            metrics = {}
            metric_elements = soup.find_all('div', {'class': 'instrument-metals_value'})
            
            # Common metrics to look for
            metric_labels = ['Открытие', 'Пред. закр.', 'Объем', 'Ср. объем', 'Изменение', 'Дн. диапазон']
            
            for i, element in enumerate(metric_elements[:len(metric_labels)]):
                if i < len(metric_labels):
                    metrics[metric_labels[i]] = element.text.strip()
            
            stock_data['metrics'] = metrics
            
            return stock_data
            
        except Exception as e:
            print(f"Error fetching detailed data: {e}")
            return {}

    def _clean_price(self, price_str):
        """
        Clean price string and convert to float if possible
        """
        try:
            # Remove any non-numeric characters except decimal point and minus
            cleaned = ''.join(c for c in price_str if c.isdigit() or c in '.-,').replace(',', '')
            return float(cleaned) if cleaned else price_str
        except:
            return price_str

    def save_to_csv(self, stocks_data, filename=None):
        """
        Save stock data to CSV file
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'russian_stocks_{timestamp}.csv'
        
        df = pd.DataFrame(stocks_data)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Data saved to {filename}")
        return filename

    def save_to_json(self, stocks_data, filename=None):
        """
        Save stock data to JSON file
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'russian_stocks_{timestamp}.json'
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(stocks_data, f, ensure_ascii=False, indent=2)
        print(f"Data saved to {filename}")
        return filename

def main():
    """
    Main function to run the scraper
    """
    print("Starting Russian Stocks Scraper...")
    
    # Create output directory
    os.makedirs('output', exist_ok=True)
    
    scraper = RussianStocksScraper()
    
    print("Fetching stock list...")
    stocks = scraper.get_stock_list()
    
    if not stocks:
        print("No stocks found. Please check the website structure or your internet connection.")
        return
    
    print(f"Found {len(stocks)} stocks")
    
    # Get detailed data for each stock (with delay to be respectful)
    print("Fetching detailed data...")
    detailed_stocks = []
    
    for i, stock in enumerate(stocks):
        print(f"Processing {i+1}/{len(stocks)}: {stock['name']}")
        
        if stock['url']:
            detailed_data = scraper.get_detailed_stock_data(stock['url'])
            stock.update(detailed_data)
            
        detailed_stocks.append(stock)
        
        # Add delay to avoid overwhelming the server
        time.sleep(1)
    
    # Save data
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    csv_file = scraper.save_to_csv(detailed_stocks, f'output/russian_stocks_{timestamp}.csv')
    json_file = scraper.save_to_json(detailed_stocks, f'output/russian_stocks_{timestamp}.json')
    
    # Display summary
    print(f"\n=== Summary ===")
    print(f"Total stocks processed: {len(detailed_stocks)}")
    print(f"CSV file: {csv_file}")
    print(f"JSON file: {json_file}")
    
    # Show first few stocks as preview
    if detailed_stocks:
        print(f"\n=== Sample Data ===")
        for i, stock in enumerate(detailed_stocks[:3]):
            print(f"{i+1}. {stock['name']}: {stock.get('last_price', 'N/A')} "
                  f"({stock.get('change_percent', 'N/A')})")

if __name__ == "__main__":
    main()
