"""
Comprehensive Russian Companies Data Parser from Investing.com
Fetches detailed information about Russian companies including financials, metrics, and profiles
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import json
import re
from datetime import datetime
import os
from typing import Dict, List, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RussianCompaniesParser:
    def __init__(self):
        self.base_url = "https://ru.investing.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
    def get_all_companies_list(self) -> List[Dict]:
        """
        Get comprehensive list of all Russian companies from multiple sections
        """
        companies = []
        
        # Different sections on Investing.com for Russian companies
        sections = [
            "/equities/russia",
            "/equities/moscow-exchange",
            "/equities/stocks-russia",
            "/etfs/russia",
            "/funds/russia"
        ]
        
        for section in sections:
            try:
                logger.info(f"Scraping section: {section}")
                url = f"{self.base_url}{section}"
                response = self.session.get(url)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Try different table IDs and structures
                table_selectors = [
                    {'id': 'cross_rate_markets_stocks_1'},
                    {'class': 'genTbl'},
                    {'class': 'results'},
                    {'id': 'etfs'},
                    {'id': 'funds'}
                ]
                
                for selector in table_selectors:
                    table = soup.find('table', selector)
                    if table:
                        companies.extend(self._parse_companies_table(table, section))
                        break
                
                time.sleep(2)  # Be respectful to the server
                
            except Exception as e:
                logger.error(f"Error scraping section {section}: {e}")
                continue
                
        return self._deduplicate_companies(companies)
    
    def _parse_companies_table(self, table, section: str) -> List[Dict]:
        """Parse companies from HTML table"""
        companies = []
        
        try:
            rows = table.find('tbody').find_all('tr')
            
            for row in rows:
                try:
                    cells = row.find_all('td')
                    if len(cells) < 3:
                        continue
                    
                    # Extract company basic info
                    name_cell = cells[1].find('a') if len(cells) > 1 else None
                    if not name_cell:
                        continue
                    
                    company_name = name_cell.text.strip()
                    company_url = name_cell.get('href', '')
                    symbol = company_url.split('/')[-1] if company_url else ''
                    
                    # Extract basic metrics
                    last_price = cells[2].text.strip() if len(cells) > 2 else 'N/A'
                    change = cells[3].text.strip() if len(cells) > 3 else 'N/A'
                    change_percent = cells[4].text.strip() if len(cells) > 4 else 'N/A'
                    
                    company_data = {
                        'company_name': company_name,
                        'symbol': symbol,
                        'url': f"{self.base_url}{company_url}" if company_url else '',
                        'section': section.replace('/equities/', '').replace('/', ''),
                        'last_price': self._clean_numeric(last_price),
                        'change': self._clean_numeric(change),
                        'change_percent': self._clean_numeric(change_percent),
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                    companies.append(company_data)
                    
                except Exception as e:
                    logger.warning(f"Error parsing company row: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error parsing table: {e}")
            
        return companies
    
    def get_company_detailed_data(self, company_url: str) -> Dict:
        """
        Get comprehensive detailed data for a specific company
        """
        try:
            logger.info(f"Fetching detailed data from: {company_url}")
            response = self.session.get(company_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            detailed_data = {}
            
            # 1. Current price and basic info
            detailed_data.update(self._get_current_price_data(soup))
            
            # 2. Company profile and description
            detailed_data.update(self._get_company_profile(soup))
            
            # 3. Key financial metrics
            detailed_data.update(self._get_financial_metrics(soup))
            
            # 4. Technical indicators
            detailed_data.update(self._get_technical_indicators(soup))
            
            # 5. Financial statements data
            detailed_data.update(self._get_financial_statements(soup, company_url))
            
            # 6. Dividends information
            detailed_data.update(self._get_dividends_data(soup, company_url))
            
            # 7. News and analysis sentiment
            detailed_data.update(self._get_news_sentiment(soup))
            
            return detailed_data
            
        except Exception as e:
            logger.error(f"Error fetching detailed data for {company_url}: {e}")
            return {}
    
    def _get_current_price_data(self, soup: BeautifulSoup) -> Dict:
        """Extract current price and market data"""
        price_data = {}
        
        try:
            # Current price
            price_element = soup.find('span', {'data-test': 'instrument-price-last'})
            if price_element:
                price_data['current_price'] = self._clean_numeric(price_element.text)
            
            # Price change
            change_element = soup.find('span', {'data-test': 'instrument-price-change'})
            if change_element:
                price_data['price_change'] = self._clean_numeric(change_element.text)
            
            # Change percentage
            change_percent_element = soup.find('span', {'data-test': 'instrument-price-change-percent'})
            if change_percent_element:
                price_data['change_percent'] = self._clean_numeric(change_percent_element.text)
            
            # Additional metrics
            metrics_grid = soup.find('div', {'class': 'instrument-grid'})
            if metrics_grid:
                metrics = metrics_grid.find_all('div', {'class': 'instrument-grid_item'})
                for metric in metrics:
                    label = metric.find('span', {'class': 'instrument-grid_label'})
                    value = metric.find('span', {'class': 'instrument-grid_value'})
                    if label and value:
                        key = self._clean_key(label.text.strip())
                        price_data[key] = self._clean_numeric(value.text.strip())
                        
        except Exception as e:
            logger.warning(f"Error extracting price data: {e}")
            
        return price_data
    
    def _get_company_profile(self, soup: BeautifulSoup) -> Dict:
        """Extract company profile information"""
        profile = {}
        
        try:
            # Company description
            desc_element = soup.find('div', {'class': 'companyProfile'})
            if desc_element:
                profile['description'] = desc_element.text.strip()[:1000]  # Limit length
            
            # Company details table
            details_table = soup.find('table', {'class': 'companyDetails'})
            if details_table:
                rows = details_table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) == 2:
                        key = self._clean_key(cells[0].text.strip())
                        value = cells[1].text.strip()
                        profile[key] = value
            
            # Sector and industry
            sector_elements = soup.find_all('a', href=re.compile(r'/sector/'))
            if sector_elements:
                profile['sector'] = sector_elements[0].text.strip()
                
        except Exception as e:
            logger.warning(f"Error extracting company profile: {e}")
            
        return profile
    
    def _get_financial_metrics(self, soup: BeautifulSoup) -> Dict:
        """Extract key financial metrics"""
        financials = {}
        
        try:
            # Look for financial metrics in various sections
            sections = soup.find_all('div', {'class': re.compile(r'financial|metric|ratio')})
            
            for section in sections:
                # Try to find key-value pairs
                items = section.find_all(['li', 'div'], class_=re.compile(r'item|metric'))
                for item in items:
                    text = item.get_text(strip=True)
                    # Look for patterns like "P/E Ratio: 15.2"
                    if ':' in text:
                        key, value = text.split(':', 1)
                        clean_key = self._clean_key(key.strip())
                        financials[clean_key] = self._clean_numeric(value.strip())
                        
        except Exception as e:
            logger.warning(f"Error extracting financial metrics: {e}")
            
        return financials
    
    def _get_technical_indicators(self, soup: BeautifulSoup) -> Dict:
        """Extract technical indicators"""
        technicals = {}
        
        try:
            # Look for technical analysis sections
            tech_sections = soup.find_all('div', text=re.compile(r'технический|technical', re.IGNORECASE))
            
            for section in tech_sections:
                parent = section.find_parent('div')
                if parent:
                    indicators = parent.find_all('span', class_=re.compile(r'indicator|value'))
                    for indicator in indicators:
                        text = indicator.get_text(strip=True)
                        if any(term in text.lower() for term in ['rsi', 'macd', 'moving average', 'support', 'resistance']):
                            technicals[self._clean_key(text)] = True
                            
        except Exception as e:
            logger.warning(f"Error extracting technical indicators: {e}")
            
        return technicals
    
    def _get_financial_statements(self, soup: BeautifulSoup, company_url: str) -> Dict:
        """Extract financial statements summary"""
        statements = {}
        
        try:
            # Try to find links to financial statements
            fin_links = soup.find_all('a', href=re.compile(r'financial|statement|balance|income'))
            
            for link in fin_links:
                if any(term in link.text.lower() for term in ['отчетность', 'financial', 'statement']):
                    statements['has_financial_statements'] = True
                    break
                    
        except Exception as e:
            logger.warning(f"Error extracting financial statements info: {e}")
            
        return statements
    
    def _get_dividends_data(self, soup: BeautifulSoup, company_url: str) -> Dict:
        """Extract dividends information"""
        dividends = {}
        
        try:
            # Look for dividends information
            div_elements = soup.find_all(text=re.compile(r'дивиденд|dividend', re.IGNORECASE))
            
            for element in div_elements:
                parent = element.parent
                if parent:
                    # Try to find dividend value
                    value_element = parent.find_next('span', class_=re.compile(r'value|data'))
                    if value_element:
                        dividends['dividend'] = self._clean_numeric(value_element.text)
                        break
                        
        except Exception as e:
            logger.warning(f"Error extracting dividends data: {e}")
            
        return dividends
    
    def _get_news_sentiment(self, soup: BeautifulSoup) -> Dict:
        """Extract news sentiment and analysis"""
        sentiment = {}
        
        try:
            # Look for sentiment indicators
            sentiment_elements = soup.find_all('div', class_=re.compile(r'sentiment|analysis'))
            
            for element in sentiment_elements:
                text = element.get_text(strip=True).lower()
                if 'buy' in text or 'покупка' in text:
                    sentiment['analyst_sentiment'] = 'buy'
                elif 'sell' in text or 'продажа' in text:
                    sentiment['analyst_sentiment'] = 'sell'
                elif 'hold' in text or 'удержание' in text:
                    sentiment['analyst_sentiment'] = 'hold'
                    
        except Exception as e:
            logger.warning(f"Error extracting news sentiment: {e}")
            
        return sentiment
    
    def _clean_numeric(self, value: str) -> float:
        """Clean and convert numeric values"""
        try:
            if value == 'N/A' or not value:
                return None
            # Remove non-numeric characters except decimal point and minus
            cleaned = re.sub(r'[^\d.,-]', '', value.replace(',', '.'))
            # Handle cases like "15.2K" -> 15200
            if 'K' in value:
                cleaned = cleaned.replace('K', '')
                return float(cleaned) * 1000
            elif 'M' in value:
                cleaned = cleaned.replace('M', '')
                return float(cleaned) * 1000000
            elif 'B' in value:
                cleaned = cleaned.replace('B', '')
                return float(cleaned) * 1000000000
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return value
    
    def _clean_key(self, key: str) -> str:
        """Clean keys for consistent naming"""
        # Convert to lowercase and replace spaces with underscores
        cleaned = re.sub(r'[^\w\s]', '', key.lower())
        cleaned = re.sub(r'\s+', '_', cleaned.strip())
        return cleaned
    
    def _deduplicate_companies(self, companies: List[Dict]) -> List[Dict]:
        """Remove duplicate companies based on symbol"""
        seen = set()
        unique_companies = []
        
        for company in companies:
            symbol = company.get('symbol')
            if symbol and symbol not in seen:
                seen.add(symbol)
                unique_companies.append(company)
                
        return unique_companies
    
    def save_comprehensive_data(self, companies_data: List[Dict], format_type: str = 'all'):
        """Save data in multiple formats"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        os.makedirs('output', exist_ok=True)
        
        base_filename = f'output/russian_companies_comprehensive_{timestamp}'
        
        if format_type in ['csv', 'all']:
            # Flatten data for CSV
            flattened_data = []
            for company in companies_data:
                flat_company = company.copy()
                # Flatten nested dictionaries
                for key, value in company.items():
                    if isinstance(value, dict):
                        for subkey, subvalue in value.items():
                            flat_company[f"{key}_{subkey}"] = subvalue
                        del flat_company[key]
                flattened_data.append(flat_company)
            
            df = pd.DataFrame(flattened_data)
            csv_file = f"{base_filename}.csv"
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            logger.info(f"Data saved to {csv_file}")
        
        if format_type in ['json', 'all']:
            json_file = f"{base_filename}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(companies_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Data saved to {json_file}")
        
        if format_type in ['excel', 'all']:
            try:
                excel_file = f"{base_filename}.xlsx"
                with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                    # Create different sheets for different data types
                    main_data = pd.DataFrame(companies_data)
                    main_data.to_excel(writer, sheet_name='Companies', index=False)
                    
                    # Create summary sheet
                    summary_data = self._create_summary(companies_data)
                    summary_data.to_excel(writer, sheet_name='Summary', index=False)
                    
                logger.info(f"Data saved to {excel_file}")
            except ImportError:
                logger.warning("openpyxl not installed, skipping Excel export")
        
        return base_filename
    
    def _create_summary(self, companies_data: List[Dict]) -> pd.DataFrame:
        """Create summary statistics"""
        summary = {
            'total_companies': len(companies_data),
            'scraping_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'sectors_covered': len(set(c.get('sector', '') for c in companies_data)),
            'companies_with_prices': len([c for c in companies_data if c.get('current_price')]),
            'companies_with_financials': len([c for c in companies_data if c.get('has_financial_statements')]),
        }
        return pd.DataFrame([summary])

def main():
    """Main execution function"""
    logger.info("Starting comprehensive Russian companies data parser...")
    
    parser = RussianCompaniesParser()
    
    # Step 1: Get all companies list
    logger.info("Step 1: Fetching companies list...")
    companies = parser.get_all_companies_list()
    
    if not companies:
        logger.error("No companies found. Exiting.")
        return
    
    logger.info(f"Found {len(companies)} unique companies")
    
    # Step 2: Get detailed data for each company
    logger.info("Step 2: Fetching detailed company data...")
    comprehensive_data = []
    
    for i, company in enumerate(companies):
        logger.info(f"Processing {i+1}/{len(companies)}: {company['company_name']}")
        
        if company.get('url'):
            detailed_info = parser.get_company_detailed_data(company['url'])
            company.update(detailed_info)
        
        comprehensive_data.append(company)
        
        # Respectful delay
        time.sleep(3)
        
        # Save progress every 10 companies
        if (i + 1) % 10 == 0:
            logger.info(f"Progress: {i+1}/{len(companies)} companies processed")
    
    # Step 3: Save all data
    logger.info("Step 3: Saving comprehensive data...")
    parser.save_comprehensive_data(comprehensive_data)
    
    # Final summary
    logger.info("=== SCRAPING COMPLETED ===")
    logger.info(f"Total companies processed: {len(comprehensive_data)}")
    logger.info(f"Data saved to output/ directory")
    
    # Display sample
    if comprehensive_data:
        sample = comprehensive_data[0]
        logger.info(f"Sample company: {sample.get('company_name')}")
        logger.info(f"Price: {sample.get('current_price')}")
        logger.info(f"Sector: {sample.get('sector', 'N/A')}")
        logger.info(f"Metrics collected: {len([k for k in sample.keys() if not k.startswith('_')])}")

if __name__ == "__main__":
    main()
