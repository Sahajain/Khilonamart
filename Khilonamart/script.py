import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from urllib.parse import urljoin
import csv
import logging
from datetime import datetime
import re

class AmazonToysScraper:
    def __init__(self):
        self.base_url = "https://www.amazon.in"
        self.session = requests.Session()
        
        # Enhanced headers to better mimic a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        
        self.session.headers.update(self.headers)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.products_data = []
    
    def get_random_delay(self, min_delay=2, max_delay=5):
        """Random delay between requests to be respectful"""
        return random.uniform(min_delay, max_delay)
    
    def make_request_with_retry(self, url, max_retries=3):
        """Make request with retry logic and exponential backoff"""
        for attempt in range(max_retries):
            try:
                delay = self.get_random_delay()
                self.logger.info(f"Waiting {delay:.2f} seconds before request...")
                time.sleep(delay)
                
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 503:
                    wait_time = (2 ** attempt) + random.uniform(1, 3)
                    self.logger.warning(f"503 error on attempt {attempt + 1}, waiting {wait_time:.2f} seconds")
                    time.sleep(wait_time)
                else:
                    self.logger.warning(f"Status code {response.status_code} on attempt {attempt + 1}")
                    time.sleep(2 ** attempt)
                    
            except requests.RequestException as e:
                self.logger.error(f"Request error on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return None
    
    def clean_text(self, text):
        """Clean and normalize text data"""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text.strip())
    
    def extract_price(self, price_text):
        """Extract numeric price from price string"""
        if not price_text:
            return None
        
        # Remove currency symbols and extract numbers
        price_match = re.search(r'[\d,]+(?:\.\d{2})?', price_text.replace(',', ''))
        if price_match:
            return float(price_match.group().replace(',', ''))
        return None
    
    def extract_rating(self, rating_text):
        """Extract numeric rating"""
        if not rating_text:
            return None
        
        rating_match = re.search(r'(\d+\.?\d*)', rating_text)
        if rating_match:
            return float(rating_match.group())
        return None
    
    def extract_review_count(self, review_text):
        """Extract number of reviews"""
        if not review_text:
            return None
        
        # Look for patterns like "1,234 reviews" or "(1,234)"
        review_match = re.search(r'([\d,]+)', review_text.replace(',', ''))
        if review_match:
            return int(review_match.group().replace(',', ''))
        return None
    
    def scrape_product_details(self, product_element):
        """Extract the 5 essential product details with improved selectors"""
        try:
            product_data = {}
            
            # Debug: Print the HTML structure of first few products
            if len(self.products_data) < 2:
                self.logger.info(f"Product HTML sample: {str(product_element)[:500]}...")
            
            # 1. Product Name - Try multiple selectors
            name_selectors = [
                'h2 a span',
                'h2 span',
                '[data-cy="title-recipe-title"] span',
                '.a-size-mini span',
                '.a-size-base-plus',
                '.s-size-mini span',
                'h2.a-size-mini span'
            ]
            
            name_element = None
            product_url = ""
            
            for selector in name_selectors:
                name_element = product_element.select_one(selector)
                if name_element:
                    break
            
            # Try finding the link for URL
            link_element = product_element.select_one('h2 a') or product_element.select_one('a[href*="/dp/"]')
            if link_element:
                relative_url = link_element.get('href', '')
                product_url = urljoin(self.base_url, relative_url)
            
            if name_element:
                product_data['Product_Name'] = self.clean_text(name_element.get_text())
            else:
                product_data['Product_Name'] = ""
            
            product_data['Product_URL'] = product_url
            
            # 2. Price - Try multiple selectors
            price_selectors = [
                '.a-price-whole',
                '.a-offscreen',
                '.a-price .a-offscreen',
                '[data-a-color="price"] .a-offscreen',
                '.a-price-symbol + .a-price-whole'
            ]
            
            price_element = None
            for selector in price_selectors:
                price_element = product_element.select_one(selector)
                if price_element:
                    break
            
            if price_element:
                price_text = price_element.get_text()
                product_data['Price'] = self.extract_price(price_text)
            else:
                product_data['Price'] = None
            
            # 3. Rating - Try multiple selectors
            rating_selectors = [
                '.a-icon-alt',
                '[aria-label*="stars"]',
                '[aria-label*="rating"]',
                '.a-star-small .a-icon-alt'
            ]
            
            rating_element = None
            for selector in rating_selectors:
                rating_element = product_element.select_one(selector)
                if rating_element:
                    break
            
            if rating_element:
                rating_text = rating_element.get('aria-label', '') or rating_element.get_text()
                product_data['Rating'] = self.extract_rating(rating_text)
            else:
                product_data['Rating'] = None
            
            # 4. Number of Reviews - Try multiple selectors
            review_selectors = [
                'a[href*="#customerReviews"]',
                '[aria-label*="review"]',
                'span.a-size-base',
                'a.a-link-normal'
            ]
            
            review_element = None
            for selector in review_selectors:
                review_element = product_element.select_one(selector)
                if review_element:
                    review_text = review_element.get_text()
                    if 'review' in review_text.lower() or any(char.isdigit() for char in review_text):
                        break
                else:
                    review_element = None
            
            if review_element:
                review_text = review_element.get_text()
                product_data['Number_of_Reviews'] = self.extract_review_count(review_text)
            else:
                product_data['Number_of_Reviews'] = None
            
            # Log first successful extraction for debugging
            if len(self.products_data) == 0 and product_data.get('Product_Name'):
                self.logger.info(f"First product extracted successfully: {product_data}")
            
            return product_data
            
        except Exception as e:
            self.logger.error(f"Error extracting product details: {str(e)}")
            return None
    
    def scrape_toys_category(self, max_pages=25, max_products=500):
        """Scrape toys and gifts category with improved logic"""
        
        # URL for Toys & Games category on Amazon India
        base_search_url = "https://www.amazon.in/s"
        search_params = {
            'k': 'toys and gifts',
            'ref': 'sr_pg_1'
        }
        
        self.logger.info(f"Starting to scrape Amazon India Toys & Gifts category...")
        self.logger.info(f"Target: {max_products} products across {max_pages} pages")
        
        page_num = 1
        products_scraped = 0
        consecutive_failures = 0
        
        while page_num <= max_pages and products_scraped < max_products and consecutive_failures < 3:
            try:
                # Construct page URL
                if page_num == 1:
                    page_url = f"{base_search_url}?k=toys+and+gifts&ref=sr_pg_1"
                else:
                    page_url = f"{base_search_url}?k=toys+and+gifts&page={page_num}&ref=sr_pg_{page_num}"
                
                self.logger.info(f"Scraping page {page_num}: {page_url}")
                
                # Make request with retry logic
                response = self.make_request_with_retry(page_url)
                
                if not response:
                    self.logger.error(f"Failed to get response for page {page_num} after retries")
                    consecutive_failures += 1
                    page_num += 1
                    continue
                
                # Parse HTML
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Try multiple selectors for product containers
                product_selectors = [
                    '[data-component-type="s-search-result"]',
                    '.s-result-item[data-asin]',
                    '[data-asin]:not([data-asin=""])',
                    '.s-widget-container .s-result-item'
                ]
                
                product_containers = []
                for selector in product_selectors:
                    product_containers = soup.select(selector)
                    if product_containers:
                        self.logger.info(f"Found products using selector: {selector}")
                        break
                
                if not product_containers:
                    self.logger.warning(f"No products found on page {page_num}")
                    # Save page HTML for debugging
                    with open(f'debug_page_{page_num}.html', 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    self.logger.info(f"Page HTML saved to debug_page_{page_num}.html for analysis")
                    consecutive_failures += 1
                    page_num += 1
                    continue
                
                self.logger.info(f"Found {len(product_containers)} products on page {page_num}")
                consecutive_failures = 0  # Reset on success
                
                # Process each product
                page_products_added = 0
                for i, container in enumerate(product_containers):
                    if products_scraped >= max_products:
                        break
                    
                    product_data = self.scrape_product_details(container)
                    
                    if product_data and product_data.get('Product_Name'):
                        self.products_data.append(product_data)
                        products_scraped += 1
                        page_products_added += 1
                        
                        self.logger.info(f"Scraped product {products_scraped}: {product_data['Product_Name'][:50]}...")
                    elif i < 3:  # Log first few failures for debugging
                        self.logger.debug(f"Failed to extract product {i+1} on page {page_num}")
                
                self.logger.info(f"Added {page_products_added} products from page {page_num}")
                page_num += 1
                
            except Exception as e:
                self.logger.error(f"Unexpected error on page {page_num}: {str(e)}")
                consecutive_failures += 1
                page_num += 1
                continue
        
        self.logger.info(f"Scraping completed. Total products scraped: {len(self.products_data)}")
        return self.products_data
    
    def create_powerbi_dataset(self):
        """Create Power BI optimized dataset with only essential fields"""
        if not self.products_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.products_data)
        
        # Power BI friendly transformations
        powerbi_df = pd.DataFrame()
        
        # 1. Product Name (cleaned)
        powerbi_df['Product_Name'] = df['Product_Name'].fillna('Unknown Product')
        
        # 2. Price (formatted for Power BI)
        powerbi_df['Price_INR'] = pd.to_numeric(df['Price'], errors='coerce')
        
        # 3. Rating (formatted for Power BI)
        powerbi_df['Rating'] = pd.to_numeric(df['Rating'], errors='coerce')
        
        # 4. Number of Reviews (formatted for Power BI)
        powerbi_df['Number_of_Reviews'] = pd.to_numeric(df['Number_of_Reviews'], errors='coerce').fillna(0).astype(int)
        
        # 5. Product URL
        powerbi_df['Product_URL'] = df['Product_URL'].fillna('')
        
        # Add helpful categorizations for Power BI analysis
        powerbi_df['Price_Range'] = powerbi_df['Price_INR'].apply(self.categorize_price)
        powerbi_df['Rating_Category'] = powerbi_df['Rating'].apply(self.categorize_rating)
        powerbi_df['Review_Category'] = powerbi_df['Number_of_Reviews'].apply(self.categorize_reviews)
        
        # Add data quality flags
        powerbi_df['Has_Price'] = powerbi_df['Price_INR'].notna()
        powerbi_df['Has_Rating'] = powerbi_df['Rating'].notna()
        powerbi_df['Has_Reviews'] = powerbi_df['Number_of_Reviews'] > 0
        
        return powerbi_df
    
    def categorize_price(self, price):
        """Categorize prices into ranges for Power BI"""
        if pd.isna(price):
            return 'No Price Listed'
        elif price < 500:
            return 'Under ₹500'
        elif price < 1000:
            return '₹500 - ₹1,000'
        elif price < 2000:
            return '₹1,000 - ₹2,000'
        elif price < 5000:
            return '₹2,000 - ₹5,000'
        else:
            return 'Above ₹5,000'
    
    def categorize_rating(self, rating):
        """Categorize ratings for Power BI"""
        if pd.isna(rating):
            return 'No Rating'
        elif rating >= 4.5:
            return 'Excellent (4.5+)'
        elif rating >= 4.0:
            return 'Very Good (4.0-4.4)'
        elif rating >= 3.5:
            return 'Good (3.5-3.9)'
        elif rating >= 3.0:
            return 'Average (3.0-3.4)'
        else:
            return 'Below Average (<3.0)'
    
    def categorize_reviews(self, review_count):
        """Categorize review counts for Power BI"""
        if review_count == 0:
            return 'No Reviews'
        elif review_count < 10:
            return 'Few Reviews (1-9)'
        elif review_count < 50:
            return 'Some Reviews (10-49)'
        elif review_count < 100:
            return 'Many Reviews (50-99)'
        elif review_count < 500:
            return 'Lots of Reviews (100-499)'
        else:
            return 'Very Popular (500+)'
    
    def save_to_csv(self, filename=None):
        """Save basic CSV with 5 essential fields"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'amazon_toys_500_products_{timestamp}.csv'
        
        if not self.products_data:
            self.logger.warning("No data to save")
            return None
        
        try:
            # Save basic 5-field CSV
            df = pd.DataFrame(self.products_data)
            df.to_csv(filename, index=False, encoding='utf-8')
            
            self.logger.info(f"Basic CSV saved to {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"Error saving CSV: {str(e)}")
            return None
    
    def save_powerbi_dataset(self, filename=None):
        """Save Power BI optimized dataset"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'PowerBI_Amazon_Toys_500_Products_{timestamp}.xlsx'
        
        try:
            powerbi_df = self.create_powerbi_dataset()
            
            if powerbi_df.empty:
                self.logger.warning("No data to save for Power BI")
                return None
            
            # Create Excel file optimized for Power BI - only main dataset
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Main dataset with essential fields + categories
                powerbi_df.to_excel(writer, sheet_name='Products_Data', index=False)
            
            self.logger.info(f"Power BI dataset saved to {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"Error saving Power BI dataset: {str(e)}")
            return None

def main():
    """Main function to run the scraper"""
    scraper = AmazonToysScraper()
    
    # Configure scraping parameters for 500 products
    MAX_PAGES = 25  # Increased to get 500 products
    MAX_PRODUCTS = 500  # Target 500 products
    
    print(" Amazon Toys & Gifts Scraper - 500 Products")
    print("=" * 60)
    print(f"Target: {MAX_PRODUCTS} products from up to {MAX_PAGES} pages")
    print("Fields: Product Name, Price, Rating, Reviews, URL")
    print("Output: CSV and Power BI ready Excel file")
    print("=" * 60)
    
    # Scrape data
    products = scraper.scrape_toys_category(max_pages=MAX_PAGES, max_products=MAX_PRODUCTS)
    
    if products:
        print(f"\n Successfully scraped {len(products)} products!")
        
        # Save files
        csv_file = scraper.save_to_csv()
        powerbi_file = scraper.save_powerbi_dataset()
        
        print(f"\n Total Products Scraped: {len(products)}")
        
        
        
        if csv_file:
            print(f"\n Basic CSV file: {csv_file}")
            
        print(f"\n Ready for Power BI import!")
    else:
        print(" No products were scraped. Check debug_page_*.html files for troubleshooting.")

if __name__ == "__main__":
    main()