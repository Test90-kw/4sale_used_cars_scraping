import asyncio
import pandas as pd
import os
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from DetailsScraper import DetailsScraping
from SavingOnDrive import SavingOnDrive
import json
import logging
from typing import Dict, List, Tuple
import time
from pathlib import Path

class ScraperMain:
    def __init__(self, brand_data: Dict[str, List[Tuple[str, int]]]):
        self.brand_data = brand_data
        self.chunk_size = 3  # Reduced from 5 to 3
        self.max_concurrent_brands = 2  # Reduced from 3 to 2
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
        self.upload_retries = 3
        self.upload_retry_delay = 15  # Increased from 10 to 15 seconds
        self.temp_dir = Path("temp_files")
        self.temp_dir.mkdir(exist_ok=True)
        self.page_delay = 3  # Added delay between page requests
        self.chunk_delay = 30  # Increased delay between chunks

    def setup_logging(self):
        """Initialize logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('scraper.log')
            ]
        )
        self.logger.setLevel(logging.INFO)

    async def scrape_brand(self, brand_name: str, urls: List[Tuple[str, int]], semaphore: asyncio.Semaphore) -> Dict:
        self.logger.info(f"Starting to scrape {brand_name}")
        car_data = {}
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        async with semaphore:
            try:
                async with async_playwright() as playwright:
                    browser = await playwright.chromium.launch(
                        headless=True,
                        args=['--disable-dev-shm-usage']  # Added to prevent memory issues
                    )
                    context = await browser.new_context(
                        viewport={'width': 1920, 'height': 1080},
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    )

                    for url_template, page_count in urls:
                        for page in range(1, page_count + 1):
                            url = url_template.format(page)
                            result = await self.scrape_page(context, url, yesterday)
                            
                            # Merge results into car_data
                            for car_type, details in result.items():
                                car_data.setdefault(car_type, []).extend(details)
                            
                            await asyncio.sleep(self.page_delay)  # Delay between pages

                    await context.close()
                    await browser.close()

            except Exception as e:
                self.logger.error(f"Error processing brand {brand_name}: {str(e)}")

        return car_data

    async def scrape_page(self, context, url: str, yesterday: str) -> Dict:
        """Scrape a single page with retry logic"""
        result = {}
        max_retries = 3
        base_delay = 5

        for attempt in range(max_retries):
            try:
                scraper = DetailsScraping(url)
                car_details = await scraper.get_car_details()
                
                for detail in car_details:
                    if detail.get("date_published", "").split()[0] == yesterday:
                        car_type = detail.get("type", "unknown")
                        result.setdefault(car_type, []).append(detail)
                
                return result

            except Exception as e:
                self.logger.error(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < max_retries - 1:
                    delay = base_delay * (attempt + 1)  # Exponential backoff
                    await asyncio.sleep(delay)
                else:
                    self.logger.error(f"All attempts failed for {url}")
        
        return result

    async def scrape_all_brands(self):
        """Process all brands in smaller chunks with more delay between operations"""
        self.temp_dir.mkdir(exist_ok=True)
        
        # Split brands into smaller chunks
        brand_chunks = [
            list(self.brand_data.items())[i:i + self.chunk_size]
            for i in range(0, len(self.brand_data), self.chunk_size)
        ]

        # Limit concurrent operations
        semaphore = asyncio.Semaphore(2)  # Reduced to 2

        # Setup Google Drive
        try:
            credentials_json = os.environ.get('CAR_GCLOUD_KEY_JSON')
            if not credentials_json:
                raise EnvironmentError("CAR_GCLOUD_KEY_JSON environment variable not found")
            credentials_dict = json.loads(credentials_json)
            drive_saver = SavingOnDrive(credentials_dict)
            drive_saver.authenticate()
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {str(e)}")
            return

        pending_uploads = []

        for chunk_index, chunk in enumerate(brand_chunks, 1):
            self.logger.info(f"Processing chunk {chunk_index}/{len(brand_chunks)}")
            
            # Create tasks for each brand in the chunk
            tasks = []
            for brand_name, brand_urls in chunk:
                task = asyncio.create_task(self.scrape_brand(brand_name, brand_urls, semaphore))
                tasks.append((brand_name, task))
                await asyncio.sleep(2)  # Delay between brand task creation
            
            # Process brands in the chunk
            for brand_name, task in tasks:
                try:
                    car_data = await task
                    if car_data:
                        excel_file = await self.save_to_excel(brand_name, car_data)
                        if excel_file:
                            pending_uploads.append(excel_file)
                            self.logger.info(f"Successfully saved data for {brand_name}")
                except Exception as e:
                    self.logger.error(f"Error processing {brand_name}: {str(e)}")

            # Upload files after each chunk
            if pending_uploads:
                uploaded_files = await self.upload_files_with_retry(drive_saver, pending_uploads)
                
                # Clean up uploaded files
                for file in uploaded_files:
                    try:
                        os.remove(file)
                        self.logger.info(f"Cleaned up local file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error cleaning up {file}: {str(e)}")
                
                pending_uploads = []

            # Add a longer delay between chunks
            if chunk_index < len(brand_chunks):
                self.logger.info(f"Waiting {self.chunk_delay} seconds before next chunk...")
                await asyncio.sleep(self.chunk_delay)

    async def upload_files_with_retry(self, drive_saver, files: List[str]) -> List[str]:
        """Upload files to Google Drive with retry mechanism"""
        uploaded_files = []
        
        for file in files:
            for attempt in range(self.upload_retries):
                try:
                    if os.path.exists(file):
                        drive_saver.save_files([file])
                        uploaded_files.append(file)
                        self.logger.info(f"Successfully uploaded {file} to Google Drive")
                        break
                except Exception as e:
                    self.logger.error(f"Upload attempt {attempt + 1} failed for {file}: {str(e)}")
                    if attempt < self.upload_retries - 1:
                        await asyncio.sleep(self.upload_retry_delay)
                    else:
                        self.logger.error(f"Failed to upload {file} after {self.upload_retries} attempts")
        
        return uploaded_files

    async def save_to_excel(self, brand_name: str, car_data: Dict) -> str:
        """Save data to Excel file asynchronously"""
        excel_file = self.temp_dir / f"{brand_name}_{datetime.now().strftime('%Y%m%d')}.xlsx"
        
        try:
            with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
                for car_type, details in car_data.items():
                    df = pd.DataFrame(details)
                    if not df.empty:
                        sheet_name = car_type[:31]  # Excel sheet name length limitation
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
            return str(excel_file)
        except Exception as e:
            self.logger.error(f"Error saving Excel file {excel_file}: {str(e)}")
            return None

if __name__ == "__main__":
    brand_data = {
        "Toyota": [
            ("https://www.q84sale.com/en/automotive/used-cars-1/toyota/{}", 18),
        ],
        "Lexus": [
            ("https://www.q84sale.com/en/automotive/used-cars/lexus/{}", 6),
        ],
        "Chevrolet": [
            ("https://www.q84sale.com/en/automotive/used-cars/chevrolet/{}", 15),
        ],
        "Ford": [
            ("https://www.q84sale.com/en/automotive/used-cars/ford/{}", 9),
        ],
        "Cadillac": [
            ("https://www.q84sale.com/en/automotive/used-cars/cadillac/{}", 3),
        ],
        "GMC": [
            ("https://www.q84sale.com/en/automotive/used-cars/gmc/{}", 9),
        ],
        "Mercury": [
            ("https://www.q84sale.com/en/automotive/used-cars/mercury/{}", 1),
        ],
        "Nissan": [
            ("https://www.q84sale.com/en/automotive/used-cars/nissan/{}", 10),
        ],
        "Infiniti": [
            ("https://www.q84sale.com/en/automotive/used-cars/infiniti/{}", 2),
        ],
        "Mercedes": [
            ("https://www.q84sale.com/en/automotive/used-cars/mercedes/{}", 9),
        ],
        "BMW": [
            ("https://www.q84sale.com/en/automotive/used-cars/bmw/{}", 7),
        ],
        "Porsche": [
            ("https://www.q84sale.com/en/automotive/used-cars/porsche/{}", 4),
        ],
        "Jaguar": [
            ("https://www.q84sale.com/en/automotive/used-cars/jaguar/{}", 1),
        ],
        "Land Rover": [
            ("https://www.q84sale.com/en/automotive/used-cars/land-rover/{}", 7),
        ],
        "Dodge": [
            ("https://www.q84sale.com/en/automotive/used-cars/dodge/{}", 4),
        ],
        "Renault": [
            ("https://www.q84sale.com/en/automotive/cars/renault/{}", 1),
        ],
        "Acura": [
            ("https://www.q84sale.com/en/automotive/cars/acura/{}", 1),
        ],
        "Subaru": [
            ("https://www.q84sale.com/en/automotive/cars/subaru/{}", 1),
        ],
        "Suzuki": [
            ("https://www.q84sale.com/en/automotive/cars/suzuki/{}", 2),
        ],
        "Isuzu": [
            ("https://www.q84sale.com/en/automotive/cars/isuzu/{}", 1),
        ],
        "Alfa Romeo": [
            ("https://www.q84sale.com/en/automotive/cars/alfa-romeo/{}", 1),
        ],
        "Fiat": [
            ("https://www.q84sale.com/en/automotive/cars/fiat/{}", 1),
        ],
        "Mclaren": [
            ("https://www.q84sale.com/en/automotive/cars/mclaren/{}", 1),
        ],
        "Hummer": [
            ("https://www.q84sale.com/en/automotive/cars/hummer/{}", 1),
        ],
        "Maserati": [
            ("https://www.q84sale.com/en/automotive/cars/maserati/{}", 1),
        ],
        "Tesla": [
            ("https://www.q84sale.com/en/automotive/cars/tesla/{}", 1),
        ],
        "Lotus": [
            ("https://www.q84sale.com/en/automotive/cars/lotus/{}", 1),
        ],
        ###NEW
        "Other Cars": [
            ("https://www.q84sale.com/en/automotive/used-cars/other-cars/{}", 1),
        ],
        "Exeed": [
            ("https://www.q84sale.com/en/automotive/used-cars/exeed/{}", 1),
        ],
        "Lynk & Co": [
            ("https://www.q84sale.com/en/automotive/cars/lynk-and-co/{}", 1),
        ],
        
        
    }
   
    
    async def main():
        # Process first set of brands
        scraper = ScraperMain(brand_data)
        await scraper.scrape_all_brands()
        
        # Wait between sets
        await asyncio.sleep(10)
    
    # Run everything in the async event loop
    asyncio.run(main())

