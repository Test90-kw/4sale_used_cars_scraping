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

class ScraperMain:
    def __init__(self, brand_data: Dict[str, List[Tuple[str, int]]]):
        self.brand_data = brand_data
        self.chunk_size = 5
        self.max_concurrent_brands = 3
        self.logger = logging.getLogger(__name__)
        self.setup_logging()

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
                    browser = await playwright.chromium.launch(headless=True)
                    context = await browser.new_context(
                        viewport={'width': 1920, 'height': 1080},
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    )

                    for url_template, page_count in urls:
                        for page in range(1, page_count + 1):
                            url = url_template.format(page)
                            for attempt in range(3):  # Retry mechanism
                                try:
                                    scraper = DetailsScraping(url)
                                    car_details = await scraper.get_car_details()

                                    for detail in car_details:
                                        if detail.get("date_published", "").split()[0] == yesterday:
                                            car_type = detail.get("type", "unknown")
                                            car_data.setdefault(car_type, []).append(detail)

                                    break  # Success, exit retry loop
                                except Exception as e:
                                    self.logger.error(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                                    if attempt == 2:  # Last attempt
                                        self.logger.error(f"Failed to scrape {url} after 3 attempts")
                                    else:
                                        await asyncio.sleep(5)  # Wait before retry

                    await context.close()
                    await browser.close()
            except Exception as e:
                self.logger.error(f"Error processing brand {brand_name}: {str(e)}")

        return car_data

    async def scrape_all_brands(self):
        # Split brands into chunks
        brand_chunks = [
            list(self.brand_data.items())[i:i + self.chunk_size]
            for i in range(0, len(self.brand_data), self.chunk_size)
        ]

        for chunk_index, chunk in enumerate(brand_chunks, 1):
            self.logger.info(f"Processing chunk {chunk_index}/{len(brand_chunks)}")

            saved_files = []
            semaphore = asyncio.Semaphore(self.max_concurrent_brands)  # Limit concurrent tasks

            tasks = [
                self.scrape_brand(brand_name, brand_url, semaphore)
                for brand_name, brand_url in chunk
            ]
        
            results = await asyncio.gather(*tasks)

            for brand_name, data in zip(chunk, results):
                if data:
                    filename = f"{brand_name[0]}_{datetime.now().strftime('%Y%m%d')}.xlsx"
                    self.save_to_excel(brand_name[0], data)
                    self.logger.info(f"Successfully saved data for {brand_name[0]}")
                    saved_files.append(filename)

            if saved_files:
                try:
                    folder_name = datetime.now().strftime('%Y-%m-%d')
                    upload_files_to_drive(self.drive_service, saved_files, folder_name)
                    self.logger.info(f"Successfully uploaded {len(saved_files)} files to Google Drive")
                    for file in saved_files:
                        os.remove(file)
                        self.logger.info(f"Cleaned up local file: {file}")
                except Exception as e:
                    self.logger.error(f"Error uploading files to Drive: {str(e)}")

            if chunk_index < len(brand_chunks):
                await asyncio.sleep(10)

    def save_to_excel(self, brand_name: str, car_data: Dict) -> str:
        excel_file = f"{brand_name}_{datetime.now().strftime('%Y%m%d')}.xlsx"

        try:
            with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
                for car_type, details in car_data.items():
                    df = pd.DataFrame(details)
                    if not df.empty:
                        sheet_name = car_type[:31]  # Excel sheet name length limitation
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
            return excel_file
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
        "Jeep": [
            ("https://www.q84sale.com/en/automotive/used-cars/jeep/{}", 4),
        ],
        "Chrysler": [
            ("https://www.q84sale.com/en/automotive/used-cars/chrysler/{}", 2),
        ],
        "Lincoln": [
            ("https://www.q84sale.com/en/automotive/used-cars/lincoln/{}", 1),
        ],
        "Kia": [
            ("https://www.q84sale.com/en/automotive/used-cars/kia/{}", 4),
        ],
        "Honda": [
            ("https://www.q84sale.com/en/automotive/used-cars/honda/{}", 3),
        ],
        "Mitsubishi": [
            ("https://www.q84sale.com/en/automotive/used-cars/mitsubishi/{}", 3),
        ],
        "Hyundai": [
            ("https://www.q84sale.com/en/automotive/used-cars/hyundai/{}", 3),
        ],
        "Genesis": [
            ("https://www.q84sale.com/en/automotive/cars/genesis-1/{}", 1),
        ],
        "Mazda": [
            ("https://www.q84sale.com/en/automotive/cars/mazda/{}", 2),
        ],
        "Mini": [
            ("https://www.q84sale.com/en/automotive/cars/mini/{}", 1),
        ],
        "Peugeot": [
            ("https://www.q84sale.com/en/automotive/cars/peugeot/{}", 1),
        ],
        "Volvo": [
            ("https://www.q84sale.com/en/automotive/cars/volvo/{}", 1),
        ],
        "Volkswagen": [
            ("https://www.q84sale.com/en/automotive/cars/volkswagen/{}", 3),
        ],
        "Bently": [
            ("https://www.q84sale.com/en/automotive/cars/bently/{}", 1),
        ],
        "Rolls Royce": [
            ("https://www.q84sale.com/en/automotive/cars/rolls-royce/{}", 1),
        ],
        "Aston Martin": [
            ("https://www.q84sale.com/en/automotive/cars/aston-martin/{}", 1),
        ],
        "Ferrari": [
            ("https://www.q84sale.com/en/automotive/cars/ferrari/{}", 1),
        ],
        "Lamborgini": [
            ("https://www.q84sale.com/en/automotive/cars/lamborgini/{}", 1),
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
        "Mclaren": [
            ("https://www.q84sale.com/en/automotive/cars/mclaren/{}", 1),
        ],
        "Hummer": [
            ("https://www.q84sale.com/en/automotive/cars/hummer/{}", 1),
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
    }
    brand_data_2 = {
        "Seat": [
            ("https://www.q84sale.com/en/automotive/cars/seat/{}", 1),
        ],
        "Citroen": [
            ("https://www.q84sale.com/en/automotive/cars/citroen/{}", 1),
        ],
        "Ssangyong": [
            ("https://www.q84sale.com/en/automotive/cars/ssangyong/{}", 1),
        ],
        "Baic": [
            ("https://www.q84sale.com/en/automotive/cars/baic/{}", 1),
        ],
        "GAC": [
            ("https://www.q84sale.com/en/automotive/cars/gac/{}", 1),
        ],
        "Changan": [
            ("https://www.q84sale.com/en/automotive/cars/changan/{}", 1),
        ],
        "Chery": [
            ("https://www.q84sale.com/en/automotive/cars/chery-2960/{}", 1),
        ],
        "Ineos": [
            ("https://www.q84sale.com/en/automotive/cars/ineos/{}", 1),
        ],
        "MG": [
            ("https://www.q84sale.com/en/automotive/cars/mg-2774/{}", 1),
        ],
        "Lynk & Co": [
            ("https://www.q84sale.com/en/automotive/cars/lynk-and-co/{}", 1),
        ],
        "BYD": [
            ("https://www.q84sale.com/en/automotive/cars/byd/{}", 1),
        ],
        "Lifan": [
            ("https://www.q84sale.com/en/automotive/used-cars/lifan/{}", 1),
        ],
        "DFM": [
            ("https://www.q84sale.com/en/automotive/used-cars/dfm/{}", 1),
        ],
        "Geely": [
            ("https://www.q84sale.com/en/automotive/used-cars/geely/{}", 1),
        ],
        "Great Wal": [
            ("https://www.q84sale.com/en/automotive/used-cars/great-wal/{}", 1),
        ],
        "Haval": [
            ("https://www.q84sale.com/en/automotive/used-cars/haval/{}", 1),
        ],
        "Hongqi": [
            ("https://www.q84sale.com/en/automotive/used-cars/hongqi/{}", 1),
        ],
        "Maxus": [
            ("https://www.q84sale.com/en/automotive/used-cars/maxus/{}", 1),
        ],
        "Bestune": [
            ("https://www.q84sale.com/en/automotive/used-cars/bestune/{}", 1),
        ],
        "Soueast": [
            ("https://www.q84sale.com/en/automotive/used-cars/soueast/{}", 1),
        ],
        "Forthing": [
            ("https://www.q84sale.com/en/automotive/used-cars/forthing/{}", 1),
        ],
        "Golf Carts EV": [
            ("https://www.q84sale.com/en/automotive/used-cars/golf-carts-ev/{}", 1),
        ],
        "Jetour": [
            ("https://www.q84sale.com/en/automotive/used-cars/jetour/{}", 1),
        ],
        "Special Needs Vehicles": [
            ("https://www.q84sale.com/en/automotive/used-cars/special-needs-vehicles/{}", 1),
        ],
        "Other Cars": [
            ("https://www.q84sale.com/en/automotive/used-cars/other-cars/{}", 1),
        ],
        "Exeed": [
            ("https://www.q84sale.com/en/automotive/used-cars/exeed/{}", 1),
        ],
    }
    
    scraper = ScraperMain(brand_data)
    asyncio.run(scraper.scrape_all_brands())
    time.sleep(30)
    scraper2 = ScraperMain(brand_data_2)
    asyncio.run(scraper2.scrape_all_brands())

