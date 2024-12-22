import asyncio
import aiofiles
import pandas as pd
import os
import mimetypes
from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from DetailsScraper import DetailsScraping
from SavingOnDrive import SavingOnDrive
import json
import logging
from itertools import islice

class ScraperMain:
    # Added static variable
    excel_files = []  # Static variable to store created Excel file names

    def __init__(self, brand_data):
        """
        :param brand_data: A dictionary where keys are brand names, and values are lists of (url_template, page_count) tuples.
        """
        self.brand_data = brand_data

    async def scrape_brand(self, brand_name, urls):
        print(f"Starting to scrape {brand_name}")
        car_data = {}

        # Calculate yesterday's date
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Yesterday's date: {yesterday}")

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            for url_template, page_count in urls:
                    for page in range(1, page_count + 1):
                        url = url_template.format(page)
                        print(f"Scraping URL: {url}")
                        try:
                            scraper = DetailsScraping(url)
                            car_details = await scraper.get_car_details()
                            print(f"Found {len(car_details)} car details on {url}")

                            for detail in car_details:
                                if detail.get("date_published"):
                                    date_published = detail.get("date_published").split()[0]
                                    if date_published == yesterday:
                                        car_type = detail.get("type", "unknown")
                                        if car_type not in car_data:
                                            car_data[car_type] = []
                                        car_data[car_type].append(detail)

                        except Exception as e:
                            print(f"Error scraping {url}: {e}")         

        return car_data

    # async def scrape_all_brands(self):
    #     tasks = []
    #     for brand_name, urls in self.brand_data.items():
    #         tasks.append(self.scrape_brand(brand_name, urls))

    #     results = await asyncio.gather(*tasks, return_exceptions=False)

    #     for brand_name, car_data in zip(self.brand_data.keys(), results):
    #         print(f"Data collected for {brand_name}: {car_data}")

    #         if car_data:
    #             print(f"Saving data for {brand_name}")
    #             try:
    #                 # Save file and update the static variable
    #                 excel_file = self.save_to_excel(brand_name, car_data)
    #                 if excel_file:
    #                     ScraperMain.excel_files.append(excel_file)
    #             except Exception as e:
    #                 print(f"Error saving data for {brand_name}: {e}")
    #         else:
    #             print(f"No data to save for {brand_name}")

    async def scrape_all_brands(self):
        def chunks(data, size):
            """Helper function to split dictionary into chunks of a given size."""
            iterator = iter(data)
            while True:
                chunk = {key: data[key] for key, _ in zip(iterator, range(size))}
                if not chunk:
                    break
                yield chunk

        brand_chunks = list(chunks(self.brand_data, 7))
        
        credentials_json = os.environ.get('CAR_GCLOUD_KEY_JSON')
        if not credentials_json:
            raise EnvironmentError("CAR_GCLOUD_KEY_JSON environment variable not found.")
        credentials_dict = json.loads(credentials_json)
        drive_saver = SavingOnDrive(credentials_dict)
        drive_saver.authenticate()
        
        for i, chunk in enumerate(brand_chunks, 1):
            print(f"Processing chunk {i}/{len(brand_chunks)}: {list(chunk.keys())}")

            tasks = [self.scrape_brand(brand_name, urls) for brand_name, urls in chunk.items()]
            results = await asyncio.gather(*tasks, return_exceptions=False)
            
            chunk_files = []
            for brand_name, car_data in zip(chunk.keys(), results):
                if car_data:
                    try:
                        excel_file = self.save_to_excel(brand_name, car_data)
                        if excel_file:
                            chunk_files.append(excel_file)
                    except Exception as e:
                        print(f"Error saving data for {brand_name}: {e}")

            if chunk_files:
                drive_saver.save_files(chunk_files)

            print(f"Completed processing and uploading chunk {i}/{len(brand_chunks)}")
            
            # for brand_name, car_data in zip(chunk.keys(), results):
            #     print(f"Data collected for {brand_name}: {car_data}")
            #     if car_data:
            #         print(f"Saving data for {brand_name}")
            #         try:
            #             excel_file = self.save_to_excel(brand_name, car_data)
            #             if excel_file:
            #                 ScraperMain.excel_files.append(excel_file)
            #         except Exception as e:
            #             print(f"Error saving data for {brand_name}: {e}")
            #     else:
            #         print(f"No data to save for {brand_name}")

            # print(f"Completed processing chunk {i}/{len(brand_chunks)}")

    
    def save_to_excel(self, brand_name, car_data):
        excel_file = f"{brand_name}.xlsx"
        print(f"Saving data to {excel_file}")

        try:
            with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
                for car_type, details in car_data.items():
                    df_new = pd.DataFrame(details)
                    if df_new.empty:
                        print(f"Skipping empty DataFrame for car type: {car_type}")
                        continue

                    print(f"Writing data to new sheet: {car_type}")
                    sheet_name = car_type[:31]
                    df_new.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"Excel file '{excel_file}' created and data saved successfully.")
            return excel_file
        except Exception as e:
            print(f"Error creating or saving Excel file '{excel_file}': {e}")
            return None

if __name__ == "__main__":

    brand_data = {
        # "Toyota": [
        #     ("https://www.q84sale.com/en/automotive/used-cars-1/toyota/{}", 18),
        # ],
        # "Lexus": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/lexus/{}", 6),
        # ],
        # "Chevrolet": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/chevrolet/{}", 15),
        # ],
        # "Ford": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/ford/{}", 9),
        # ],
        # "Cadillac": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/cadillac/{}", 3),
        # ],
        # "GMC": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/gmc/{}", 9),
        # ],
        # "Mercury": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/mercury/{}", 1),
        # ],
        # "Nissan": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/nissan/{}", 10),
        # ],
        # "Infiniti": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/infiniti/{}", 2),
        # ],
        # "Mercedes": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/mercedes/{}", 9),
        # ],
        # "BMW": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/bmw/{}", 7),
        # ],
        # "Porsche": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/porsche/{}", 4),
        # ],
        # "Jaguar": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/jaguar/{}", 1),
        # ],
        # "Land Rover": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/land-rover/{}", 7),
        # ],
        # "Dodge": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/dodge/{}", 4),
        # ],
        # "Jeep": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/jeep/{}", 4),
        # ],
        # "Chrysler": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/chrysler/{}", 2),
        # ],
        "Lincoln": [
            ("https://www.q84sale.com/en/automotive/used-cars/lincoln/{}", 1),
        ],
        # "Kia": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/kia/{}", 4),
        # ],
        # "Honda": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/honda/{}", 3),
        # ],
        # "Mitsubishi": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/mitsubishi/{}", 3),
        # ],
        # "Hyundai": [
        #     ("https://www.q84sale.com/en/automotive/used-cars/hyundai/{}", 3),
        # ],
        # "Genesis": [
        #     ("https://www.q84sale.com/en/automotive/cars/genesis-1/{}", 1),
        # ],
        # "Mazda": [
        #     ("https://www.q84sale.com/en/automotive/cars/mazda/{}", 2),
        # ],
        "Mini": [
            ("https://www.q84sale.com/en/automotive/cars/mini/{}", 1),
        ],
        "Peugeot": [
            ("https://www.q84sale.com/en/automotive/cars/peugeot/{}", 1),
        ],
        "Volvo": [
            ("https://www.q84sale.com/en/automotive/cars/volvo/{}", 1),
        ],
        # "Volkswagen": [
        #     ("https://www.q84sale.com/en/automotive/cars/volkswagen/{}", 3),
        # ],
        # "Bently": [
        #     ("https://www.q84sale.com/en/automotive/cars/bently/{}", 1),
        # ],
        # "Rolls Royce": [
        #     ("https://www.q84sale.com/en/automotive/cars/rolls-royce/{}", 1),
        # ],
        # "Aston Martin": [
        #     ("https://www.q84sale.com/en/automotive/cars/aston-martin/{}", 1),
        # ],
        # "Ferrari": [
        #     ("https://www.q84sale.com/en/automotive/cars/ferrari/{}", 1),
        # ],
        # "Lamborgini": [
        #     ("https://www.q84sale.com/en/automotive/cars/lamborgini/{}", 1),
        # ],
        # "Maserati": [
        #     ("https://www.q84sale.com/en/automotive/cars/maserati/{}", 1),
        # ],
        # "Tesla": [
        #     ("https://www.q84sale.com/en/automotive/cars/tesla/{}", 1),
        # ],
        # "Lotus": [
        #     ("https://www.q84sale.com/en/automotive/cars/lotus/{}", 1),
        # ],
        # "Mclaren": [
        #     ("https://www.q84sale.com/en/automotive/cars/mclaren/{}", 1),
        # ],
        # "Hummer": [
        #     ("https://www.q84sale.com/en/automotive/cars/hummer/{}", 1),
        # ],
        # "Renault": [
        #     ("https://www.q84sale.com/en/automotive/cars/renault/{}", 1),
        # ],
        # "Acura": [
        #     ("https://www.q84sale.com/en/automotive/cars/acura/{}", 1),
        # ],
        # "Subaru": [
        #     ("https://www.q84sale.com/en/automotive/cars/subaru/{}", 1),
        # ],
        # "Suzuki": [
        #     ("https://www.q84sale.com/en/automotive/cars/suzuki/{}", 2),
        # ],
        # "Isuzu": [
        #     ("https://www.q84sale.com/en/automotive/cars/isuzu/{}", 1),
        # ],
        "Alfa Romeo": [
            ("https://www.q84sale.com/en/automotive/cars/alfa-romeo/{}", 1),
        ],
        "Fiat": [
            ("https://www.q84sale.com/en/automotive/cars/fiat/{}", 1),
        ],
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

    # Load the service account JSON key from the GitHub secret
    credentials_json = os.environ.get('CAR_GCLOUD_KEY_JSON')
    if not credentials_json:
        raise EnvironmentError("CAR_GCLOUD_KEY_JSON environment variable not found.")
    
    credentials_dict = json.loads(credentials_json)

    print("Excel files: ", ScraperMain.excel_files)

    # Initialize the SavingOnDrive class
    drive_saver = SavingOnDrive(credentials_dict)
    drive_saver.authenticate()
    
    # Save files to Google Drive
    if ScraperMain.excel_files:
        print(f"Uploading files to Google Drive: {ScraperMain.excel_files}")
        drive_saver.save_files(ScraperMain.excel_files)
    else:
        print("No files to upload to Google Drive.")
