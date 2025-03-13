import os
import time
import random
import csv
import json
import logging
import multiprocessing
import pandas as pd
from queue import Empty
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException

# Configuration
NUM_PROCESSES = 8  # Adjusted for M3 cores
BATCH_SIZE = 500  # Restart WebDriver after 500 requests
TIMEOUT = 15  # Timeout in seconds
MAX_RETRIES = 3  # Number of retries for failed pages
FAILED_URLS_CSV = "failed_urls.csv"

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_random_user_agent():
    """Returns a random user agent to avoid detection."""
    ua = UserAgent()
    return ua.random

def get_driver():
    """Creates and returns a new Selenium WebDriver instance."""
    options = webdriver.ChromeOptions()
    options.add_argument(f"user-agent={get_random_user_agent()}")
    options.add_argument("--headless=new")  # Run in headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--blink-settings=imagesEnabled=false")  # Disable images for faster page loads
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-blink-features=AutomationControlled")  # Bypass bot detection

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def read_urls(URL_CSV):
    """Reads URLs from the CSV file."""
    return pd.read_csv(URL_CSV).iloc[:, 0].tolist()


def get_property_details(driver, url):
    """Extracts extended property details from the given URL."""
    driver.get(url)
    try:
        script_tag = WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, "//script[@id='__NEXT_DATA__']"))
        ).get_attribute("innerHTML")

        try:
            data = json.loads(script_tag)
        except json.JSONDecodeError as e:
            logging.warning(f"❌ Failed to parse JSON from {url}: {str(e)}")
            return None

        dehydrated_state = data.get("props", {}).get("pageProps", {}).get("dehydratedState", {})
        queries = dehydrated_state.get("queries", [])
        state_data = queries[0].get("state", {}).get("data", {}) if queries else {}
        statement = state_data.get("data", {}).get("statement", {})

        if not isinstance(statement, dict):
            logging.warning(f"❌ JSON structure issue at {url}")
            return None

        return {
            "ID": statement.get("id", "N/A"),
            "Price": statement.get("total_price", "N/A"),
            "Currency_ID": statement.get("currency_id", "N/A"),
            "Area": f"{statement.get('area', 'N/A')} m²",
            "District Name": statement.get("district_name", "N/A"),
            "District ID": statement.get("district_id", "N/A"),
            "Address": statement.get("address", "N/A"),
            "Urban Name": statement.get("urban_name", "N/A"),
            "Urban ID": statement.get("urban_id", "N/A"),
            "Condition ID": statement.get("condition_id", "N/A"),
            "Room Type ID": statement.get("room_type_id", "N/A"),
            "Bedroom Type ID": statement.get("bedroom_type_id", "N/A"),
            "Bathroom Type ID": statement.get("bathroom_type_id", "N/A"),
            "Floor": statement.get("floor", "N/A"),
            "Total Floors": statement.get("total_floors", "N/A"),
            "Balconies": statement.get("balconies", "N/A"),
            "Balcony Area": f"{statement.get('balcony_area', 'N/A')} m²",
            "Owner Name": statement.get("owner_name", "N/A"),
            "User ID": statement.get("user_id", "N/A"),
            "User Type": statement.get("user_type", {}).get("type", "Unknown"),
            "User Statements Count": statement.get("user_statements_count", "N/A"),
            "Is VIP": statement.get("is_vip", "N/A"),
            "Is VIP Plus": statement.get("is_vip_plus", "N/A"),
            "Is Super VIP": statement.get("is_super_vip", "N/A"),
            "Views": statement.get("views", "N/A"),
            "Published": statement.get("created_at", "N/A"),
            "Last Updated": statement.get("last_updated", "N/A"),
            "Status": statement.get("status_id", "N/A"),
            "Longitude": statement.get("lng", "N/A"),
            "Latitude": statement.get("lat", "N/A")
        }
    except TimeoutException:
        logging.warning(f"❌ Timeout while loading {url}")
        return None
    except Exception as e:
        logging.error(f"❌ Unexpected error at {url}: {str(e)}")
        return None





def worker(queue, results):
    """Worker function for multiprocessing."""
    driver = get_driver()
    processed_count = 0
    while True:
        try:
            url = queue.get_nowait()
        except Empty:
            break  # Exit if queue is empty

        print(f"🔍 Scraping: {url}")
        data = get_property_details(driver, url)
        if data:
            results.append(data)
            processed_count += 1

        # Restart WebDriver every BATCH_SIZE pages to free memory
        if processed_count % BATCH_SIZE == 0:
            print(f"🔄 Restarting WebDriver after {BATCH_SIZE} pages...")
            driver.quit()
            driver = get_driver()

    driver.quit()


def scrape_property_pages(DATA_CSV, URL_CSV):
    """Runs multiple workers in parallel to scrape only the first 10 URLs and stores data in a DataFrame."""
    start_time = time.time()
    urls = read_urls(URL_CSV)  # Take only the first 10 links
    queue = multiprocessing.Queue()

    for url in urls:
        queue.put(url)

    manager = multiprocessing.Manager()
    results = manager.list()

    # Start multiprocessing workers
    processes = []
    for _ in range(NUM_PROCESSES):
        p = multiprocessing.Process(target=worker, args=(queue, results))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    # Convert results list to DataFrame
    df = pd.DataFrame(list(results))
    df.to_csv(DATA_CSV, index=False)

    end_time = time.time()
    print(f"✅ Scraping completed for first 10 links in {URL_CSV} in {end_time - start_time:.2f} seconds.")
    return df


if __name__ == "__main__":
    df = scrape_property_pages("HousingData_12March.csv", "Links_12March.csv", )
    #df_super_vip = scrape_property_pages("SUPER_VIP.csv", "SUPER_VIP_LINKS.csv")
    #df_vip_plus = scrape_property_pages("VIP_PLUS.csv", "VIP_PLUS_LINKS.csv")
    #df_vip = scrape_property_pages("VIP.csv", "VIP_LINKS.csv")