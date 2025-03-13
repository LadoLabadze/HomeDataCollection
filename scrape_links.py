import os
import time
import random
import csv
import multiprocessing
from collections import deque
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException

# Configuration
URLS_TO_SCRAPE = {
    "Links_12March.csv": "https://www.myhome.ge/s/iyideba-bina-Tbilisshi/?deal_types=1&real_estate_types=1&cities=1&CardView=3&urbans=38,43,47,30,29,51,52,53,59,2,23,24,27,64,65,66,67,57,61,62,28&districts=4,5,1,3,6&currency_id=2&owner_type=physical&statuses=2,3&price_from=30000&price_to=130000&area_from=30&area_to=120&area_types=1&square_price_from=400&square_price_to=2500&page=",
    #"Links.csv": "https://www.myhome.ge/s/iyideba-bina-Tbilisshi/?deal_types=1&real_estate_types=1&cities=1&currency_id=2&CardView=3&urbans=38,43,47,30,29,51,52,53,59,2,23,24,27,64,65,66,67,57,61,62,28&districts=4,5,1,3,6&area_from=35&area_to=60&area_types=1&statuses=2,3&price_from=50000&price_to=100000&page=",
    #"SUPER_VIP_links.csv": "https://www.myhome.ge/s/iyideba-bina-Tbilisshi/?is_super_vip=true&deal_types=1&real_estate_types=1&currency_id=1&CardView=3&cities=1&page=",
    #"VIP_PLUS_links.csv": "https://www.myhome.ge/s/iyideba-bina-Tbilisshi/?is_vip_plus=true&deal_types=1&real_estate_types=1&currency_id=1&CardView=3&cities=1&page=",
    #"VIP_links.csv": "https://www.myhome.ge/s/iyideba-bina-Tbilisshi/?is_vip=true&deal_types=1&real_estate_types=1&currency_id=1&CardView=3&cities=1&page="
}

MAX_LINKS = 6000  # Max links per category
NUM_PROCESSES = 8  # Use all CPU cores on MacBook Air M3
BATCH_SIZE = 100  # Restart WebDriver after every 100 pages
TIMEOUT = 10  # Timeout for page loads
MAX_RETRIES = 3  # Max retries for failed pages
STOP_PAGE = 1000  # Change this number as needed

# Function to get a random user agent
def get_random_user_agent():
    """Returns a random user agent to avoid detection."""
    ua = UserAgent()
    return ua.random


# Function to get a Selenium WebDriver instance
def get_driver():
    """Creates and returns a new WebDriver instance."""
    options = webdriver.ChromeOptions()
    options.add_argument(f"user-agent={get_random_user_agent()}")
    options.add_argument("--headless=new")  # Run in headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--blink-settings=imagesEnabled=false")  # Disable images for faster loading
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # Enable Performance Logging
    options.add_argument("--disable-blink-features=AutomationControlled")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")  # Bypass bot detection
    return driver


# Function to extract property links from a page
def get_property_links(driver, page_url, retry=0):
    """Scrapes links from a single page."""
    driver.get(page_url)

    try:
        # Wait for listings to load (max TIMEOUT seconds)
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/pr/')]"))
        )
        property_elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/pr/')]")
        return [elem.get_attribute("href") for elem in property_elements if elem.get_attribute("href")]

    except Exception as e:
        print(f"⚠️ Error on {page_url}: {str(e)} - Retrying ({retry}/{MAX_RETRIES})")
        if retry < MAX_RETRIES:
            time.sleep(random.uniform(2, 4))  # Small delay before retry
            return get_property_links(driver, page_url, retry + 1)
        return []  # No links found after retries




def scrape_multiple_pages(BASE_URL, URL_CSV):
    """Scrapes property listings and saves them to CSV."""
    start_time = time.time()  # Start measuring time
    driver = get_driver()
    page = 1
    links = deque()
    link_count = 0  # Initialize the link counter

    print(f"🔍 Starting Scraping: {URL_CSV}")

    while len(links) < MAX_LINKS and page <= STOP_PAGE:  # Stop at STOP_PAGE
        page_url = f"{BASE_URL}{page}"
        new_links = get_property_links(driver, page_url)

        if not new_links:
            print("🚫 No more listings found, stopping...")
            break  # Stop if no properties found

        links.extend(new_links)  # Store extracted links
        link_count += len(new_links)  # Update the link counter

        if link_count >= 500 and link_count % 500 == 0:
            print(f"⚡ Scraped {link_count} links so far...")  # Update message every 500 links

        print(f"✅ Scraped page {page}...")

        page += 1  # Increment the page number

        # Add small delay between page loads to look more human-like
        time.sleep(random.uniform(0.2, 1.5))

        # Restart WebDriver every BATCH_SIZE pages to free memory
        if page % BATCH_SIZE == 0:
            print(f"🔄 Restarting WebDriver after {BATCH_SIZE} pages...")
            driver.quit()
            driver = get_driver()

    driver.quit()

    # Save all collected links at once
    save_links_to_csv(links, URL_CSV)

    end_time = time.time()  # End measuring time
    elapsed_time = end_time - start_time  # Calculate total execution time
    print(f"✅ Completed: {URL_CSV} → {len(links)} links saved in {elapsed_time:.2f} seconds.")




# Function to save collected links to CSV at once
def save_links_to_csv(links, URL_CSV):
    """Saves the collected links to a CSV file."""
    if not links:
        return  # No links to save

    with open(URL_CSV, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Links"])  # Write header
        writer.writerows([[link] for link in links])


# Multi-processing for parallel execution
def run_multi_processing():
    """Runs multiple scraping processes in parallel."""
    processes = []
    for filename, url in URLS_TO_SCRAPE.items():
        p = multiprocessing.Process(target=scrape_multiple_pages, args=(url, filename))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()


if __name__ == "__main__":
    run_multi_processing()
