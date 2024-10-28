from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    InvalidSessionIdException
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
import argparse
import csv
import os
import re
import requests
import multiprocessing

GECKODRIVER_PATH='/usr/local/bin/geckodriver'
DATA_BASE_PATH='./data/watkins_marine_mammal'

options = Options()
options.headless = True
options.set_preference('permissions.default.image', 2)  # Disable images
options.set_preference('permissions.default.stylesheet', 2)  # Disable CSS (optional)
options.page_load_strategy = 'none'
    
# Set up Selenium WebDriver (make sure to provide the correct path to your ChromeDriver)
service = Service(executable_path=GECKODRIVER_PATH)
driver = webdriver.Firefox(service=service, options=options)

# Define the base URL
BASE_URL = "https://cis.whoi.edu"

# Function to initialize WebDriver
def init_driver():
    options = Options()
    options.headless = True  # Run in headless mode
    options.set_preference('permissions.default.image', 2)  # Disable images
    options.set_preference('permissions.default.stylesheet', 2)  # Disable CSS
    service = Service(executable_path=GECKODRIVER_PATH)
    driver = webdriver.Firefox(service=service, options=options)
    return driver

# Function to download a single audio file
def download_audio_file(link, folder):
    filename = link.split("/")[-1]
    audio_path = os.path.join(folder, filename)
    
    print(f"Downloading {filename}...")
    try:
        response = requests.get(link, timeout=30)
        response.raise_for_status()
        with open(audio_path, 'wb') as f:
            f.write(response.content)
        print(f"{filename} downloaded!")
    except requests.RequestException as e:
        print(f"Failed to download {filename}: {e}")

# Function to download multiple audio files in parallel
def download_audio_files(audio_links, folder, max_workers=5):
    if not os.path.exists(folder):
        os.makedirs(folder)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_audio_file, link, folder) for link in audio_links]
        for future in as_completed(futures):
            future.result()  # This will raise any exceptions that occurred during the download

# Function to fetch metadata from the second table
def fetch_metadata(metadata_url):
    driver = init_driver()
    metadata = {}
    try:
        driver.get(metadata_url)
        wait = WebDriverWait(driver, 10)
        # Wait until at least two tables are present
        wait.until(lambda d: len(d.find_elements(By.TAG_NAME, "table")) >= 2)
        tables = driver.find_elements(By.TAG_NAME, "table")
        if len(tables) < 2:
            print(f"Less than 2 tables found in {metadata_url}")
            return metadata  # Return empty if second table isn't present
        second_table = tables[1]  # Index 1 for the second table
        rows = second_table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header row
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) == 2:
                field = cells[0].text.strip().rstrip(':')  # Remove trailing colon
                value = cells[1].text.strip()
                metadata[field] = value
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Failed to fetch metadata from {metadata_url}: {e}")
    except InvalidSessionIdException as e:
        print(f"Invalid session when accessing {metadata_url}: {e}")
    finally:
        driver.quit()
    return metadata

def metadata_worker(metadata_urls, output_queue):
    metadata_list = []
    for url in metadata_urls:
        metadata = fetch_metadata(url)
        if metadata:
            metadata_list.append(metadata)
    output_queue.put(metadata_list)

# Function to save metadata to CSV
def save_metadata_to_csv(metadata_list, csv_filename):
    if not metadata_list:
        print("No metadata to save.")
        return
    
    # Extract the directory path from the CSV filename
    directory = os.path.dirname(csv_filename)
    
    # Create the directory path if it doesn't exist
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory path: {directory}")
    
    # Determine all unique keys for CSV headers
    fieldnames = sorted({key for data in metadata_list for key in data.keys()})
    
    # Write metadata to CSV
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for data in metadata_list:
            writer.writerow(data)
    print(f"Metadata saved to {csv_filename}")


# Function to extract metadata URL from JavaScript string
def extract_metadata_url(js_string, metadata_base_url):
    # Use regex to extract the metadata URL part from the JavaScript call
    match = re.search(r"popUpWin\('([^']+)'\);", js_string)
    if match:
        return metadata_base_url + match.group(1)
    return None

# Function to get list of dictionaries with metadata for each link provided
def get_metadata_list(metadata_links):
    # Prepare for multiprocessing metadata fetching
    num_processes = min(4, multiprocessing.cpu_count(), len(metadata_links))  # Adjust based on your system
    chunk_size = len(metadata_links) // num_processes
    metadata_chunks = [metadata_links[i * chunk_size:(i + 1) * chunk_size] for i in range(num_processes)]
    # Handle any remaining links
    if len(metadata_links) % num_processes != 0:
        metadata_chunks[-1].extend(metadata_links[num_processes * chunk_size:])
        
    # Create a multiprocessing Queue to collect results
    output_queue = multiprocessing.Queue()
    
    # Create and start processes
    processes = []
    for chunk in metadata_chunks:
        p = multiprocessing.Process(target=metadata_worker, args=(chunk, output_queue))
        p.start()
        processes.append(p)
    
    # Collect results
    metadata_list = []
    for _ in processes:
        metadata_list.extend(output_queue.get())
    
    # Wait for all processes to finish
    for p in processes:
        p.join()
        
    print(f"Fetched metadata for {len(metadata_list)} entries.")
    return metadata_list

# Extract all audio file links and metadata links
def get_audio_and_metadata_links(table, max_count, metadata_base_url):
    audio_links = []
    metadata_links = []
    
    table_rows = table.find_elements(By.TAG_NAME, "tr")[1:]
              
    for row in (table_rows if max_count is None else table_rows[:max_count]):
        try:
            audio_tag = row.find_element(By.PARTIAL_LINK_TEXT, "Download")
            metadata_tag = row.find_element(By.PARTIAL_LINK_TEXT, "Metadata")
            if audio_tag and metadata_tag:
                audio_links.append(audio_tag.get_attribute('href'))
                js_string = metadata_tag.get_attribute('href')
                metadata_url = extract_metadata_url(js_string, metadata_base_url)
                if metadata_url:
                    metadata_links.append(metadata_url)
        except NoSuchElementException:
            print("Link not found in row. Skipping.")
            continue
                
    return audio_links, metadata_links

# Main function to scrape the webpage and download data
def main():    
    # Parsing command-line arguments
    parser = argparse.ArgumentParser(description="Scrape whale sounds and metadata.")
    parser.add_argument('--page_suffix', type=str, default="/science/B/whalesounds/bestOf.cfm?code=BA2A", help="Suffix for the page URL.")
    parser.add_argument('--num_files', type=int, default=None, help="Number of audio files or metadata entries to download. Default is all.")
    parser.add_argument('--download_type', type=str, choices=['audio', 'metadata', 'both'], default='both',
                        help="Download type: 'audio', 'metadata', or 'both'.")
    parser.add_argument('--csv_file_path', type=str, default="/sperm_whale/metadata/sperm_whale_metadata.csv", help="Path to the csv file with metadata relative to data folder")
    parser.add_argument('--audio_folder_path', type=str, default="/sperm_whale/audio", help="Path to the folder to store audio files")

    args = parser.parse_args()
    
    csv_filename= os.path.join(*DATA_BASE_PATH.split('/'), *args.csv_file_path.split('/'))
    audio_folder = os.path.join(*DATA_BASE_PATH.split('/'), *args.audio_folder_path.split('/'))
    
    print(DATA_BASE_PATH, args.csv_file_path, csv_filename, audio_folder)
    
    # URL of the page with audio files and metadata links
    page_url = urljoin(BASE_URL, f"{args.page_suffix}")
    metadata_base_url = '/'.join(page_url.split('/')[:-1]) + '/'
    
    # Load the main page
    driver.get(page_url)
    print('Page loaded successfully')

    # Wait until the table is present
    wait = WebDriverWait(driver, 20)
    table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
    print('Table found')
    
    # Fetch all audio and metadata links, limited by `num_files` if provided
    audio_links, metadata_links = get_audio_and_metadata_links(table, args.num_files, metadata_base_url)
               
    # Process downloads based on the download type
    if args.download_type in ['audio', 'both']:
        print("Starting audio file downloads...")
        download_audio_files(audio_links, audio_folder)
        
            
    if args.download_type in ['metadata', 'both']:
        print("Starting gathering metadata infos...")
        metadata_list = get_metadata_list(metadata_links)
        
        # Save metadata to CSV
        save_metadata_to_csv(metadata_list, csv_filename)
    
    # Close the Selenium driver
    driver.quit()

if __name__ == "__main__":
    main()
