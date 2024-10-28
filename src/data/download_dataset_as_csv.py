import argparse
import requests
import pandas as pd
from io import BytesIO
import os

# Dictionary mapping dataset names to their download URLs
DATASET_LINKS = {
    "dominica-bermant": "https://pmc.ncbi.nlm.nih.gov/articles/instance/6715799/bin/41598_2019_48909_MOESM2_ESM.xlsx",
    "etp-bermant": "https://pmc.ncbi.nlm.nih.gov/articles/instance/6715799/bin/41598_2019_48909_MOESM3_ESM.xlsx",
    "dominica-sharma": "https://raw.githubusercontent.com/pratyushasharma/sw-combinatoriality/refs/heads/main/data/DominicaCodas.csv",
    "sperm-whale-dialogues-sharma": "https://raw.githubusercontent.com/pratyushasharma/sw-combinatoriality/refs/heads/main/data/sperm-whale-dialogues.csv"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
}

DATA_FOLDER = './data/'

def download_file(dataset_name):
    # Check if the dataset name is in the dictionary
    if dataset_name not in DATASET_LINKS:
        print(f"Error: '{dataset_name}' not found in the available datasets.")
        print("Available datasets:")
        for name in DATASET_LINKS:
            print(f"  - {name}")
        return
    
    url = DATASET_LINKS[dataset_name]
    filename = f'{DATA_FOLDER}/{dataset_name}.csv'  # Ensure filename ends with .csv
    
    print(f"Starting download for '{dataset_name}' from {url}...")
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()  # Raise an error for bad HTTP status codes

        # Check if the file is an XLSX by extension
        if url.endswith('.xlsx'):
            # Convert the content to CSV using pandas
            xlsx_data = BytesIO(response.content)
            df = pd.read_excel(xlsx_data)
            df.to_csv(filename, index=False)
            print(f"Converted and saved '{filename}' as CSV.")
        else:
            # Save as CSV directly
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"'{filename}' downloaded as CSV.")
            
    except requests.RequestException as e:
        print(f"Failed to download '{dataset_name}': {e}")

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Download datasets by name.")
    parser.add_argument("--dataset_name", type=str, default=None, help="Name of the dataset to download. By default downloads all datasets.")
    args = parser.parse_args()
    
    if args.dataset_name is not None:
        download_file(args.dataset_name)
    else:
        for name in DATASET_LINKS:
            download_file(name)

if __name__ == "__main__":
    main()