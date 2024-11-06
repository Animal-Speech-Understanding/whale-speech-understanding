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
    "sperm-whale-dialogues-sharma": "https://raw.githubusercontent.com/pratyushasharma/sw-combinatoriality/refs/heads/main/data/sperm-whale-dialogues.csv",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
}

DATA_FOLDER = "./data"

# Ensure data folder exists
os.makedirs(DATA_FOLDER, exist_ok=True)


def download_and_save_file(dataset_name):
    """Downloads and saves the specified dataset as a CSV file."""
    url = DATASET_LINKS[dataset_name]
    filename = os.path.join(DATA_FOLDER, f"{dataset_name}.csv")

    print(f"Downloading '{dataset_name}' from {url}...")
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()

        # Convert to CSV if file is an XLSX, otherwise save directly
        if url.endswith(".xlsx"):
            df = pd.read_excel(BytesIO(response.content))
            df.to_csv(filename, index=False)
            print(f"'{dataset_name}' downloaded and saved as CSV.")
        else:
            with open(filename, "wb") as f:
                f.write(response.content)
            print(f"'{dataset_name}' downloaded and saved as CSV.")

    except requests.RequestException as e:
        print(f"Failed to download '{dataset_name}': {e}")


def download_all_datasets():
    """Downloads all datasets listed in DATASET_LINKS."""
    for dataset_name in DATASET_LINKS:
        download_and_save_file(dataset_name)


def main():
    parser = argparse.ArgumentParser(description="Download datasets by name.")
    parser.add_argument(
        "--dataset_name",
        type=str,
        help="Name of the dataset to download. If omitted, all datasets are downloaded.",
    )
    args = parser.parse_args()

    if args.dataset_name:
        if args.dataset_name in DATASET_LINKS:
            download_and_save_file(args.dataset_name)
        else:
            print(f"Error: '{args.dataset_name}' not found. Available datasets:")
            for name in DATASET_LINKS:
                print(f"  - {name}")
    else:
        download_all_datasets()


if __name__ == "__main__":
    main()
