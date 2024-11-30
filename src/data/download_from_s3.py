import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


class S3Downloader:
    def __init__(self, aws_access_key, aws_secret_key, region_name):
        """
        Initializes the S3Downloader with AWS credentials.
        """
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )

    @staticmethod
    def parse_s3_url(s3_url):
        """
        Parses an S3 URL into bucket name and object key.
        """
        if not s3_url.startswith("s3://"):
            raise ValueError(f"Invalid S3 URL: {s3_url}")
        parts = s3_url[5:].split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid S3 URL: {s3_url}")
        return parts[0], parts[1]

    def download_file(self, s3_url, output_dir):
        """
        Downloads a single file from an S3 URL and saves it to the output directory.
        """
        bucket_name, key = self.parse_s3_url(s3_url)
        file_name = os.path.join(output_dir, os.path.basename(key))
        
        print(f"Downloading {key} from bucket {bucket_name}...")
        self.s3_client.download_file(bucket_name, key, file_name)
        print(f"File saved to {file_name}")

    def download_files(self, s3_urls, output_dir):
        """
        Downloads multiple files from a list of S3 URLs.
        """
        os.makedirs(output_dir, exist_ok=True)
        for s3_url in s3_urls:
            try:
                self.download_file(s3_url, output_dir)
            except NoCredentialsError:
                print("Error: AWS credentials not found.")
            except PartialCredentialsError:
                print("Error: Incomplete AWS credentials.")
            except Exception as e:
                print(f"Failed to download {s3_url}: {e}")


# Main body
if __name__ == "__main__":
    # Load AWS credentials from .env file
    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION')

    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_REGION:
        raise EnvironmentError("Missing AWS credentials in .env file")

    # List of S3 file links to download
    s3_links = [
        "s3://ieee-dataport/data/1274354/dataset-11.zip",
        "s3://ieee-dataport/data/1236738/Dominica_dataset.zip",
    ]

    # Output directory
    output_dir = "./data"

    # Create an instance of S3Downloader and download files
    downloader = S3Downloader(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
    downloader.download_files(s3_links, output_dir)
