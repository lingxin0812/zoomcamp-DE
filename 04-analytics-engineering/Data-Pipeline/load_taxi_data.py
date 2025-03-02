import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import pandas as pd
import time

# Change this to your bucket name
BUCKET_NAME = "taxi_data_bucket_ling"

# If you authenticated through the GCP SDK, you can comment out these two lines
CREDENTIALS_FILE = r"C:\Users\lingx\dbt_bigquery\google_credentials.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

# Base URL for the .csv.gz files
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-"
MONTHS = [f"{i:02d}" for i in range(1, 13)]  # Example: Download only January (01)
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(month):
    """Download the .csv.gz file for the given month."""
    url = f"{BASE_URL}{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2020-{month}.csv.gz")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")

        """ df = pd.read_parquet(file_path)
        if 'DOlocationID' in df.columns:
            df['airport_fee'] = df['airport_fee'].astype(float)  # Convert to DOUBLE
            df.to_parquet(file_path, engine='pyarrow', index=False)  # Overwrite with conversion
            print(f"Converted 'airport_fee' to DOUBLE in {file_path}") """

        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def convert_to_parquet(csv_gz_path):
    """Convert the downloaded .csv.gz file to .parquet format."""
    parquet_path = csv_gz_path.replace(".csv.gz", ".parquet")

    try:
        print(f"Converting {csv_gz_path} to {parquet_path}...")
        # Read the .csv.gz file into a DataFrame
        df = pd.read_csv(csv_gz_path, compression="gzip")
        # Save the DataFrame as a .parquet file
        df.to_parquet(parquet_path)
        print(f"Converted: {parquet_path}")
        return parquet_path
    except Exception as e:
        print(f"Failed to convert {csv_gz_path} to .parquet: {e}")
        return None


def verify_gcs_upload(blob_name):
    """Verify if the file was successfully uploaded to GCS."""
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    """Upload the .parquet file to GCS."""
    folder_name = "yellow_taxi"
    blob_name = f"{folder_name}/{os.path.basename(file_path)}"
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    # Step 1: Download .csv.gz files
    with ThreadPoolExecutor(max_workers=4) as executor:
        csv_gz_paths = list(executor.map(download_file, MONTHS))

    # Step 2: Convert .csv.gz files to .parquet
    with ThreadPoolExecutor(max_workers=4) as executor:
        parquet_paths = list(executor.map(convert_to_parquet, filter(None, csv_gz_paths)))

    # Step 3: Upload .parquet files to GCS
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, parquet_paths))

    print("All files processed and verified.")
