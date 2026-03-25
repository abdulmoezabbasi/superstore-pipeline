from google.cloud import storage
import os

# Config
BUCKET_NAME = "superstore-pipeline"
SOURCE_FILE = "data/superstore.csv"
DESTINATION_BLOB = "raw/superstore.csv"

def upload_to_gcs():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(DESTINATION_BLOB)
    
    blob.upload_from_filename(SOURCE_FILE)
    print(f"✅ Uploaded {SOURCE_FILE} to gs://{BUCKET_NAME}/{DESTINATION_BLOB}")

if __name__ == "__main__":
    upload_to_gcs()