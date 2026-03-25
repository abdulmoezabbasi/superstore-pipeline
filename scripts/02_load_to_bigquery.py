from google.cloud import bigquery

# Config
PROJECT_ID = "superstore-pipeline-491319"
DATASET_ID = "raw"
TABLE_ID = "superstore"
BUCKET_URI = "gs://superstore-pipeline/raw/superstore.csv"

def load_to_bigquery():
    client = bigquery.Client(project=PROJECT_ID)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # skip header row
        autodetect=True,       # auto detect column types
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # overwrite if exists
    )
    
    print(f"⏳ Loading data from {BUCKET_URI} into {table_ref}...")
    
    load_job = client.load_table_from_uri(
        BUCKET_URI,
        table_ref,
        job_config=job_config
    )
    
    load_job.result()  # wait for job to finish
    
    table = client.get_table(table_ref)
    print(f"✅ Loaded {table.num_rows} rows into {table_ref}")

if __name__ == "__main__":
    load_to_bigquery()