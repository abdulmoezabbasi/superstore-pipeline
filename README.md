# Superstore Sales Pipeline

I built this project to get hands-on experience with cloud data engineering. 
The idea was simple: take a raw CSV file and build a proper pipeline around 
it, cloud storage, a data warehouse, SQL models, and some actual analysis 
at the end.

## What it does

Takes the Superstore sales dataset and runs it through a full ELT pipeline:

- Uploads the raw CSV to Google Cloud Storage
- Loads it into BigQuery as a raw table
- Transforms it into a proper dimensional model (fact + dim tables)
- Runs analytics queries to pull out business insights

## What I found

- Total revenue of $2.3M across 9,994 transactions
- 12.47% overall profit margin
- The West region drove the most revenue ($725K) and profit ($108K)
- Top 20% of customers account for 48% of total revenue
- Technology/Copiers had the highest profit margin by sub-category
- Sean Miller was the #1 customer by sales ($25K) but actually ran at a loss (-$1,980 profit) — interesting edge case

## Tech used

Python, Google Cloud Storage, BigQuery, SQL

## How to run it
```bash
# Setup
python3 -m venv venv
source venv/bin/activate
pip install google-cloud-bigquery google-cloud-storage pandas

# Run pipeline steps in order
python scripts/01_upload_to_gcs.py
python scripts/02_load_to_bigquery.py
python scripts/03_run_transforms.py
python scripts/04_analytics.py
```

## Project structure
```
superstore-pipeline/
├── data/
│   └── superstore.csv
├── scripts/
│   ├── 01_upload_to_gcs.py
│   ├── 02_load_to_bigquery.py
│   ├── 03_transform.sql
│   ├── 03_run_transforms.py
│   └── 04_analytics.py
└── README.md
```
