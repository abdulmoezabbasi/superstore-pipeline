from google.cloud import bigquery

PROJECT_ID = "superstore-pipeline-491319"

def run_transforms():
    client = bigquery.Client(project=PROJECT_ID)

    # Create analytics dataset first
    dataset_id = f"{PROJECT_ID}.analytics"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    
    try:
        client.create_dataset(dataset)
        print("✅ Created analytics dataset")
    except Exception:
        print("ℹ️ Analytics dataset already exists")

    # Define transforms directly in Python
    statements = [
        f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.analytics.dim_customer` AS
        SELECT DISTINCT
            `Customer ID` AS customer_id,
            `Customer Name` AS customer_name,
            Segment AS segment
        FROM `{PROJECT_ID}.raw.superstore`""",

        f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.analytics.dim_product` AS
        SELECT DISTINCT
            `Product ID` AS product_id,
            `Product Name` AS product_name,
            Category AS category,
            `Sub-Category` AS sub_category
        FROM `{PROJECT_ID}.raw.superstore`""",

        f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.analytics.dim_region` AS
        SELECT DISTINCT
            Region AS region,
            State AS state,
            City AS city
        FROM `{PROJECT_ID}.raw.superstore`""",

        f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.analytics.fact_sales` AS
        SELECT
            `Order ID` AS order_id,
            `Order Date` AS order_date,
            `Ship Date` AS ship_date,
            `Customer ID` AS customer_id,
            `Product ID` AS product_id,
            Region AS region,
            Sales AS sales,
            Quantity AS quantity,
            Discount AS discount,
            Profit AS profit
        FROM `{PROJECT_ID}.raw.superstore`"""
    ]

    for i, statement in enumerate(statements):
        print(f"⏳ Running transform {i+1}/{len(statements)}...")
        job = client.query(statement)
        job.result()
        print(f"✅ Done")

    print("\n🎉 All transforms complete!")

if __name__ == "__main__":
    run_transforms()