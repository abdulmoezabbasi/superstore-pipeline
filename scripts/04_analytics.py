from google.cloud import bigquery

PROJECT_ID = "superstore-pipeline-491319"
client = bigquery.Client(project=PROJECT_ID)

def run_query(title, sql):
    print(f"\n{'='*50}")
    print(f"📊 {title}")
    print('='*50)
    results = client.query(sql).result()
    for row in results:
        print(dict(row))

# 1. Total Revenue & Profit
run_query("Overall Business Performance", f"""
    SELECT 
        ROUND(SUM(sales), 2) AS total_sales,
        ROUND(SUM(profit), 2) AS total_profit,
        ROUND(SUM(profit)/SUM(sales)*100, 2) AS profit_margin_pct,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS total_customers
    FROM `{PROJECT_ID}.analytics.fact_sales`
""")

# 2. Revenue by Region
run_query("Sales by Region", f"""
    SELECT 
        region,
        ROUND(SUM(sales), 2) AS total_sales,
        ROUND(SUM(profit), 2) AS total_profit
    FROM `{PROJECT_ID}.analytics.fact_sales`
    GROUP BY region
    ORDER BY total_sales DESC
""")

# 3. Top 10 Customers by Revenue
run_query("Top 10 Customers by Revenue", f"""
    SELECT 
        c.customer_name,
        c.segment,
        ROUND(SUM(f.sales), 2) AS total_sales,
        ROUND(SUM(f.profit), 2) AS total_profit
    FROM `{PROJECT_ID}.analytics.fact_sales` f
    JOIN `{PROJECT_ID}.analytics.dim_customer` c 
        ON f.customer_id = c.customer_id
    GROUP BY c.customer_name, c.segment
    ORDER BY total_sales DESC
    LIMIT 10
""")

# 4. Most Profitable Product Categories
run_query("Profit by Category", f"""
    SELECT 
        p.category,
        p.sub_category,
        ROUND(SUM(f.sales), 2) AS total_sales,
        ROUND(SUM(f.profit), 2) AS total_profit
    FROM `{PROJECT_ID}.analytics.fact_sales` f
    JOIN `{PROJECT_ID}.analytics.dim_product` p 
        ON f.product_id = p.product_id
    GROUP BY p.category, p.sub_category
    ORDER BY total_profit DESC
    LIMIT 10
""")

# 5. Top 20% customers revenue contribution
run_query("Top 20% Customers Revenue Share", f"""
    WITH customer_sales AS (
        SELECT 
            customer_id,
            SUM(sales) AS total_sales
        FROM `{PROJECT_ID}.analytics.fact_sales`
        GROUP BY customer_id
    ),
    ranked AS (
        SELECT *,
            NTILE(5) OVER (ORDER BY total_sales DESC) AS quintile
        FROM customer_sales
    )
    SELECT 
        ROUND(SUM(CASE WHEN quintile = 1 THEN total_sales END) / SUM(total_sales) * 100, 2) AS top_20pct_revenue_share
    FROM ranked
""")

print("\n✅ All analytics complete!")