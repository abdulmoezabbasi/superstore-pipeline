-- ============================================
-- STEP 1: Create analytics dataset
-- ============================================
-- (Run this first time only in BigQuery console)
-- CREATE SCHEMA `superstore-pipeline-491319.analytics`;

-- ============================================
-- STEP 2: dim_customer
-- ============================================
CREATE OR REPLACE TABLE `superstore-pipeline-491319.analytics.dim_customer` AS
SELECT DISTINCT
    `Customer ID`   AS customer_id,
    `Customer Name` AS customer_name,
    Segment         AS segment
FROM `superstore-pipeline-491319.raw.superstore`;

-- ============================================
-- STEP 3: dim_product
-- ============================================
CREATE OR REPLACE TABLE `superstore-pipeline-491319.analytics.dim_product` AS
SELECT DISTINCT
    `Product ID`   AS product_id,
    `Product Name` AS product_name,
    Category       AS category,
    `Sub-Category` AS sub_category
FROM `superstore-pipeline-491319.raw.superstore`;

-- ============================================
-- STEP 4: dim_region
-- ============================================
CREATE OR REPLACE TABLE `superstore-pipeline-491319.analytics.dim_region` AS
SELECT DISTINCT
    Region  AS region,
    State   AS state,
    City    AS city
FROM `superstore-pipeline-491319.raw.superstore`;

-- ============================================
-- STEP 5: fact_sales
-- ============================================
CREATE OR REPLACE TABLE `superstore-pipeline-491319.analytics.fact_sales` AS
SELECT
    `Order ID`    AS order_id,
    `Order Date`  AS order_date,
    `Ship Date`   AS ship_date,
    `Customer ID` AS customer_id,
    `Product ID`  AS product_id,
    Region        AS region,
    Sales         AS sales,
    Quantity      AS quantity,
    Discount      AS discount,
    Profit        AS profit
FROM `superstore-pipeline-491319.raw.superstore`;


