-- Databricks Notebook SQL File
-- Comprehensive Learning of Writing to Delta Tables with ACID Transactions
-- COMMAND ----------
-- Step 1: Set Up Directories and Import Data
-- COMMAND ----------
-- Step 2: Create Delta Tables
-- Create `orders` Delta table from Parquet
CREATE TABLE
    orders USING DELTA AS
SELECT
    *
FROM
    parquet.`/mnt/demo/bookstore/orders`;

-- Verify the data
SELECT
    *
FROM
    orders;

-- COMMAND ----------
-- Step 3: Overwriting Data with CREATE OR REPLACE TABLE
-- Replace `orders` table data completely using CREATE OR REPLACE TABLE
CREATE
OR REPLACE TABLE orders USING DELTA AS
SELECT
    *
FROM
    parquet.`/mnt/demo/bookstore/orders`;

-- Check table history
DESCRIBE HISTORY orders;

-- COMMAND ----------
-- Step 4: Overwriting Data with INSERT OVERWRITE
-- Overwrite `orders` table data using INSERT OVERWRITE
INSERT OVERWRITE orders
SELECT
    *
FROM
    parquet.`/mnt/demo/bookstore/orders-new`;

-- Verify the data
SELECT
    *
FROM
    orders;

-- Check table history
DESCRIBE HISTORY orders;

-- COMMAND ----------
-- Attempt Schema Mismatch Scenario with INSERT OVERWRITE
-- Attempt to insert data with a mismatched schema
INSERT OVERWRITE orders
SELECT
    *,
    current_timestamp() AS new_column
FROM
    parquet.`/mnt/demo/bookstore/orders`;

-- COMMAND ----------
-- Step 5: Appending Data
-- Append new records to `orders` table
INSERT INTO
    orders
SELECT
    *
FROM
    parquet.`/mnt/demo/bookstore/orders-new`;

-- Check the number of records
SELECT
    COUNT(*)
FROM
    orders;

-- COMMAND ----------
-- Step 6: Merging Data
-- Create or Replace Temporary View for customer updates
CREATE
OR REPLACE TEMP VIEW customers_updates AS
SELECT
    *
FROM
    json.`/mnt/demo/bookstore/customers-json-new`;

-- Merge data into `customers` table
MERGE INTO customers c USING customers_updates u ON c.customer_id = u.customer_id WHEN MATCHED
AND c.email IS NULL
AND u.email IS NOT NULL THEN
UPDATE
SET
    email = u.email,
    updated = u.updated WHEN NOT MATCHED THEN INSERT *;

-- Verify the changes
SELECT
    *
FROM
    customers;

-- COMMAND ----------
-- Step 7: Conditional Merge with Specific Criteria
-- Create a temporary view for new book updates
CREATE
OR REPLACE TEMP VIEW books_updates (
    book_id STRING,
    title STRING,
    author STRING,
    category STRING,
    price DOUBLE
) USING CSV OPTIONS (
    path = "/mnt/demo/bookstore/books-csv-new",
    header = "true",
    delimiter = ";"
);

-- Preview the new data
SELECT
    *
FROM
    books_updates;

-- Merge into the `books` table only if the category is 'Computer Science'
MERGE INTO books b USING books_updates u ON b.book_id = u.book_id
AND b.title = u.title WHEN NOT MATCHED
AND u.category = 'Computer Science' THEN INSERT *;

-- Verify the changes
SELECT
    *
FROM
    books;