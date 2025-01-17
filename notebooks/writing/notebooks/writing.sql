-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Comprehensive Learning of Writing to Delta Tables with ACID Transactions

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Step 1: Set Up Directories and Import Data

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Step 2: Create Delta Tables
-- MAGIC Create `orders` Delta table from Parquet.

-- COMMAND ----------
-- MAGIC %sql
CREATE TABLE
    orders USING DELTA AS
SELECT
    *
FROM
    parquet.`/mnt/demo/bookstore/orders`;

-- COMMAND ----------
-- MAGIC %sql
-- MAGIC Verify the data.
SELECT
    *
FROM
    orders;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Step 3: Overwriting Data with CREATE OR REPLACE TABLE
-- MAGIC Replace `orders` table data completely using CREATE OR REPLACE TABLE.

-- COMMAND ----------
-- MAGIC %sql
CREATE
OR REPLACE TABLE orders USING DELTA AS
SELECT
    *
FROM
    parquet.`/mnt/demo/bookstore/orders`;

-- COMMAND ----------
-- MAGIC %sql
-- MAGIC Check table history.
DESCRIBE HISTORY orders;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Step 4: Overwriting Data with INSERT OVERWRITE
-- MAGIC Overwrite `orders` table data using INSERT OVERWRITE.

-- COMMAND ----------
-- MAGIC %sql
INSERT OVERWRITE orders
SELECT
    *
FROM
    parquet.`/mnt/demo/bookstore/orders-new`;

-- COMMAND ----------
-- MAGIC %sql
-- MAGIC Verify the data.
SELECT
    *
FROM
    orders;

-- COMMAND ----------
-- MAGIC %sql
-- MAGIC Check table history.
DESCRIBE HISTORY orders;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Attempt Schema Mismatch Scenario with INSERT OVERWRITE
-- MAGIC Attempt to insert data with a mismatched schema.

-- COMMAND ----------
-- MAGIC %sql
INSERT OVERWRITE orders
SELECT
    *,
    current_timestamp() AS new_column
FROM
    parquet.`/mnt/demo/bookstore/orders`;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Step 5: Appending Data
-- MAGIC Append new records to `orders` table.

-- COMMAND ----------
-- MAGIC %sql
INSERT INTO
    orders
SELECT
    *
FROM
    parquet.`/mnt/demo/bookstore/orders-new`;

-- COMMAND ----------
-- MAGIC %sql
-- MAGIC Check the number of records.
SELECT
    COUNT(*)
FROM
    orders;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Step 6: Merging Data
-- MAGIC Create or Replace Temporary View for customer updates.

-- COMMAND ----------
-- MAGIC %sql
CREATE
OR REPLACE TEMP VIEW customers_updates AS
SELECT
    *
FROM
    json.`/mnt/demo/bookstore/customers-json-new`;

-- COMMAND ----------
-- MAGIC %sql
-- MAGIC Merge data into `customers` table.
MERGE INTO customers c USING customers_updates u ON c.customer_id = u.customer_id WHEN MATCHED
AND c.email IS NULL
AND u.email IS NOT NULL THEN
UPDATE
SET
    email = u.email,
    updated = u.updated WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------
-- MAGIC %sql
-- MAGIC Verify the changes.
SELECT
    *
FROM
    customers;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Step 7: Conditional Merge with Specific Criteria
-- MAGIC Create a temporary view for new book updates.

-- COMMAND ----------
-- MAGIC %sql
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

-- COMMAND ----------
-- MAGIC %sql
-- MAGIC Preview the new data.
SELECT
    *
FROM
    books_updates;

-- COMMAND ----------
-- MAGIC %sql
-- MAGIC Merge into the `books` table only if the category is 'Computer Science'.
MERGE INTO books b USING books_updates u ON b.book_id = u.book_id
AND b.title = u.title WHEN NOT MATCHED
AND u.category = 'Computer Science' THEN INSERT *;

-- COMMAND ----------
-- MAGIC %sql
-- MAGIC Verify the changes.
SELECT
    *
FROM
    books;
