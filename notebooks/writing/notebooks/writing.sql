-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Comprehensive Learning of Writing to Delta Tables with ACID Transactions Using JSON Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Set Up Directories and Import Data
-- MAGIC Ensure JSON files are located in the directory:  
-- MAGIC `file:/Workspace/Users/<user>/databricks/notebooks/writing/data/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Create Delta Tables
-- MAGIC Create `orders` Delta table from JSON.

-- COMMAND ----------

drop table orders

-- COMMAND ----------

CREATE TABLE orders AS
SELECT
    *
FROM
    json.`file:/Workspace/Users/<user>/databricks/notebooks/writing/data/orders.json`;

-- COMMAND ----------

-- Verify the data.
SELECT
    *
FROM
    orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Overwriting Data with CREATE OR REPLACE TABLE
-- MAGIC Replace `orders` table data completely using CREATE OR REPLACE TABLE.

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT
    *
FROM
    json.`file:/Workspace/Users/<user>/databricks/notebooks/writing/data/orders.json`;

-- COMMAND ----------

-- Check table history.
DESCRIBE HISTORY orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Overwriting Data with INSERT OVERWRITE
-- MAGIC Overwrite `orders` table data using INSERT OVERWRITE.

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT
    *
FROM
    json.`file:/Workspace/Users/<user>/databricks/notebooks/writing/data/orders-new.json`;

-- COMMAND ----------

-- Verify the data.
SELECT
    *
FROM
    orders;

-- COMMAND ----------

-- Check table history.
DESCRIBE HISTORY orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Attempt Schema Mismatch Scenario with INSERT OVERWRITE
-- MAGIC Attempt to insert data with a mismatched schema.

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT
    *,
    current_timestamp() AS new_column
FROM
    json.`file:/Workspace/Users/<user>/databricks/notebooks/writing/data/orders.json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Appending Data
-- MAGIC Append new records to `orders` table.

-- COMMAND ----------

INSERT INTO
    orders
SELECT
    *
FROM
    json.`file:/Workspace/Users/<user>/databricks/notebooks/writing/data/orders-new.json`;

-- COMMAND ----------

-- Check the number of records.
SELECT
    COUNT(*)
FROM
    orders;

-- COMMAND ----------

-- Verify the data.
SELECT
    *
FROM
    orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 6: Merging Data
-- MAGIC Create or Replace Temporary View for customer updates.

-- COMMAND ----------

-- This temporary view is used to stage the data from the JSON file so that it can be easily accessed and manipulated in subsequent SQL operations without repeatedly reading the file.
CREATE OR REPLACE TEMP VIEW customers_updates AS
SELECT
    *
FROM
    json.`file:/Workspace/Users/<user>/databricks/notebooks/writing/data/customers-updates.json`;

-- COMMAND ----------

DROP TABLE IF EXISTS customers;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS
    customers AS
SELECT
    *
FROM
    json.`file:/Workspace/Users/<user>/databricks/notebooks/writing/data/customers.json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

-- Verify the data.
SELECT
    *
FROM
    customers;

-- COMMAND ----------

-- This MERGE operation ensures that the customers table is updated with new email addresses and timestamps from the customers_updates view, and new records are inserted if they do not already exist in the customers table.

-- WHEN MATCHED: This clause updates existing records in the customers table if a match is found based on customer_id and additional conditions.
-- WHEN NOT MATCHED: This clause inserts new records into the customers table if no match is found based on customer_id.
-- This logic ensures that each customer_id is unique in the customers table, thus preventing duplicates.

-- Merge data into `customers` table.
-- MERGE INTO customers c: 
    -- This specifies the target table for the merge operation, which is customers.
    -- The alias c is used to reference the customers table.
MERGE INTO customers c 

-- USING customers_updates u:
    -- This specifies the source of the data for the merge operation, which is the customers_updates temporary view.
    -- The alias u is used to reference the customers_updates view.

-- ON c.customer_id = u.customer_id:
    -- This specifies the condition for matching records between the customers table and the customers_updates view.
    -- Records are matched based on the customer_id column.
USING customers_updates u ON c.customer_id = u.customer_id 

-- WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
    -- This part of the MERGE statement specifies the condition for updating records in the target table (customers) when a match is found.

    -- WHEN MATCHED: This indicates that the following actions should be taken when a record in the customers table matches a record in the customers_updates view based on the ON condition specified earlier.
    -- AND c.email IS NULL AND u.email IS NOT NULL: This adds an additional condition to the WHEN MATCHED clause. It specifies that the update should only occur if the email in the customers table is NULL and the email in the customers_updates view is not NULL.
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN

-- UPDATE SET email = u.email, last_updated = u.last_updated:
    -- This specifies the update action to be performed when the match condition is met.
    -- The email and last_updated columns in the customers table are updated with the corresponding values from the customers_updates view.
UPDATE SET
    email = u.email,
    last_updated = u.last_updated 

-- WHEN NOT MATCHED THEN INSERT:
    -- This specifies the action to be performed when there is no match between the customers table and the customers_updates view.
    -- The INSERT * statement inserts the entire record from the customers_updates view into the customers table.
WHEN NOT MATCHED THEN 

INSERT *;

-- COMMAND ----------

-- Verify the changes.
SELECT
    *
FROM
    customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 7: Conditional Merge with Specific Criteria
-- MAGIC Create a temporary view for new book updates.

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW books_updates (
    book_id STRING,
    title STRING,
    author STRING,
    category STRING,
    price DOUBLE
) USING JSON OPTIONS (
    path = "file:/Workspace/Users/<user>/databricks/notebooks/writing/data/books-updates.json"
);

-- COMMAND ----------

SELECT 
  *
FROM books_updates

-- COMMAND ----------

DROP TABLE IF EXISTS books

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS
    books AS
SELECT
    *
FROM
    json.`file:/Workspace/Users/<user>/databricks/notebooks/writing/data/books.json`;

DESCRIBE books;

-- COMMAND ----------

-- Preview the new data.
SELECT
    *
FROM
    books;

-- COMMAND ----------

-- Merge into the `books` table only if the category is 'Programming'.
MERGE INTO books b 

USING books_updates u ON b.book_id = u.book_id
AND b.title = u.title WHEN NOT MATCHED
AND u.category = 'Programming' THEN INSERT *;

-- COMMAND ----------

-- Verify the changes.
SELECT
    *
FROM
    books;
