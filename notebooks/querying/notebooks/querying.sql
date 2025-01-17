-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Querying Files with Spark SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## JSON Data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls("file:/Workspace/Users/<user>/databricks/notebooks/querying-ecommerce/data/transactions-csv/")
-- MAGIC display(files)

-- COMMAND ----------

-- File contents queried directly using Spark SQL
SELECT
    *
FROM
    json.`file:/Workspace/Users/<user>/databricks/notebooks/querying-ecommerce/data/customers-json/customers_001.json`;

-- COMMAND ----------

-- Multiple files queried directly using Spark SQL
-- Select all columns
SELECT
    *
FROM
    json.`file:/Workspace/Users/<user>/databricks/notebooks/querying-ecommerce/data/customers-json/customers_*.json`;

-- COMMAND ----------

-- Count the total number of records in the JSON files located at the specified path 
-- Record: row in a table
-- COUNT(*) counts the number of records in the JSON files
SELECT
    COUNT(*)
FROM
    -- If you don't specify a file, the entire directory is queried
    json.`file:/Workspace/Users/<user>/databricks/notebooks/querying-ecommerce/data/customers-json`;

-- COMMAND ----------

SELECT
    *, -- Select all columns from the JSON files
    input_file_name () AS source_file -- Add a new column 'source_file' that contains the name of the file from which each record was read
    -- AS creates an alias, which is a temp name given to a table or column for the duration of a query.
FROM
    json.`file:/Workspace/Users/<user>/databricks/notebooks/querying-ecommerce/data/customers-json/*.json`;

-- Specify the path to the JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Text Data

-- COMMAND ----------

-- Reads from the text file
SELECT
    *
FROM
    text.`file:/Workspace/Users/<user>/databricks/notebooks/querying-ecommerce/data/customers-json/customers_003.txt`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Binary Data

-- COMMAND ----------

SELECT
    *
FROM
    binaryFile.`file:/Workspace/Users/<user>/databricks/notebooks/querying-ecommerce/data/customers-json/customers_004.bin`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CSV Data

-- COMMAND ----------

-- Reads a CSV files
SELECT
    *
FROM
    csv.`file:/Workspace/Users/<user>/databricks/notebooks/querying-ecommerce/data/transactions-csv/`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS

-- COMMAND ----------

CREATE TABLE
    customers_01 AS
SELECT
    *
FROM
    json.`file:/Workspace/Users/<user>/databricks/notebooks/querying-ecommerce/data/customers-json`;

DESCRIBE EXTENDED customers_01;

-- COMMAND ----------

-- The temporary view serves as a staging layer for transformations, options handling, and schema validation.
-- Loading data into a Delta table from a temporary view ensures you get the full benefits of Delta Lake features while maintaining data integrity and performance.
-- Directly referencing external files skips this staging step, risking slower queries and limited functionality.
CREATE TEMP VIEW transactions_tmp_vw_02 (
    transaction_id STRING,
    customer_id STRING,
    amount DOUBLE,
    currency STRING,
    timestamp STRING
) USING CSV OPTIONS (
    path = "file:/Workspace/Users/<user>/databricks/notebooks/querying-ecommerce/data/transactions-csv/transactions_*.csv",
    header = "true",
    delimiter = ","
);

CREATE TABLE
    transactions_delta_02 AS
SELECT
    *
FROM
    transactions_tmp_vw_02;

DESCRIBE EXTENDED transactions_delta_02;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Refresh

-- COMMAND ----------

-- Add a new file to the directory manually or programmatically
REFRESH TABLE transactions_delta_02;

SELECT
    COUNT(*)
FROM
    transactions_delta_02;
