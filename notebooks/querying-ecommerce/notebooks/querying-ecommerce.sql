-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Querying Files with Spark SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## JSON Data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json")

-- COMMAND ----------

-- File contents queried directly using Spark SQL
SELECT
    *
FROM
    json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json/customers_001.json`;

-- COMMAND ----------

-- Multiple files queried directly using Spark SQL
SELECT
    *
FROM
    json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json/customers_*.json`;

-- COMMAND ----------


-- Count the total number of records in the JSON files located at the specified path 
-- Record: row in a table
SELECT
    COUNT(*)
FROM
-- If you don't specify a file, the entire directory is queried
    json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json`;

-- COMMAND ----------

SELECT
    *,  -- Select all columns from the JSON files
    input_file_name() AS source_file  -- Add a new column 'source_file' that contains the name of the file from which each record was read
    -- AS creates an alias, which is a temp name given to a table or column for the duration of a query.
FROM
    json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json`;  -- Specify the path to the JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Text Data

-- COMMAND ----------

-- Create a table from the text file
SELECT
  *
FROM 
  text. `file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json/customers_003.txt`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Binary Data

-- COMMAND ----------

SELECT 
  *
FROM
  binaryFile. `file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json/customers_004.bin`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CSV Data

-- COMMAND ----------

-- Create a table from the CSV files
SELECT
    *
FROM
    csv.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/transactions-csv`;

-- COMMAND ----------

CREATE TABLE
    transactions_csv (
        transaction_id STRING,
        customer_id STRING,
        amount DOUBLE,
        currency STRING,
        timestamp STRING
    ) USING CSV OPTIONS (header = "true", delimiter = ";") LOCATION 'file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/transactions-csv';

-- COMMAND ----------

SELECT
    *
FROM
    transactions_csv;

-- COMMAND ----------

DESCRIBE EXTENDED transactions_csv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delta Tables - Creating and Managing

-- COMMAND ----------

CREATE TABLE
    customers_delta AS
SELECT
    *
FROM
    json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json`;

DESCRIBE EXTENDED customers_delta;

-- COMMAND ----------

CREATE TEMP VIEW transactions_tmp_vw (
    transaction_id STRING,
    customer_id STRING,
    amount DOUBLE,
    currency STRING,
    timestamp STRING
) USING CSV OPTIONS (
    path = "file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/transactions-csv/transactions_*.csv",
    header = "true",
    delimiter = ";"
);

CREATE TABLE
    transactions_delta AS
SELECT
    *
FROM
    transactions_tmp_vw;

DESCRIBE EXTENDED transactions_delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Testing Data Refresh with Non-Delta Tables

-- COMMAND ----------

-- Add a new file to the directory manually or programmatically
REFRESH TABLE transactions_csv;

SELECT
    COUNT(*)
FROM
    transactions_csv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## End of Notebook
