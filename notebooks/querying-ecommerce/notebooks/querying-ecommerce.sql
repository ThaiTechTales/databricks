-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Querying Files with Spark SQL - Ecommerce Transactions Dataset
-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Querying JSON Data - Customers
-- COMMAND ----------
SELECT
    *
FROM
    json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json/customers_001.json`;

-- COMMAND ----------
SELECT
    *
FROM
    json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json/customers_*.json`;

-- COMMAND ----------
SELECT
    *
FROM
    json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json`;

-- COMMAND ----------
SELECT
    COUNT(*)
FROM
    json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json`;

-- COMMAND ----------
SELECT
    *,
    input_file_name () AS source_file
FROM
    json.`file:/Workspace/Users/thai.le.trial.02@gmail.com/databricks/notebooks/querying-ecommerce/data/customers-json`;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Querying CSV Data - Transactions
-- COMMAND ----------
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