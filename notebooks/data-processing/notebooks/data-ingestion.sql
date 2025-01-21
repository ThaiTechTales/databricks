-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Incremental Data Ingestion Using Auto Loader
-- MAGIC This notebook demonstrates how to use Auto Loader in Databricks for incremental data ingestion.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. Setting Up the Environment
-- MAGIC Before starting, ensure the source directory and required tables are created.

-- COMMAND ----------
-- MAGIC %python
from pyspark.sql.functions import *
from datetime import datetime

# Directory setup
source_dir = "dbfs:/mnt/demo/sales-raw"
checkpoint_dir = "dbfs:/mnt/demo/sales_checkpoint"

# Helper function to simulate new data ingestion
def generate_sample_data(directory, file_count=1):
    for i in range(file_count):
        df = spark.range(1000).withColumn("order_id", expr("id"))
        df = df.withColumn("order_date", lit(datetime.now()))
        df = df.withColumn("customer_id", expr("id % 100"))
        df = df.withColumn("amount", expr("rand() * 100"))
        df.write.mode("overwrite").parquet(f"{directory}/file_{i}.parquet")

# Create initial dataset
dbutils.fs.mkdirs(source_dir)
generate_sample_data(source_dir)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. Explore the Source Directory
-- MAGIC Listing all the files in the source directory to confirm the setup.

-- COMMAND ----------
-- MAGIC %python
files = dbutils.fs.ls(source_dir)
display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 3. Auto Loader Stream Setup
-- MAGIC The following code demonstrates the use of Auto Loader to read and process data incrementally.

-- COMMAND ----------
-- MAGIC %python
(spark.readStream
    .format("cloudFiles")  -- Use Auto Loader format
    .option("cloudFiles.format", "parquet")  -- Source file format
    .option("cloudFiles.schemaLocation", checkpoint_dir)  -- Schema storage for incremental updates
    .load(source_dir)  -- Source directory
    .writeStream
    .option("checkpointLocation", checkpoint_dir)  -- Checkpoint directory
    .outputMode("append")  -- Append mode for incremental processing
    .table("sales_updates")  -- Target table
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 4. Query the Target Table
-- MAGIC Verify the data ingestion process by querying the target table.

-- COMMAND ----------
-- MAGIC %sql
SELECT * FROM sales_updates LIMIT 10;

-- COMMAND ----------
-- MAGIC %sql
SELECT count(*) AS total_records FROM sales_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 5. Simulating New Data Arrival
-- MAGIC Add new data files to the source directory to simulate real-time ingestion.

-- COMMAND ----------
-- MAGIC %python
generate_sample_data(source_dir, file_count=2)

-- COMMAND ----------
-- MAGIC %python
files = dbutils.fs.ls(source_dir)
display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 6. Validate Ingestion of New Data
-- MAGIC Confirm the new data is ingested by the Auto Loader stream.

-- COMMAND ----------
-- MAGIC %sql
SELECT count(*) AS total_records FROM sales_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 7. Table History
-- MAGIC Explore the table history to understand how Delta Lake manages versions.

-- COMMAND ----------
-- MAGIC %sql
DESCRIBE HISTORY sales_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 8. Cleaning Up Resources
-- MAGIC Remove the created resources to maintain a clean environment.

-- COMMAND ----------
-- MAGIC %sql
DROP TABLE IF EXISTS sales_updates;

-- COMMAND ----------
-- MAGIC %python
dbutils.fs.rm(source_dir, True)
dbutils.fs.rm(checkpoint_dir, True)