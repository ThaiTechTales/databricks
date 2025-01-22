-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Incremental Data Ingestion Using Auto Loader
-- MAGIC This notebook demonstrates how to use Auto Loader in Databricks for incremental data ingestion with a simplified dataset.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. Setting Up the Environment
-- MAGIC Before starting, ensure the source directory and required tables are created.

-- COMMAND ----------

-- MAGIC %python
# Import required modules
from pyspark.sql.functions import *
from datetime import datetime

# Define source and checkpoint directories
source_dir = "dbfs:/mnt/demo/simple-data-raw"
checkpoint_dir = "dbfs:/mnt/demo/simple-data-checkpoint"

# Recreate source directory
dbutils.fs.mkdirs(source_dir)

# Helper function to generate simple data
def generate_simple_data(directory, file_count=1):
    for i in range(file_count):
        df = (spark.createDataFrame([
            (101, "2025-01-01", 99.99),
            (102, "2025-01-02", 49.50),
            (103, "2025-01-03", 29.99)
        ], ["id", "date", "value"])
        .withColumn("date", col("date").cast("date"))  # Ensure date format
        .withColumn("id", col("id").cast("int"))  # Ensure id is int
        .withColumn("value", col("value").cast("double")))  # Ensure value is double
        df.write.mode("overwrite").parquet(f"{directory}/file_{i}.parquet")

# Generate initial dataset
generate_simple_data(source_dir, file_count=1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. Explore the Source Directory
-- MAGIC List the files in the source directory to confirm data generation.

-- COMMAND ----------

-- MAGIC %python
# List files in the source directory
files = dbutils.fs.ls(source_dir)
display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 3. Auto Loader Stream Setup
-- MAGIC Use Auto Loader to ingest data incrementally.

-- COMMAND ----------

-- MAGIC %python
# Define the Auto Loader streaming query
streaming_df = (spark.readStream
    .format("cloudFiles")  # Auto Loader format
    .option("cloudFiles.format", "parquet")  # Source file format
    .option("cloudFiles.schemaLocation", checkpoint_dir)  # Schema storage
    .load(source_dir)  # Source directory
)

# Write the stream to a Delta table
(streaming_df.writeStream
    .option("checkpointLocation", checkpoint_dir)  # Checkpoint directory
    .outputMode("append")  # Append new records
    .toTable("simple_data_table")  # Target Delta table
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 4. Query the Target Table
-- MAGIC Verify that the data has been ingested.

-- COMMAND ----------

-- MAGIC %sql
SELECT * FROM simple_data_table;

-- COMMAND ----------

-- MAGIC %sql
SELECT COUNT(*) AS total_records FROM simple_data_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 5. Simulating New Data Arrival
-- MAGIC Add new data files to simulate real-time ingestion.

-- COMMAND ----------

-- MAGIC %python
# Generate additional data
generate_simple_data(source_dir, file_count=2)

# List the updated source directory
files = dbutils.fs.ls(source_dir)
display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 6. Validate New Data Ingestion
-- MAGIC Confirm that the new data has been processed by the stream.

-- COMMAND ----------

-- MAGIC %sql
SELECT COUNT(*) AS total_records FROM simple_data_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 7. Table History
-- MAGIC Explore the history of the Delta table to see schema versions and data changes.

-- COMMAND ----------

-- MAGIC %sql
DESCRIBE HISTORY simple_data_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 8. Cleaning Up Resources
-- MAGIC Remove the created resources to maintain a clean environment.

-- COMMAND ----------

-- MAGIC %sql
DROP TABLE IF EXISTS simple_data_table;

-- COMMAND ----------

-- MAGIC %python
# Clean up the source and checkpoint directories
dbutils.fs.rm(source_dir, recurse=True)
dbutils.fs.rm(checkpoint_dir, recurse=True)
