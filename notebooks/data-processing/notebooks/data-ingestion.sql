-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Incremental Data Ingestion Using Auto Loader
-- MAGIC This notebook demonstrates how to use Auto Loader in Databricks for incremental data ingestion while addressing schema mismatches.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. Setting Up the Environment
-- MAGIC Define directories and create the target Delta table.

-- COMMAND ----------

-- MAGIC %python
# Import required modules
from pyspark.sql.functions import *
from datetime import datetime

# Define source and checkpoint directories
source_dir = "dbfs:/FileStore/repos/databricks/notebooks/data-processing/notebooks/data/simple-sales-raw"
checkpoint_dir = "dbfs:/FileStore/repos/databricks/notebooks/data-processing/notebooks/data/simple-sales-checkpoint"

# Recreate source directory
dbutils.fs.mkdirs(source_dir)

# Clean up source directory (ensure no old files exist)
dbutils.fs.rm(source_dir, recurse=True)
dbutils.fs.mkdirs(source_dir)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. Create the Target Delta Table
-- MAGIC Define the `simple_sales_updates` table with the expected schema.

-- COMMAND ----------

DROP TABLE IF EXISTS default.simple_sales_updates;

CREATE TABLE default.simple_sales_updates (
    order_id INT,
    order_date DATE,
    amount DOUBLE
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 3. Generate Simple Data
-- MAGIC Create and load Parquet files into the source directory.

-- COMMAND ----------

-- MAGIC %python
from datetime import date

# Function to generate sample data
def generate_sample_data(directory, file_count=1):
    for i in range(file_count):
        # Create a DataFrame with sample data
        df = spark.createDataFrame(
            [
                (101, date(2025, 1, 1), 99.99),
                (102, date(2025, 1, 2), 49.50),
                (103, date(2025, 1, 3), 29.99),
            ],
            ["id", "order_date", "value"]
        )
        # Write the data to a Parquet file
        df.write.mode("overwrite").parquet(f"{directory}/file_{i}.parquet")

# Generate one file in the source directory
generate_sample_data(source_dir, file_count=1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 4. Verify the Source Data
-- MAGIC Confirm that the data exists in the source directory.

-- COMMAND ----------

-- MAGIC %python
# List files in the directory
files = dbutils.fs.ls(source_dir)
display(files)

-- COMMAND ----------

-- MAGIC %python
# Check the content of the file
file_path = f"{source_dir}/file_0.parquet"
df = spark.read.format("parquet").load(file_path)
df.printSchema()
df.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 5. Configure and Start the Streaming Query
-- MAGIC Use Auto Loader to ingest data into the `simple_sales_updates` Delta table.

-- COMMAND ----------

-- MAGIC %python
# Clear the checkpoint directory (optional, to avoid state conflicts)
dbutils.fs.rm(checkpoint_dir, recurse=True)
dbutils.fs.mkdirs(checkpoint_dir)

# Define the streaming query
streaming_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")  # Source file format
    .option("cloudFiles.schemaLocation", checkpoint_dir)  # Schema storage
    .load(source_dir)  # Source directory
    .withColumnRenamed("id", "order_id")  # Rename column to match Delta table schema
    .withColumnRenamed("value", "amount")  # Rename column to match Delta table schema
    .select("order_id", "order_date", "amount")  # Select relevant columns
)

# Write the stream to the Delta table
(streaming_df.writeStream
    .option("checkpointLocation", checkpoint_dir)  # Checkpoint directory
    .option("mergeSchema", "true")  # Enable schema merging
    .outputMode("append")  # Append mode
    .toTable("default.simple_sales_updates")  # Fully qualified table name
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 6. Validate the Data Ingestion
-- MAGIC Query the `simple_sales_updates` table to verify data ingestion.

-- COMMAND ----------

SELECT * FROM simple_sales_updates;

-- COMMAND ----------

SELECT COUNT(*) AS total_records FROM simple_sales_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 7. Simulate New Data Arrival
-- MAGIC Add more data to the source directory to simulate real-time updates.

-- COMMAND ----------

-- MAGIC %python
# Generate additional data files
generate_sample_data(source_dir, file_count=3)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 8. Verify Ingestion of New Data
-- MAGIC Confirm that the new data has been processed by the streaming query.

-- COMMAND ----------

SELECT COUNT(*) AS total_records FROM simple_sales_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 9. Explore Table History
-- MAGIC Delta Lake maintains a history of all operations on the table.

-- COMMAND ----------

DESCRIBE HISTORY simple_sales_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 10. Clean Up Resources
-- MAGIC Drop the Delta table and delete the directories.

-- COMMAND ----------

DROP TABLE IF EXISTS simple_sales_updates;

-- COMMAND ----------

-- MAGIC %python
# Clean up directories
dbutils.fs.rm(source_dir, recurse=True)
dbutils.fs.rm(checkpoint_dir, recurse=True)