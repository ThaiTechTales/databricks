-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Incremental Data Ingestion Using Auto Loader
-- MAGIC This notebook demonstrates how to use Auto Loader in Databricks for incremental data ingestion with a simple dataset.

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
source_dir = "dbfs:/mnt/demo/simple-sales-raw"
checkpoint_dir = "dbfs:/mnt/demo/simple-sales-checkpoint"

# Recreate source directory
dbutils.fs.mkdirs(source_dir)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. Create the Target Delta Table
-- MAGIC Define the `simple_sales_updates` table with the expected schema.

-- COMMAND ----------

-- MAGIC %sql
CREATE TABLE IF NOT EXISTS simple_sales_updates (
    id INT,
    order_date DATE,
    value DOUBLE
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 3. Generate Simple Data
-- MAGIC Create and load Parquet files into the source directory.

-- COMMAND ----------

-- MAGIC %python
# Helper function to generate simple data
def generate_simple_data(directory, file_count=1):
    for i in range(file_count):
        df = (spark.createDataFrame([
            (101, "2025-01-01", 99.99),
            (102, "2025-01-02", 49.50),
            (103, "2025-01-03", 29.99)
        ], ["id", "order_date", "value"])
        .withColumn("order_date", col("order_date").cast("date"))  # Ensure correct type
        .withColumn("id", col("id").cast("int"))  # Ensure id is int
        .withColumn("value", col("value").cast("double")))  # Ensure value is double
        df.write.mode("overwrite").parquet(f"{directory}/file_{i}.parquet")

# Generate initial dataset
generate_simple_data(source_dir, file_count=1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 4. Verify the Source Data
-- MAGIC Confirm that the data exists in the source directory.

-- COMMAND ----------

-- MAGIC %python
# List files in the source directory
files = dbutils.fs.ls(source_dir)
display(files)

-- COMMAND ----------

-- MAGIC %python
# Check the content and schema of the Parquet file
file_path = f"{source_dir}/file_0.parquet"  # Use an actual file path from the directory
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
# Define the Auto Loader streaming query with explicit type casting
streaming_df = (spark.readStream
    .format("cloudFiles")  # Auto Loader format
    .option("cloudFiles.format", "parquet")  # Source file format
    .option("cloudFiles.schemaLocation", checkpoint_dir)  # Schema storage
    .load(source_dir)  # Source directory
    .withColumn("id", col("id").cast("int"))  # Explicitly cast id to int
    .withColumn("order_date", col("order_date").cast("date"))  # Ensure date format
    .withColumn("value", col("value").cast("double"))  # Ensure value is double
)

(streaming_df.writeStream
    .option("checkpointLocation", checkpoint_dir)  # Checkpoint directory
    .outputMode("append")  # Append new records
    .toTable("simple_sales_updates")  # Target Delta table
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 6. Validate the Data Ingestion
-- MAGIC Query the `simple_sales_updates` table to verify data ingestion.

-- COMMAND ----------

-- MAGIC %sql
SELECT * FROM simple_sales_updates;

-- COMMAND ----------

-- MAGIC %sql
SELECT COUNT(*) AS total_records FROM simple_sales_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 7. Simulate New Data Arrival
-- MAGIC Add more data to the source directory to simulate real-time updates.

-- COMMAND ----------

-- MAGIC %python
# Generate additional data files
generate_simple_data(source_dir, file_count=2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 8. Verify Ingestion of New Data
-- MAGIC Confirm that the new data has been processed by the streaming query.

-- COMMAND ----------

-- MAGIC %sql
SELECT COUNT(*) AS total_records FROM simple_sales_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 9. Explore Table History
-- MAGIC Delta Lake maintains a history of all operations on the table.

-- COMMAND ----------

-- MAGIC %sql
DESCRIBE HISTORY simple_sales_updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 10. Clean Up Resources
-- MAGIC Drop the Delta table and delete the directories.

-- COMMAND ----------

-- MAGIC %sql
DROP TABLE IF EXISTS simple_sales_updates;

-- COMMAND ----------

-- MAGIC %python
# Clean up directories
dbutils.fs.rm(source_dir, recurse=True)
dbutils.fs.rm(checkpoint_dir, recurse=True)