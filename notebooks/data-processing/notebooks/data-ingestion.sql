-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Incremental Data Ingestion Using Auto Loader
-- MAGIC This notebook demonstrates how to use Auto Loader in Databricks for incremental data ingestion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. Setting Up the Environment
-- MAGIC Define directories and create the target Delta table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Import required modules
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from datetime import datetime
-- MAGIC
-- MAGIC # Define source and checkpoint directories
-- MAGIC source_dir = "dbfs:/mnt/demo/simple-sales-raw"
-- MAGIC checkpoint_dir = "dbfs:/mnt/demo/simple-sales-checkpoint"
-- MAGIC
-- MAGIC # Recreate source directory
-- MAGIC dbutils.fs.mkdirs(source_dir)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/mnt/demo/simple-sales-raw", recurse=True)

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
-- MAGIC from datetime import date
-- MAGIC
-- MAGIC # Recreate the directory and generate data
-- MAGIC dbutils.fs.mkdirs("dbfs:/mnt/demo/simple-sales-raw")
-- MAGIC
-- MAGIC def generate_sample_data(directory, file_count=1):
-- MAGIC     for i in range(file_count):
-- MAGIC         # Create a DataFrame with sample data
-- MAGIC         df = spark.createDataFrame(
-- MAGIC             [
-- MAGIC                 (101, date(2025, 1, 1), 99.99),
-- MAGIC                 (102, date(2025, 1, 2), 49.50),
-- MAGIC                 (103, date(2025, 1, 3), 29.99),
-- MAGIC             ],
-- MAGIC             ["id", "order_date", "value"]
-- MAGIC         )
-- MAGIC         # Write the data to the Parquet file
-- MAGIC         df.write.mode("overwrite").parquet(f"{directory}/file_{i}.parquet")
-- MAGIC
-- MAGIC # Generate one file in the source directory
-- MAGIC generate_sample_data("dbfs:/mnt/demo/simple-sales-raw", file_count=1)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 4. Verify the Source Data
-- MAGIC Confirm that the data exists in the source directory.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # List files in the directory
-- MAGIC files = dbutils.fs.ls("dbfs:/mnt/demo/simple-sales-raw")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Check the content of the file
-- MAGIC file_path = "dbfs:/mnt/demo/simple-sales-raw/file_0.parquet"
-- MAGIC df = spark.read.format("parquet").load(file_path)
-- MAGIC df.printSchema()
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 5. Configure and Start the Streaming Query
-- MAGIC Use Auto Loader to ingest data into the `simple_sales_updates` Delta table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Clear the checkpoint directory (optional, to avoid state conflicts)
-- MAGIC dbutils.fs.rm("dbfs:/mnt/demo/simple-sales-checkpoint", recurse=True)
-- MAGIC
-- MAGIC # Define the streaming query
-- MAGIC streaming_df = (
-- MAGIC     spark.readStream
-- MAGIC     .format("cloudFiles")
-- MAGIC     .option("cloudFiles.format", "parquet")  # Source file format
-- MAGIC     .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/simple-sales-checkpoint")  # Schema storage
-- MAGIC     .load("dbfs:/mnt/demo/simple-sales-raw")  # Source directory
-- MAGIC     .withColumnRenamed("id", "order_id")  # Rename column to match Delta table schema
-- MAGIC     .withColumnRenamed("value", "amount")  # Rename column to match Delta table schema
-- MAGIC     .select("order_id", "order_date", "amount")  # Select relevant columns
-- MAGIC )
-- MAGIC
-- MAGIC # Write the stream to the Delta table
-- MAGIC (streaming_df.writeStream
-- MAGIC     .option("checkpointLocation", "dbfs:/mnt/demo/simple-sales-checkpoint")  # Checkpoint directory
-- MAGIC     .option("mergeSchema", "true")  # Enable schema merging
-- MAGIC     .outputMode("append")  # Append mode
-- MAGIC     .toTable("default.simple_sales_updates")  # Fully qualified table name
-- MAGIC )
-- MAGIC

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
-- MAGIC # Generate additional data files
-- MAGIC generate_simple_data(source_dir, file_count=3)

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
-- MAGIC # Clean up directories
-- MAGIC dbutils.fs.rm(source_dir, recurse=True)
-- MAGIC dbutils.fs.rm(checkpoint_dir, recurse=True)